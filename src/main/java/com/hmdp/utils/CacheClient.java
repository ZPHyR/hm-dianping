package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.events.Event;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    //缓存重建线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    public void set(String key, Object data, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(data), time, unit);
    }

    public void setWithLogicalExpire(String key, Object data, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData(data);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 泛型、方法作为参数传入
     *
     * @param keyPrefix
     * @param id
     * @param type
     * @param dbFallback
     * @param <R>
     * @param <ID>
     * @return
     */
    public <R, ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        //1 redis
        String json = stringRedisTemplate.opsForValue().get(key);

        //1.1 yes, return
        if (!StringUtils.isBlank(json)) {
            return JSONUtil.toBean(json, type);
        }

        /**
         *防止缓存穿透，第一次查sql后在redis存了空缓存，此时要拦截查询sql
         * shopJson为blank，不为null，是“”，为空缓存
         *
         * 为null表示什么都没有，之前也没查过，去查询sql
         */
        if (json != null) {
            return null;
        }

        //2 sql
        R r = dbFallback.apply(id);

        //2.1 no, return false
        if (r == null) {
            stringRedisTemplate.opsForValue()
                    .set(key, "", 2, TimeUnit.MINUTES);
            return null;
        }

        //3 write redis
        this.set(key, r, time, unit);

        //4 return
        return r;
    }

    private boolean tryLock(String lock) {
        Boolean mutexLock = stringRedisTemplate.opsForValue()
                .setIfAbsent(lock, "1", 1, TimeUnit.MINUTES);
        return BooleanUtil.isTrue(mutexLock);
    }

    private void unLock(String lock) {
        stringRedisTemplate.delete(lock);
    }

    /**
     * 逻辑过期防止缓存击穿
     *
     * @param id
     * @return
     */
    public <R, ID> R queryWithLogicExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit)
            throws InterruptedException {
        R r = null;
        String key = keyPrefix + id;
        //1 redis
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        //1.1 no, return
        if (StringUtils.isBlank(shopJson)) {
            return null;
        }

        //1.2 yes, json to Shop
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        r = JSONUtil.toBean(data, type);

        //2 if expired
        LocalDateTime expireTime = redisData.getExpireTime();

        //2.1 not expired
        if (expireTime.isAfter(LocalDateTime.now())) {
            return r;
        }

        //2.2 expired
        String lockKey = LOCK_SHOP_KEY + id;
        boolean notLocked = tryLock(lockKey);
        if (notLocked) {
            //3 thread to query and write redis
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R r1 = dbFallback.apply(id);

                    this.setWithLogicalExpire(key, r1, time, unit);
                } finally {
                    unLock(lockKey);
                }
            });
        }
        return r;
    }
}
