package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metrics;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Resource
    CacheClient cacheClient;

    private boolean tryLock(String lock) {
        Boolean mutexLock = stringRedisTemplate.opsForValue()
                .setIfAbsent(lock, "1", 1, TimeUnit.MINUTES);
        return BooleanUtil.isTrue(mutexLock);
    }

    private void unLock(String lock) {
        stringRedisTemplate.delete(lock);
    }

    @Override
    public Object queryById(Long id) throws InterruptedException {
        //穿透
//        Shop shop = queryWithPassThrough(id);
        //击穿
//        Shop shop = queryWithMutex(id);
        //穿透
//        Shop shop = cacheClient.queryWithPassThrough(
//                CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //击穿
        Shop shop = cacheClient.queryWithLogicExpire(
                CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if (shop == null) {
            return Result.fail("wrong id");
        }
        return Result.ok(shop);
    }

    /**
     * 更新后删除缓存
     *
     * @param shop
     * @return
     */
    @Override
    @Transactional//事务
    public Result update(Shop shop) {
        if (shop.getId() == null) {
            return Result.fail("wrong id");
        }
        //1 update sql
        updateById(shop);

        //2 update redis
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shop.getId());

        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1 是否需要坐标
        if (x == null || y == null) {
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }
        //2 分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = (current) * SystemConstants.DEFAULT_PAGE_SIZE;

        //3 查redis，排序，得到id，distance
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search("shop:geo:" + typeId,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(100, Metrics.KILOMETERS),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeCoordinates().limit(end)
                );

        //4 解析出id，并截取分页对应
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }

        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();

        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());

        if (list.size() <= from) {
            return Result.ok(Collections.emptyList());
        }

        list.stream().skip(from).forEach(result -> {
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));

            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        //5 根据id查shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids)
                .last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shops);
    }

    /**
     * 互斥锁防止缓存穿透
     *
     * @param id
     * @return
     */
    public Shop queryWithPassThrough(Long id) {
        //1 redis
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        //1.1 yes, return
        if (!StringUtils.isBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        /**
         *防止缓存穿透，第一次查sql后在redis存了空缓存，此时要拦截查询sql
         * shopJson为blank，不为null，是“”，为空缓存
         *
         * 为null表示什么都没有，之前也没查过，去查询sql
         */
        if (shopJson != null) {
            return null;
        }

        //2 sql
        Shop shop = getById(id);

        //2.1 no, return false
        if (shop == null) {
            stringRedisTemplate.opsForValue()
                    .set(CACHE_SHOP_KEY + id, "", 2, TimeUnit.MINUTES);
            return null;
        }

        //3 write redis
        stringRedisTemplate.opsForValue()
                .set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), 30, TimeUnit.MINUTES);

        //4 return
        return shop;
    }

    /**
     * 互斥锁防止缓存击穿
     *
     * @param id
     * @return
     */
    public Shop queryWithMutex(Long id) throws InterruptedException {

        //1 redis
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        Shop shop = null;

        //1.1 yes, return
        if (!StringUtils.isBlank(shopJson)) {
            shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }


        /**
         *防止缓存穿透，第一次查sql后在redis存了空缓存，此时要拦截查询sql
         * shopJson为blank，不为null，是“”，为空缓存
         *
         * 为null表示什么都没有，之前也没查过，去查询sql
         */
        if (shopJson != null) {
            return null;
        }
        //1.2 no, try get lock
        String lockKey = "lock:shop:" + id;
        try {
            if (!tryLock(lockKey)) {
                Thread.sleep(50);
                return queryWithMutex(id);

            }

            //2 query sql
            shop = getById(id);

            //2.1 no, return false, write redis null
            if (shop == null) {
                stringRedisTemplate.opsForValue()
                        .set(CACHE_SHOP_KEY + id, "", 2, TimeUnit.MINUTES);
                return null;
            }

            //3 yes, write redis
            stringRedisTemplate.opsForValue()
                    .set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), 30, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //4 unlock
            unLock(lockKey);
        }
        //4 return
        return shop;
    }

    /**
     * 逻辑过期时间方式防止缓存击穿的存redis方法
     * 过期了起线程去更新，然后拿旧的先用着
     *
     * @param id
     * @param expireSeconds
     */
    public void saveShop2Redis(Long id, Long expireSeconds) {
        //1 get shop
        Shop shop = getById(id);

        //2 set data
        RedisData redisData = new RedisData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

        //3 write redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    //缓存重建线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /**
     * 逻辑过期防止缓存击穿
     *
     * @param id
     * @return
     */
    public Shop queryWithLogicExpire(Long id) throws InterruptedException {
        Shop shop = null;
        //1 redis
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        //1.1 no, return
        if (StringUtils.isBlank(shopJson)) {
            return null;
        }

        //1.2 yes, json to Shop
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        shop = JSONUtil.toBean(data, Shop.class);

        //2 if expired
        LocalDateTime expireTime = redisData.getExpireTime();

        //2.1 not expired
        if (expireTime.isAfter(LocalDateTime.now())) {
            return shop;
        }

        //2.2 expired
        String lockKey = LOCK_SHOP_KEY + id;
        boolean notLocked = tryLock(lockKey);
        if (notLocked) {
            //3 thread to query and write redis
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    this.saveShop2Redis(id, 30L);
                } finally {
                    unLock(lockKey);
                }
            });
        }
        return shop;
    }
}
