package com.hmdp.utils;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class RedisLock implements ILock {
    @Resource
    StringRedisTemplate stringRedisTemplate;

    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID() + "-";
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    //初始化lua
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }


    @Override
    public boolean tryLock(long timeoutSec, String name) {
        long id = Thread.currentThread().getId();
        Boolean succ = stringRedisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + name, ID_PREFIX + id, timeoutSec, TimeUnit.SECONDS);
        return succ.equals(Boolean.TRUE);
    }

    public void unlock(String name) {
        /**
         * 删之前查一下锁是不是当前线程的，不要释放了同一业务下不同请求的锁
         *
         * 出现在以下情况：
         * 1.a上锁
         * 2.a阻塞
         * 3.a锁过期
         * 4.b上锁
         * 5.a唤醒
         * 6.a完成
         * 7.a想解锁
         */
//        long id = Thread.currentThread().getId();
//        String currentId = stringRedisTemplate.opsForValue().get(KEY_PREFIX + id);
//        if ((id + ID_PREFIX).equals(currentId)) {
//            /**
//             * 又tm在通过判断后阻塞了咋办?
//             * lua脚本有原子性，用这个
//             * 判断、删除一起完成
//             */
//            stringRedisTemplate.delete(KEY_PREFIX + name);
//        }
        stringRedisTemplate.execute(UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX + name),
                ID_PREFIX + Thread.currentThread().getId());
    }
}
