package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWoker {

    private static final long BEGIN_TIME_STAMP = 1640995200l;

    @Resource
    StringRedisTemplate stringRedisTemplate;

    public long nextId(String prefix) {
        //time stamp
        LocalDateTime now = LocalDateTime.now();
        long l = now.toEpochSecond(ZoneOffset.UTC);
        long timeStamp = l - BEGIN_TIME_STAMP;

        //id
        String yyyyMMdd = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + prefix + ":" + yyyyMMdd);

        //concat
        return timeStamp << 32 + count;
    }


}
