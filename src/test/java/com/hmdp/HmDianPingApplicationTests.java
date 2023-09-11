package com.hmdp;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;

@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Test
    public void testRedis() {
        stringRedisTemplate.opsForValue().set("k1", "v1");
        System.out.println(stringRedisTemplate.opsForValue().get("k1"));

        System.out.println(">>>>>>>>>>");

        stringRedisTemplate.opsForValue().getAndDelete("k1");
    }
}

