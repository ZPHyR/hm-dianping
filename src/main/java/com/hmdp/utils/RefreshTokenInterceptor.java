package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RefreshTokenInterceptor implements HandlerInterceptor {

    private StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //1 token
        String token = request.getHeader("authorization");
        if (StringUtils.isBlank(token)) {
            return true;
        }

        //2 get user
        Map<Object, Object> userMap =
                stringRedisTemplate.opsForHash().entries(RedisConstants.LOGIN_USER_KEY + token);

        //3 check user
        if (userMap.isEmpty()) {
            return true;
        }

        //4 hashmap to User
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);

        //5 save user to thread-local
        UserHolder.saveUser(userDTO);

        //6 reset redis TLL
        stringRedisTemplate.expire(RedisConstants.LOGIN_USER_KEY + token,
                RedisConstants.LOGIN_USER_TTL, TimeUnit.SECONDS);

        //5 go
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
    }
}
