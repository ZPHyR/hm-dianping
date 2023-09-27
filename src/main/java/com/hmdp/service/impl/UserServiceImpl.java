package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1 check phone number
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("wrong number");
        }

        //2 generate code
        String code = RandomUtil.randomNumbers(6);

        //3 save code
//        session.setAttribute("code", code);
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);

        //4 send code
        log.debug("code sent:{}", code);

        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1 check phone number
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("wrong number");
        }

        //2 check code
        String cachedCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (cachedCode == null || !cachedCode.equals(code)) {
            return Result.fail("code wrong!");
        }

        //3 check password

        //4 search user
        User user = query().eq("phone", phone).one();

        //4.1 if no user, create new
        if (user == null) {
            user = createUserWithPhone(phone);
        }

        String token = UUID.randomUUID().toString(true);

        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);

        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                new CopyOptions().create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((feildName, fieldValue) -> fieldValue.toString()));

        //5 save user
        String tokenKey = LOGIN_USER_KEY + token;

        /**
         * 注意，userMap中有一个Long的id，得转成String
         */
//        userMap.forEach((key, value) -> {
//            if (null != value) userMap.put(key, String.valueOf(value));
//        });

        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.SECONDS);

        return Result.ok(token);
    }

    @Override
    public Result sign() {
        //get user
        Long id = UserHolder.getUser().getId();
        //get time
        LocalDateTime now = LocalDateTime.now();
        int dayOfMonth = now.getDayOfMonth();
        //get key
        String keySuffix = now.format(DateTimeFormatter.ofPattern("yyyyMM"));
        String key = "sign:" + id + ":" + keySuffix;
        //write redis
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);

        return Result.ok();
    }

    @Override
    public Result signCount() {
        //get user
        Long id = UserHolder.getUser().getId();
        //get time
        LocalDateTime now = LocalDateTime.now();
        int dayOfMonth = now.getDayOfMonth();
        //get key
        String keySuffix = now.format(DateTimeFormatter.ofPattern("yyyyMM"));
        String key = "sign:" + id + ":" + keySuffix;

        List<Long> result = stringRedisTemplate.opsForValue().bitField(key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0));

        if (result == null || result.isEmpty()) {
            return Result.ok(0);
        }

        Long num = result.get(0);
        if (num == null || num == 0) {
            return Result.ok(0);
        }
        int cnt = 0;
        while (true) {
            if ((num & 1) == 1) {
                cnt++;
                num >>>= 1;
            } else {
                break;
            }
        }
        return Result.ok(cnt);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));

        save(user);

        return user;

    }
}
