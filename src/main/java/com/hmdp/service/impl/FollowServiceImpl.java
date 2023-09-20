package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private UserServiceImpl userService;

    @Override
    public Result unFollow(Long followUserId) {
        Long id = UserHolder.getUser().getId();
        remove(new QueryWrapper<Follow>()
                .eq("user_id", id).eq("follow_user_id", followUserId));
        return Result.ok();
    }

    @Override
    public Result follow(Long followUserId) {
        Long id = UserHolder.getUser().getId();
        Follow follow = new Follow();
        follow.setUserId(id);
        follow.setFollowUserId(followUserId);
        follow.setCreateTime(LocalDateTime.now());
        save(follow);
        stringRedisTemplate.opsForSet().add("follows:" + id, followUserId.toString());
        return Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        Long id = UserHolder.getUser().getId();
        Integer count = query()
                .eq("user_id", id).eq("follow_user_id", followUserId).count();
        stringRedisTemplate.opsForSet().remove("follows:" + id, followUserId.toString());
        return Result.ok(count > 0);
    }

    @Override
    public Result commonFollows(Long id) {
        Long userId = UserHolder.getUser().getId();
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect("follows:" + id, "follows:" + userId);

        if (intersect == null || intersect.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        List<Long> list = intersect.stream().map(Long::valueOf).collect(Collectors.toList());

        List<UserDTO> userDTOS = userService.listByIds(list)
                .stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        return Result.ok(userDTOS);
    }
}
