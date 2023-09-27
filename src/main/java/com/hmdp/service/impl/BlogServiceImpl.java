package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
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
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Resource
    IFollowService followService;

    @Override
    public Result queryBlogById(Long id) {
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在");
        }
        queryBlogCreator(blog);

        isBlogLiked(blog);

        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        if (UserHolder.getUser() == null) {
            return;
        }
        Long userId = UserHolder.getUser().getId();
        //check if user already liked
        String key = "blog:liked:" + blog.getId();
        Double liked = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(liked != null);
    }

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryBlogCreator(blog);
            this.isBlogLiked(blog);

        });
        return Result.ok(records);
    }

    @Override
    public Result likeBlog(Long id) {
        //get user
        Long userId = UserHolder.getUser().getId();
        //check if user already liked
        String key = "blog:liked:" + id;
        Double liked = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (liked == null) {
            //not liked
            //sql
            boolean success = update().setSql("liked=liked+1").eq("id", id).update();
            if (success) {
                //需要显示blog的点赞用户，根据排序决定顺序，所以使用有score的sortedset
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        } else {
            //liked
            //sql
            boolean success = update().setSql("liked=liked-1").eq("id", id).update();
            if (success) {
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        String key = "blog:liked:" + id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);

        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());

        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }

        List<UserDTO> userDTOS = userService.query().in("id", ids)
                .last("ORDER BY FIELD(id," + StrUtil.join(",", ids) + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);

    }

    //推模式Feed流
    @Override
    public Result saveBlog(Blog blog) {
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());

        save(blog);
        //select all fans
        List<Follow> follows = followService.query()
                .eq("follow_user_id", user.getId()).list();
        for (Follow f : follows) {
            Long id = f.getId();
            String key = "feed:" + id;
            stringRedisTemplate.opsForZSet()
                    .add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        return Result.ok(blog);
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        UserDTO user = UserHolder.getUser();
        Long id = user.getId();
        String key = "feed:" + id;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int os = 1;
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            ids.add(Long.valueOf(tuple.getValue()));
            long time = tuple.getScore().longValue();
            if (time == minTime) {
                os++;
            } else {
                os = 1;
            }
        }
        List<Blog> blogs = query()
                .in("id", ids)
                .last("ORDER BY FIELD(id," + StrUtil.join(",", ids) + ")").list();

        for (Blog blog : blogs) {
            queryBlogCreator(blog);
            isBlogLiked(blog);
        }

        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setOffset(os);
        scrollResult.setMinTime(minTime);

        return Result.ok(scrollResult);
    }

    private void queryBlogCreator(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
