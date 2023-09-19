package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWoker;
import com.hmdp.utils.RedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    ISeckillVoucherService seckillVoucherService;
    @Resource
    RedisIdWoker redisIdWoker;
    @Resource
    StringRedisTemplate stringRedisTemplate;
    @Resource
    RedisLock redisLock;
    @Resource
    RedissonClient redissonClient;
    private ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    //初始化lua
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    //本地消息队列及使用
    /*private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {

                try {
                    //get order from queue
                    VoucherOrder voucherOrder = orderTasks.take();
                    //create order
                    handleVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }*/

    //redis消息队列及使用
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";
        @Override
        public void run() {
            while (true) {
                try {
                    //get message from queue
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //check message
                    if (list == null || list.isEmpty()) {
                        continue;
                    }
                    //parse message
                    MapRecord<String, Object, Object> mapRecord = list.get(0);
                    Map<Object, Object> value = mapRecord.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //create order
                    handleVoucherOrder(voucherOrder);
                    //ACK
                    stringRedisTemplate.opsForStream().acknowledge("s1", "g1", mapRecord.getId());
                } catch (Exception e) {
                    log.error("订单处理异常", e);
                    //error, handle pending && unacked message
                    try {
                        handelPendingList();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }

        private void handelPendingList() throws InterruptedException {
            while (true) {
                try {
                    //get message from pending list
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            //read one
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //check message, pending list empty, so no unhandled message
                    if (list == null || list.isEmpty()) {
                        break;
                    }
                    //parse message
                    MapRecord<String, Object, Object> mapRecord = list.get(0);
                    Map<Object, Object> value = mapRecord.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //create order
                    handleVoucherOrder(voucherOrder);
                    //ACK
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", mapRecord.getId());
                } catch (Exception e) {
                    log.error("处理pending list异常", e);
                    Thread.sleep(200);
                    //error, try again to handle pending && unacked message
                    handelPendingList();
                }
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        //redisson锁
        RLock lock = redissonClient.getLock("order:" + userId);
        boolean trylock = lock.tryLock();

        if (!trylock) {
            log.error("不能重复下单");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            redisLock.unlock("order:" + userId);
        }
    }

    private IVoucherOrderService proxy;

    //lua脚本，本地队列，一系列判断和写sql分开进行
    /*@Override
    public Result secKill(Long voucherId) throws InterruptedException {

        //1. lua
        Long userId = UserHolder.getUser().getId();
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString()
        );

        //2 check result, 0==success
        int r = result.intValue();
        if (r != 0) {
            if (r == 1) {
                return Result.fail("库存不足");
            } else {
                return Result.fail("不能重复下单");
            }
        }

        //3 new order, and put to blockingQueue
        long orderId = redisIdWoker.nextId("order");

        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);

        orderTasks.add(voucherOrder);

        proxy = (IVoucherOrderService) AopContext.currentProxy();

        return Result.ok(orderId);
    }*/

    //朴实无华的加锁处理
    /*@Override
    public Result secKill(Long voucherId) throws InterruptedException {

        //get voucher
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);

        //check
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀未开始");
        }
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已结束");
        }

        if (voucher.getStock() < 1) {
            return Result.fail("库存不足");
        }
        //单机锁
        Long userId = UserHolder.getUser().getId();
//        synchronized (userId.toString().intern()) {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }

        //redis分布式锁
//        boolean trylock = redisLock.tryLock(1200, "order:" + userId);

        //redisson锁
        RLock lock = redissonClient.getLock("order:" + userId);
        boolean trylock = lock.tryLock();

        if (!trylock) {
            return Result.fail("不能重复下单");
        }
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            redisLock.unlock("order:" + userId);
        }
    }*/

    //redis 消息队列
    @Override
    public Result secKill(Long voucherId) throws InterruptedException {
        long orderId = redisIdWoker.nextId("order");
        Long userId = UserHolder.getUser().getId();

        //1. lua
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );

        //2 check result, 0==success
        int r = result.intValue();
        if (r != 0) {
            if (r == 1) {
                return Result.fail("库存不足");
            } else {
                return Result.fail("不能重复下单");
            }
        }
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //one user only one voucher
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            log.error("已经购买过了");
        }
        //try set sql
        boolean success = seckillVoucherService.update()
                .setSql("stock=stock-1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)//check stock
                .update();

        if (!success) {
            log.error("try again");
        }
        save(voucherOrder);
    }
}
