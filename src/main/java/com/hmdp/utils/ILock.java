package com.hmdp.utils;

public interface ILock {
    boolean tryLock(long timeoutSec, String name);

    void unlock(String name);
}
