package com.shedule.x.config.factory;

import lombok.Getter;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Getter
public class SemaphoreGuard implements AutoCloseable {
    private final Semaphore semaphore;
    private final String semaphoreName;
    private final Object id;
    private boolean acquired;

    public SemaphoreGuard(Semaphore semaphore, String semaphoreName, Object id) {
        this.semaphore = semaphore;
        this.semaphoreName = semaphoreName;
        this.id = id;
        this.acquired = false;
    }

    public void acquire(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        if (!semaphore.tryAcquire(timeout, unit)) {
            throw new TimeoutException("Timed out acquiring %s for id=%s".formatted(semaphoreName, id));
        }
        acquired = true;
    }

    public void close() {
        if (acquired) {
            semaphore.release();
            acquired = false;
        }
    }
}