package com.shedule.x.config.factory;

import java.util.concurrent.TimeoutException;

@FunctionalInterface
    public interface QuadFunction<T, U, V, W, R> {
        R apply(T t, U u, V v, W w) throws InterruptedException, TimeoutException;
    }