package com.balamaci.reactor.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class CountingConsumer<T> implements Consumer<T> {

    private final AtomicLong counter = new AtomicLong(0);

    private final String subscriberName;

    private static final Logger log = LoggerFactory.getLogger(CountingConsumer.class);

    public CountingConsumer(String subscriberName) {
        this.subscriberName = subscriberName;
    }

    @Override
    public void accept(T t) {
        counter.incrementAndGet();
        if(subscriberName != null) {
            log.info("{} received {}", subscriberName, t);
        }
    }

    public long getCount() {
        return counter.get();
    }
}
