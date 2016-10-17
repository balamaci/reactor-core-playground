package com.balamaci.rx;

import com.balamaci.rx.util.Helpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import rx.Observable;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author sbalamaci
 */
public interface BaseTestObservables {

    Logger log = LoggerFactory.getLogger(BaseTestObservables.class);

    default Flux<Integer> simpleObservable() {
        Flux<Integer> flux = Flux.create(subscriber -> {
            log.info("Started emitting");

            log.info("Emitting 1st");
            subscriber.next(1);

            log.info("Emitting 2nd");
            subscriber.next(2);

            subscriber.complete();
        });

        return flux;
    }

    default void subscribeWithLog(Flux flux) {
        flux.subscribe(
                val -> log.info("Subscriber received: {}", val),
                logErrorConsumer(),
                logCompleteMethod()
        );
    }

    default void subscribeWithLogWaiting(Flux flux) {
        CountDownLatch latch = new CountDownLatch(1);
        flux.subscribe(
                val -> log.info("Subscriber received: {}", val),
                logErrorConsumer(latch),
                logCompleteMethod(latch)
        );
        Helpers.wait(latch);
    }


    default  <T> Flux<T> periodicEmitter(T t1, T t2, T t3, int interval, TemporalUnit unit) {
        return periodicEmitter(t1, t2, t3, interval, unit, interval);
    }

    default  <T> Flux<T> periodicEmitter(T t1, T t2, T t3, int interval,
                                         TemporalUnit unit, int initialDelay) {
        Flux<T> itemsStream = Flux.just(t1, t2, t3);
        Flux<Long> timer = Flux.interval(Duration.of(initialDelay, unit), Duration.of(interval, unit));

        return Flux.zip(itemsStream, timer, (key, val) -> key);
    }

    default  <T> Flux<T> periodicEmitter(T[] items, int interval,
                                         TemporalUnit unit, int initialDelay) {
        Flux<T> itemsStream = Flux.fromArray(items);
        Flux<Long> timer = Flux.interval(Duration.of(initialDelay, unit), Duration.of(interval, unit));

        return Flux.zip(itemsStream, timer, (key, val) -> key);
    }

    default  <T> Observable<T> periodicEmitter(T[] items, int interval,
                                               TimeUnit unit) {
        return periodicEmitter(items, interval, unit);
    }

    default  Flux<String> delayedByLengthEmitter(ChronoUnit unit, String...items) {
        Flux<String> itemsStream = Flux.fromArray(items);

        return itemsStream.concatMap(item -> Flux.just(item)
                                              .doOnNext(val -> log.info("Received {} delaying for {} ", val, val.length()))
                                              .delay(Duration.of(item.length(), unit))
                            );
    }

    default Consumer<Throwable> logErrorConsumer() {
        return err -> log.error("Subscriber received error '{}'", err.getMessage());
    }

    default Consumer<Throwable> logErrorConsumer(CountDownLatch latch) {
        return err -> {
            log.error("Subscriber received error '{}'", err.getMessage());
            latch.countDown();
        };
    }


    default Runnable logCompleteMethod() {
        return () -> log.info("Subscriber got Completed event");
    }

    default Runnable logCompleteMethod(CountDownLatch latch) {
        return () -> {
            log.info("Subscriber got Completed event");
            latch.countDown();
        };
    }

}


