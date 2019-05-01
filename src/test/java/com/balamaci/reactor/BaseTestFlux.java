package com.balamaci.reactor;

import com.balamaci.reactor.util.Helpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 * @author sbalamaci
 */
interface BaseTestFlux {

    Logger log = LoggerFactory.getLogger(BaseTestFlux.class);

    default Flux<Integer> simpleFlux() {
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

    default <T> void subscribeWithLog(Flux<T> flux) {
        flux.subscribe(
                logNext(),
                logErrorConsumer(),
                logCompleteMethod()
        );
    }

    default <T> void subscribeWithLog(Mono<T> mono) {
        mono.subscribe(
                logNext(),
                logErrorConsumer(),
                logCompleteMethod()
        );
    }

    default <T> void subscribeWithLogWaiting(Flux<T> flux) {
        CountDownLatch latch = new CountDownLatch(1);
        flux.subscribe(
                logNext(),
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


    default  Flux<String> delayedByLengthEmitter(ChronoUnit unit, String...items) {
        Flux<String> itemsStream = Flux.fromArray(items);

        return itemsStream.concatMap(item -> Flux.just(item)
                                              .doOnNext(val -> log.info("Received {} delaying for {} ", val, val.length()))
                                              .delayElements(Duration.of(item.length(), unit))
                            );
    }

    default <T> Consumer<? super T> logNext() {
        return (Consumer<T>) val -> log.info("Subscriber received: {}", val);
    }

    default <T> Consumer<? super T> logNextAndSlowByMillis(int millis) {
        return (Consumer<T>) val -> {
            log.info("Subscriber received: {}", val);
            Helpers.sleepMillis(millis);
        };
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


