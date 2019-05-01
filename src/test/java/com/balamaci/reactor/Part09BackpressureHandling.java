package com.balamaci.reactor;

import com.balamaci.reactor.util.Helpers;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;

/**
 * @author sbalamaci
 */
public class Part09BackpressureHandling implements BaseTestFlux {


    @Test
    public void customBackpressureAwareFlux() {
        Flux<Integer> flux = new CustomFlux(5, 10).log();

        flux.subscribe(
                val -> log.info("Subscriber received: {}", val), logErrorConsumer(), logCompleteMethod(),
                (subscription -> subscription.request(3)));
    }


    @Test
    public void fluxWithCreateHasBackpressureSupport() {
        CountDownLatch latch = new CountDownLatch(1);

//        Flux<Integer> flux = createFlux(10, FluxSink.OverflowStrategy.DROP)
//        Flux<Integer> flux = createFlux(10, FluxSink.OverflowStrategy.ERROR)
        Flux<Integer> flux = createFlux(10, FluxSink.OverflowStrategy.LATEST)
                .log()
                .publishOn(Schedulers.newElastic("elast"), 5);

        flux.subscribe(logNextAndSlowByMillis(50), //simulate a slow subscriber
                logErrorConsumer(latch),
                logCompleteMethod(latch));

        Helpers.wait(latch);
    }



     /**
      * Not only a slow subscriber triggers backpressure, but also a slow operator
      * that would slow down the handling of events and new request calls for new items
      */
    @Test
    public void backpressureStrategyTriggeredBySlowOperator() {
        Flux<String> flux = createFlux(20, FluxSink.OverflowStrategy.IGNORE)
                .onBackpressureDrop(overflowVal -> log.info("Dropped {}", overflowVal))
                .onBackpressureBuffer(5)
                .log()
                .publishOn(Schedulers.newElastic("elast"), 2)
                .map(val -> {
                    Helpers.sleepMillis(1000);
                    return "*" + val + "*";
                });
        subscribeWithLogWaiting(flux);
    }

    @Test
    public void cascadingBackpressureOperators() {
        CountDownLatch latch = new CountDownLatch(1);

        Flux<Integer> flux = createFlux(20, FluxSink.OverflowStrategy.IGNORE)
                .onBackpressureBuffer(5)
                .log()
                .limitRate(10)
                .onBackpressureDrop(overflowVal -> log.info("Dropped {}", overflowVal))
                .log()
                .publishOn(Schedulers.newElastic("elast"), 5);
        subscribeWithSlowSubscriber(flux, latch);
        Helpers.wait(latch);

    }


    @Test
    public void fluxWithCustomLogicOnBackpressureBuffer() {
        CountDownLatch latch = new CountDownLatch(1);


        Flux<Integer> flux = createFlux(15, FluxSink.OverflowStrategy.ERROR)
                .onBackpressureDrop((val) -> log.info("Dropping overflown val={}", val))
                .log()
                .publishOn(Schedulers.newElastic("elast"), 5);

        subscribeWithSlowSubscriber(flux, latch);

        Helpers.wait(latch);
    }



    /**
     * Zipping a slow stream with a faster one also can cause a backpressure problem
     */
    @Test
    public void zipOperatorHasALimit() {
        Flux<Integer> fast = createFlux(100, FluxSink.OverflowStrategy.DROP).log();
        Flux<Long> slowStream = Flux.interval(Duration.of(100, ChronoUnit.MILLIS));

        Flux<String> zippedFlux = Flux.zip(fast, slowStream,
                (val1, val2) -> val1 + " " + val2);

        subscribeWithLogWaiting(zippedFlux);
    }


    private class CustomFlux extends Flux<Integer> {

        private int startFrom;
        private int count;

        CustomFlux(int startFrom, int count) {
            this.startFrom = startFrom;
            this.count = count;
        }

        @Override
        public void subscribe(CoreSubscriber<? super Integer> subscriber) {
            subscriber.onSubscribe(new CustomRangeSubscription(startFrom, count, subscriber));
        }

        class CustomRangeSubscription implements Subscription {

            volatile boolean cancelled;
            private int count;
            private int currentCount;
            private int startFrom;

            private Subscriber<? super Integer> actualSubscriber;

            CustomRangeSubscription(int startFrom, int count, Subscriber<? super Integer> actualSubscriber) {
                this.count = count;
                this.startFrom = startFrom;
                this.actualSubscriber = actualSubscriber;
            }

            @Override
            public void request(long items) {
                for(int i=0; i < items; i++) {
                    if(cancelled) {
                        return;
                    }

                    if(currentCount == count) {
                        actualSubscriber.onComplete();
                        return;
                    }

                    actualSubscriber.onNext(startFrom + currentCount);

                    currentCount++;
                }
            }

            @Override
            public void cancel() {
                cancelled = true;
            }
        }
    }

        private Flux<Integer> createFlux(int events, FluxSink.OverflowStrategy overflowStrategy) {
            return Flux.create(subscriber -> {
                log.info("Started emitting");

                for (int i = 1; i < events; i++) {
                    if (subscriber.isCancelled()) {
                        return;
                    }
                    log.info("Emitting {}", i);
                    subscriber.next(i);
                }

                subscriber.complete();
            }, overflowStrategy);
        }

        private <T> void subscribeWithSlowSubscriber(Flux<T> flux, CountDownLatch latch) {
            flux.subscribe(logNextAndSlowByMillis(200),
                    logErrorConsumer(latch),
                    logCompleteMethod(latch));
        }


}
