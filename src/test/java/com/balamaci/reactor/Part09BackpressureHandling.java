package com.balamaci.reactor;

import com.balamaci.reactor.util.Helpers;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.CountDownLatch;

/**
 * @author sbalamaci
 */
public class Part09BackpressureHandling implements BaseTestFlux {



    @Test
    public void customBackpressureAwareFlux() {
        Flux<Integer> flux = new CustomFlux(5, 10).log();

        flux.subscribe(
                val -> log.info("Subscriber received: {}", val), 3);
    }

    @Test
    public void throwingBackpressureNotSupported() {
        CountDownLatch latch = new CountDownLatch(1);

        Flux<Integer> flux = observableWithoutBackpressureSupport();
//                .log()
//                .onBackpressureBuffer(30)
//                .onBackpressureDrop(val -> log.info("Dropped {}", val));

        flux = flux
                .publishOn(Schedulers.newElastic("subscribe"));
        subscribeWithSlowSubscriber(flux, latch);

        Helpers.wait(latch);
    }

/**
 * Not only a slow subscriber triggers backpressure, but also a slow operator
 *//*

    @Test
    public void throwingBackpressureNotSupportedSlowOperator() {
        CountDownLatch latch = new CountDownLatch(1);

        Observable<Integer> observable = observableWithoutBackpressureSupport();

        observable = observable
                .observeOn(Schedulers.io())
                .map(val -> {
                    Helpers.sleepMillis(50);
                    return val * 2;
                });

        subscribeWithLog(observable, latch);

        Helpers.wait(latch);
    }

    */
/**
 * Subjects are also not backpressure aware
 *//*

    @Test
    public void throwingBackpressureNotSupportedSubject() {
        CountDownLatch latch = new CountDownLatch(1);

        PublishSubject<Integer> subject = PublishSubject.create();

        Observable<Integer> observable = subject
                .observeOn(Schedulers.io());
        subscribeWithSlowSubscriber(observable, latch);

        for(int i=0; i < 200; i++) {
            log.info("Emitting {}", i);
            subject.onNext(i);
        }

        Helpers.wait(latch);
    }

    */

    /**
     * Zipping a slow stream with a faster one also can cause a backpressure problem
     *//*

    @Test
    public void zipOperatorHasALimit() {
        CountDownLatch latch = new CountDownLatch(1);

        Observable<Integer> fast = observableWithoutBackpressureSupport();
        Observable<Long> slowStream = Observable.interval(100, TimeUnit.MILLISECONDS);

        Observable<String> observable = Observable.zip(fast, slowStream,
                (val1, val2) -> val1 + " " + val2);

        subscribeWithLog(observable, latch);
        Helpers.wait(latch);
    }

    @Test
    public void backpressureAwareObservable() {
        CountDownLatch latch = new CountDownLatch(1);

        Observable<Integer> observable = Observable.range(0, 200);

        observable = observable
                .observeOn(Schedulers.io());

        subscribeWithSlowSubscriber(observable, latch);
        Helpers.wait(latch);
    }

    // Handling
    //========================================================

    @Test
    public void dropOverflowingEvents() {
        CountDownLatch latch = new CountDownLatch(1);

        Observable<Integer> observable = observableWithoutBackpressureSupport();

        observable = observable
                .onBackpressureDrop(val -> log.info("Dropped {}", val))
                .observeOn(Schedulers.io());
        subscribeWithSlowSubscriber(observable, latch);

        Helpers.wait(latch);
    }
*/

    private class CustomFlux extends Flux<Integer> {

        private int startFrom;
        private int count;

        CustomFlux(int startFrom, int count) {
            this.startFrom = startFrom;
            this.count = count;
        }

        @Override
        public void subscribe(Subscriber<? super Integer> subscriber) {
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

        private Flux<Integer> observableWithoutBackpressureSupport() {
            return Flux.create(subscriber -> {
                log.info("Started emitting");

                for (int i = 0; i < 300; i++) {
                    if (subscriber.isCancelled()) {
                        return;
                    }
                    log.info("Emitting {}", i);
                    subscriber.next(i);
                }

                subscriber.complete();
            }, FluxSink.OverflowStrategy.ERROR);
        }

        private <T> void subscribeWithSlowSubscriber(Flux<T> observable, CountDownLatch latch) {
            observable.subscribe(logNextAndSlowByMillis(50),
                    logErrorConsumer(latch),
                    logCompleteMethod(latch));
        }


}
