package com.balamaci.reactor;

import com.balamaci.reactor.util.CountingConsumer;
import com.balamaci.reactor.util.Helpers;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;

/**
 * @author sbalamaci
 */
public class Part04ProcessorsAndConnectableFlux implements BaseTestFlux {

    private static final Logger log = LoggerFactory.getLogger(Part04ProcessorsAndConnectableFlux.class);

    @Test
    public void directProcessor() {
        DirectProcessor<String> hotSource = DirectProcessor.create();

        Flux<String> hotFlux = hotSource.map(String::toUpperCase);

        hotFlux.subscribe(it -> log.info("Subscriber1 received:{}", it));

        hotSource.onNext("blue");
        hotSource.onNext("green");

        log.info("*** Subscribing 2nd**");
        hotFlux.subscribe(it -> log.info("Subscriber2 received:{}", it));

        hotSource.onNext("orange");
        hotSource.onNext("purple");
        hotSource.onComplete();
    }

    @Test
    public void simpleSharedFlux() {
        Flux<Integer> flux = Flux.range(0, 10)
                .doOnSubscribe((val) -> log.info("Subscribing ..."))
//                .delayElements(Duration.of(10, ChronoUnit.MILLIS))
                .share();

//        Flux<Integer> events = flux
//                .publishOn(Schedulers.elastic(), 2);

        CountDownLatch latch = new CountDownLatch(1);
        flux.subscribe(logNext("1"));
        flux.subscribe(logNext("2"), logErrorConsumer(latch), logCompleteMethod(latch));
        Helpers.wait(latch);
    }

    @Test
    public void sharedFlux() {
        Flux<Integer> flux = Flux.range(0, 200)
                .delayElements(Duration.of(10, ChronoUnit.MILLIS))
                .share();

        Flux<Integer> events = flux
                .publishOn(Schedulers.elastic(), 2);

        events.subscribe((val) -> log.info("FAST Subscriber received:{}", val));

        CountDownLatch latch = new CountDownLatch(1);
        CountingConsumer<Integer> countingConsumer = new CountingConsumer<>("SLOW Subscriber");

        Flux<Integer> delayedFlux = events.
                delayElements(Duration.of(100, ChronoUnit.MILLIS));
        delayedFlux
                .subscribe(countingConsumer, logErrorConsumer(latch), logCompleteMethod(latch));

        Helpers.wait(latch);
        log.info("SlowSubscriber received total={}", countingConsumer.getCount());
    }

}
