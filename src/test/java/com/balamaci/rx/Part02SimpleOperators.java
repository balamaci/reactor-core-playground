package com.balamaci.rx;

import com.balamaci.rx.util.Helpers;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;

/**
 * @author sbalamaci
 */
public class Part02SimpleOperators implements BaseTestObservables {

    private static final Logger log = LoggerFactory.getLogger(Part02SimpleOperators.class);

    /**
     * Delay operator - the Thread.sleep of the reactive world, it's pausing for a particular increment of time
     * before emitting the whole range events which are thus shifted by the specified time amount.
     *
     * The delay operator uses a Scheduler {@see Part07Schedulers} by default, which actually means it's
     * running the operators and the subscribe operations on a different thread, which means the test method
     * will terminate before we see the text from the log.
     *
     * To prevent this we use the .toBlocking() operator which returns a BlockingObservable. Operators on
     * BlockingObservable block(wait) until upstream Observable is completed
     */
    @Test
    public void delayOperator() {
        CountDownLatch latch = new CountDownLatch(1);

        Flux.range(0, 5)
                .delay(Duration.of(2, ChronoUnit.SECONDS))
                .subscribe(
                        tick -> log.info("Tick {}", tick),
                        (ex) -> log.info("Error emitted"),
                        () -> log.info("Completed"));

        Helpers.wait(latch);
    }


    @Test
    public void delayOperatorWithVariableDelay() {
        CountDownLatch latch = new CountDownLatch(1);

        Flux.range(0, 5)
                .concatMap(val -> Mono.just(val).delaySubscription(Duration.of(val * 2, ChronoUnit.SECONDS)))
                .subscribe(
                        tick -> log.info("Tick {}", tick),
                        (ex) -> log.info("Error emitted"),
                        () -> log.info("Completed"));

        Helpers.wait(latch);
    }

    /**
     * Periodically emits a number starting from 0 and then increasing the value on each emission
     */
    @Test
    public void intervalOperator() {
        log.info("Starting");

        Flux.interval(Duration.of(1, ChronoUnit.SECONDS))
                .take(5)
                .subscribe(
                        tick -> log.info("Tick {}", tick),
                        (ex) -> log.info("Error emitted"),
                        () -> log.info("Completed"));
    }



}
