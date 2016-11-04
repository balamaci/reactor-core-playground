package com.balamaci.reactor;

import com.balamaci.reactor.util.Helpers;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * RxJava provides some high level concepts for concurrent execution, like ExecutorService we're not dealing
 * with the low level constructs like creating the Threads ourselves. Instead we're using a {@see reactor.Scheduler} which create
 * Workers who are responsible for scheduling and running code. By default RxJava will not introduce concurrency
 * and will run the operations on the subscription thread.
 *
 * There are two methods through which we can introduce Schedulers into our chain of operations:
 * - <b>subscribeOn allows to specify which Scheduler invokes the code contained in the lambda code for Observable.create()
 * - <b>publishOn</b> allows control to which Scheduler executes the code in the downstream operators
 *
 * RxJava provides some general use Schedulers already implemented:
 *  - Schedulers.computation() - to be used for CPU intensive tasks. A threadpool
 *  - Schedulers.io() - to be used for IO bound tasks
 *  - Schedulers.from(Executor) - custom ExecutorService
 *  - Schedulers.newThread() - always creates a new thread when a worker is needed. Since it's not thread pooled
 *  and always creates a new thread instead of reusing one, this scheduler is not very useful
 *
 * Although we said by default RxJava doesn't introduce concurrency, some operators that involve waiting like 'delay',
 * 'interval' need to run on a Scheduler, otherwise they would just block the subscribing thread.
 * By default **Schedulers.computation()** is used, but the Scheduler can be passed as a parameter.
 *
 * @author sbalamaci
 */
public class Part07Schedulers implements BaseTestFlux {

    /**
     * subscribeOn allows to specify which Scheduler invokes the code contained in the lambda code for Observable.create()
     */
    @Test
    public void testSubscribeOn() {
        log.info("Starting");

        Flux<Integer> flux = Flux.create(subscriber -> { //code that will execute inside the elastic scheduler
            log.info("Starting slow network op");
            Helpers.sleepMillis(2000);

            log.info("Emitting 1st");
            subscriber.next(1);

            subscriber.complete();
        });
        flux = flux.subscribeOn(Schedulers.elastic()) //Specify execution on the IO Scheduler
                .map(val -> {
                    int newValue = val * 2;
                    log.info("Mapping new val {}", newValue);
                    return newValue;
                });

        subscribeWithLogWaiting(flux);
    }


    /**
     * publishOn switches the thread that is used for the subscribers downstream from it's invocation
     * If we initially subscribedOn the IoScheduler we and we
     * further make another call .
     */
    @Test
    public void testPublishOn() {
        log.info("Starting");

        Flux<Integer> observable = simpleFlux()
                .subscribeOn(Schedulers.newElastic("elastic-subscribe"))
                .publishOn(Schedulers.newElastic("elastic-publish"))
                .map(val -> {
                    int newValue = val * 2;
                    log.info("Mapping new val {}", newValue);
                    return newValue;
                })
                .publishOn(Schedulers.newElastic("elastic-2nd-publish"));

        subscribeWithLogWaiting(observable);
    }

    /**
     * Multiple calls to subscribeOn have no effect, just the first one will take effect, so we'll see the code
     * execute on the first  thread.
     */
    @Test
    public void multipleCallsToSubscribeOn() {
        log.info("Starting");

        Flux<Integer> observable = simpleFlux()
                .subscribeOn(Schedulers.newElastic("subscribeA"))
                .subscribeOn(Schedulers.newElastic("subscribeB"))
                .map(val -> {
                    int newValue = val * 2;
                    log.info("Mapping new val {}", newValue);
                    return newValue;
                });

        subscribeWithLogWaiting(observable);
    }

    /**
     * By using subscribeOn in flatMap you can control the thread on which flapMap subscribes to the particular
     * stream. By using a scheduler from a custom executor to which we allow a limited number of threads,
     * we can also control how many concurrent threads are handling the stream operations inside the flatMap
     */
    @Test
    public void flatMapConcurrency() {
        log.info("Starting");

        ExecutorService singleExecutor = Executors.newFixedThreadPool(2);

        Flux<String> observable = Flux.just("red", "green", "blue", "yellow", "orange")
                .flatMap(color -> simulateRemoteOperationByUppercasing(color)
                                    .map(changedColor -> {
                                        String newValue = "**" + changedColor + "**";
                                        log.info("Decorating {}", newValue);
                                        return newValue;
                                    })
                                    .subscribeOn(Schedulers.fromExecutor(singleExecutor))
                );

        subscribeWithLogWaiting(observable);
    }

    private Mono<String> simulateRemoteOperationByUppercasing(String color) {
        return Mono.just(color).map(colorVal -> {
            Helpers.sleepMillis(3000);

            log.info("Emitting {}", color.toUpperCase());
            return color.toUpperCase();
        });

    }
}
