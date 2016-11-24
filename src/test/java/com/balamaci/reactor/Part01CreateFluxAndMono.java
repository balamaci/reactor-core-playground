package com.balamaci.reactor;

import com.balamaci.reactor.util.Helpers;
import javaslang.collection.List;
import org.junit.Test;
import reactor.core.Cancellation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * Both Flux and Mono are extensions of Publisher
 *
 * @author sbalamaci
 */
public class Part01CreateFluxAndMono implements BaseTestFlux {

    @Test
    public void just() {
        Flux<Integer> flux = Flux.just(1, 5, 10);

        flux.subscribe(
                val -> log.info("Subscriber received: {}", val));
    }

    @Test
    public void range() {
        Flux<Integer> flux = Flux.range(1, 10);
        flux.subscribe(
                val -> log.info("Subscriber received: {}", val), 5);
    }

    @Test
    public void fromArray() {
        Flux<String> flux = Flux.fromArray(new String[]{"red", "green", "blue", "black"});

        flux.subscribe(
                val -> log.info("Subscriber received: {}"));
    }

    @Test
    public void fromIterable() {
        Flux<String> flux = Flux.fromIterable(List.of("red", "green", "blue"));

        flux.subscribe(
                val -> log.info("Subscriber received: {}"));
    }

    @Test
    public void fromJavaStream() {
        Stream<String> stream = Stream.of("red", "green");
        Flux<String> flux = Flux.fromStream(stream);

        flux.subscribe(
                val -> log.info("Subscriber received: {}"));
    }

    /**
     * We can also create a stream from Future, making easier to switch from legacy code to reactive
     * Since CompletableFuture can only return a single entity, Mono is the returned type when converting
     * from a Future
     */
    @Test
    public void fromFuture() {
        CompletableFuture<String> completableFuture = CompletableFuture.
                supplyAsync(() -> { //starts a background thread the ForkJoin common pool
                      Helpers.sleepMillis(100);
                      return "red";
                });

        Mono<String> mono = Mono.fromFuture(completableFuture);
        mono.subscribe(val -> log.info("Subscriber received: {}", val));
    }

    /**
     * Using Flux.create to handle the actual emissions of events with the events like onNext, onComplete, onError
     * <p>
     * When subscribing to the Flux with flux.subscribe() the lambda code inside create() gets executed.
     * Flux.subscribe can take 3 handlers for each type of event - onNext, onError and onComplete
     * <p>
     * When using Flux.create you need to be aware of <b>Backpressure</b> and that Flux based on 'create' method
     * are not Backpressure aware {@see Part07BackpressureHandling}.
     */
    @Test
    public void createSimpleFlux() {
        Flux<Integer> flux = Flux.create(subscriber -> {
            log.info("Started emitting");

            log.info("Emitting 1st");
            subscriber.next(1);

            log.info("Emitting 2nd");
            subscriber.next(2);

            subscriber.complete();
        });

        log.info("Subscribing");
        Cancellation cancellation = flux.subscribe(
                val -> log.info("Subscriber received: {}", val),
                err -> log.error("Subscriber received error", err),
                () -> log.info("Subscriber got Completed event"));
    }


    /**
     * Flux emits an Error event which is a terminal operation and the subscriber is no longer executing
     * it's onNext callback.
     */
    @Test
    public void createSimpleFluxThatEmitsError() {
        Flux<Integer> flux = Flux.create(subscriber -> {
            log.info("Started emitting");

            log.info("Emitting 1st");
            subscriber.next(1);

            subscriber.error(new RuntimeException("Test exception"));

            log.info("Emitting 2nd");
            subscriber.next(2);
        });

        Cancellation cancellation = flux.subscribe(
                val -> log.info("Subscriber received: {}", val),
                err -> log.error("Subscriber received error", err),
                () -> log.info("Subscriber got Completed event")
        );
    }

    /**
     * Flux and Mono are lazy, meaning that the code inside create() doesn't get executed without subscribing to the Flux
     * So even if we sleep for a long time inside create() method(to simulate a costly operation),
     * without subscribing to this Observable the code is not executed and the method returns immediately.
     */
    @Test
    public void fluxIsLazy() {
        Flux<Integer> flux = Flux.create(subscriber -> {
            log.info("Started emitting but sleeping for 5 secs"); //this is not executed
            Helpers.sleepMillis(5000);
            subscriber.next(1);
        });
        log.info("Finished");
    }

    /**
     * When subscribing to an Flux, the create() method gets executed for each subscription
     * this means that the events inside create are re-emitted to each subscriber. So every subscriber will get the
     * same events and will not lose any events.
     */
    @Test
    public void multipleSubscriptionsToSameFlux() {
        Flux<Integer> flux = Flux.create(subscriber -> {
            log.info("Started emitting");

            log.info("Emitting 1st event");
            subscriber.next(1);

            log.info("Emitting 2nd event");
            subscriber.next(2);

            subscriber.complete();
        });

        log.info("Subscribing 1st subscriber");
        flux.subscribe(val -> log.info("First Subscriber received: {}", val));

        log.info("=======================");

        log.info("Subscribing 2nd subscriber");
        flux.subscribe(val -> log.info("Second Subscriber received: {}", val));
    }

    /**
     * Inside the create() method, we can check is there are still active subscribers to our Observable.
     * It's a way to prevent to do extra work(like for ex. querying a datasource for entries) if no one is listening
     * In the following example we'd expect to have an infinite stream, but because we stop if there are no active
     * subscribers we stop producing events.
     * The **take()** operator unsubscribes from the Observable after it's received the specified amount of events
     */
    @Test
    public void canceledFlux() {
        Flux<Integer> observable = Flux.create(subscriber -> {

            int i = 1;
            while(true) {
                if(subscriber.isCancelled()) {
                    break;
                }

                subscriber.next(i++);
            }
            //subscriber.onCompleted(); too late to emit Complete event since the subscription already canceled
        });

        observable
                .take(5)
                .subscribe(
                        val -> log.info("Subscriber received: {}", val),
                        err -> log.error("Subscriber received error", err),
                        () -> log.info("Subscriber got Completed event") //The Complete event is triggered by 'take()' operator
        );
    }

}
