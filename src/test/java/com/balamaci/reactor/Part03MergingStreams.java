package com.balamaci.reactor;

import com.balamaci.reactor.util.Helpers;
import javafx.util.Pair;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;

/**
 * Operators for working with multiple streams
 *
 *
 */
public class Part03MergingStreams implements BaseTestObservables {

    /**
     * Zip operator operates sort of like a zipper in the sense that it takes an event from one stream and waits
     * for an event from another other stream. Once an event for the other stream arrives, it uses the zip function
     * to merge the two events.
     * <p>
     * This is an useful scenario when for example you want to make requests to remote services in parallel and
     * wait for both responses before continuing.
     * <p>
     * Zip operator besides the streams to zip, also takes as parameter a function which will produce the
     * combined result of the zipped streams once each stream emitted it's value
     */
    @Test
    public void zipUsedForTakingTheResultOfCombinedAsyncOperations() {
        /* This stream completes faster */
        Mono<Boolean> isUserBlockedStream = Mono.fromFuture(CompletableFuture.supplyAsync(() -> {
            Helpers.sleepMillis(200);

            log.info("Stream1 emitted");
            return Boolean.FALSE;
        }));
        Mono<String> userCreditScoreStream = Mono.fromFuture(CompletableFuture.supplyAsync(() -> {
            Helpers.sleepMillis(2300);

            log.info("Stream2 emitted");
            return "GOOD";
        }));

        /* zip waits for the slower stream to complete and invokes the zipping functions on both results */
        Flux<Tuple2<Boolean, String>> userCheckStream = Flux.zip(isUserBlockedStream, userCreditScoreStream,
                (blocked, creditScore) -> Tuples.of(blocked, creditScore));
        subscribeWithLogWaiting(userCheckStream);
    }

    /**
     * Implementing a periodic emitter, by waiting for a slower stream to emit periodically.
     * Since the zip operator need a pair of events, the slow stream will work like a timer by periodically emitting
     * with zip setting the pace of emissions downstream.
     */
    @Test
    public void zipUsedToSlowDownAnotherStream() {
        Flux<String> colors = Flux.just("red", "green", "blue");
        Flux<Long> timer = Flux.interval(Duration.of(2, ChronoUnit.SECONDS));

        Flux<String> periodicEmitter = Flux.zip(colors, timer, (key, val) -> key);
        subscribeWithLog(periodicEmitter);
    }


    /**
     * Merge operator combines one or more stream and passes events downstream as soon
     * as they appear
     * <p>
     * The subscriber will receive both color strings and numbers from the Observable.interval
     * as soon as they are emitted
     */
    @Test
    public void mergeOperator() {
        log.info("Starting");

        Flux<String> colors = periodicEmitter("red", "green", "blue", 2, ChronoUnit.SECONDS);

        Flux<Long> numbers = Flux.interval(Duration.of(1, ChronoUnit.SECONDS))
                .take(5);

        Flux flux = Flux.merge(colors, numbers);
        subscribeWithLogWaiting(flux);
    }

    /**
     * Concat operator appends another streams at the end of another
     * The ex. shows that even the 'numbers' streams should start early, the 'colors' stream emits fully its events
     * before we see any 'numbers'.
     * This is because 'numbers' stream is actually subscribed only after the 'colors' complete.
     * Should the second stream be a 'hot' emitter, its events would be lost until the first one finishes
     * and the seconds stream is subscribed.
     */
    @Test
    public void concatStreams() {
        log.info("Starting");
        Flux<String> colors = periodicEmitter("red", "green", "blue", 2, ChronoUnit.SECONDS);

        Flux<Long> numbers = Flux.interval(Duration.of(1, ChronoUnit.SECONDS))
                .take(4);

        Flux flux = Flux.concat(colors, numbers);
        subscribeWithLogWaiting(flux);
    }

    /**
     * combineLatest pairs events from multiple streams, but instead of waiting for an event
     * from all other streams, it uses the last emitted event from that stream
     */
    @Test
    public void combineLatest() {
        log.info("Starting");

        Flux<String> colors = periodicEmitter("red", "green", "blue", 3, ChronoUnit.SECONDS);
        Flux<Long> numbers = Flux.interval(Duration.of(1, ChronoUnit.SECONDS))
                                    .take(4);

        Flux flux = Flux.combineLatest(colors, numbers, Pair::new);
        subscribeWithLog(flux);
    }
}
