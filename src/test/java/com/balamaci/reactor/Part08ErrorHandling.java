package com.balamaci.reactor;

import org.junit.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Exceptions are for exceptional situations.
 * The ReactiveStreams contract specifies that exceptions are terminal operations.
 *
 * There are however operators available for error flow control
 */
public class Part08ErrorHandling implements BaseTestFlux {

    private static final ConcurrentHashMap<String, AtomicInteger> attemptsMap = new ConcurrentHashMap<>();

    /**
     * After the map() operator encounters an error, it triggers the error handler
     * in the map operator, which also unsubscribes(cancels the subscription) from the stream,
     * therefore 'yellow' is not even sent downstream.
     */
    @Test
    public void errorIsTerminalOperation() {
        Flux<String> colors = Flux.just("green", "blue", "red", "yellow")
                .doOnCancel(() -> log.info("Subscription canceled"))
                .map(color -> {
                    if ("red".equals(color)) {
                        throw new RuntimeException("Encountered red");
                    }
                    return color + "*";
                })
                .map(val -> val + "XXX");

        subscribeWithLog(colors);
    }


    /**
     * The 'onErrorReturn' operator doesn't prevent the unsubscription from the 'numbers'
     * and 'colors' stream of the map operator, but it does translate the exception
     * for the downstream operators and the final Subscriber which receives it in the 'onNext()'
     * instead in 'onError()'
     */
    @Test
    public void onErrorReturnDoesntPreventUnsubscription() {
        Flux<Integer> numbers = Flux.just("1", "3", "a", "4", "5", "c")
                                    .map(Integer::parseInt) //parseInt throws NumberFormatException
                                    .onErrorReturn(0);      //for non numeric values like "a", "c"
        subscribeWithLog(numbers);

        log.info("Another stream");

        Flux<String> colors = Flux.just("green", "blue", "red", "yellow", "blue")
                .map(color -> {
                    if ("red".equals(color)) {
                        throw new RuntimeException("Encountered red");
                    }
                    return color + "*";
                })
                .onErrorReturn(th -> th instanceof RuntimeException, "-blank-")
                .map(val -> val + "XXX");

        subscribeWithLog(colors);
    }



    @Test
    public void onErrorReturnWithFlatMap() {
        //flatMap encounters an error when it subscribes to 'red' substreams and thus unsubscribe from
        // 'colors' stream and the remaining colors still are not longer emitted
        Flux<String> colors = Flux.just("green", "blue", "red", "white", "blue")
                .flatMap(color -> simulateRemoteOperation(color))
                .onErrorReturn("-blank-"); //onErrorReturn just has the effect of translating

        subscribeWithLog(colors);

        log.info("*****************");

        //bellow onErrorReturn() is applied to the flatMap substream and thus translates the exception to
        //a value and so flatMap continues on with the other colors after red
        colors = Flux.just("green", "blue", "red", "white", "blue")
                .flatMap(color -> simulateRemoteOperation(color)
                                    .onErrorReturn("-blank-")  //onErrorReturn doesn't trigger
                        // the onError() inside flatMap so it doesn't unsubscribe from 'colors'
                );

        subscribeWithLog(colors);
    }


    /**
     * onErrorResumeWith() returns a stream instead of an exception, useful for example
     * to invoke a fallback method that returns also a stream
     */
    @Test
    public void onErrorResumeWith() {
        Flux<String> colors = Flux.just("green", "blue", "red", "white", "blue")
                .flatMap(color -> simulateRemoteOperation(color)
                        .onErrorResumeWith(th -> {
                            if (th instanceof IllegalArgumentException) {
                                return Flux.error(new RuntimeException("Fatal, wrong arguments"));
                            }
                            return fallbackRemoteOperation();
                        })
                );

        subscribeWithLog(colors);
    }

    private Flux<String> fallbackRemoteOperation() {
        return Flux.just("blank");
    }



    /**
     ************* Retry Logic ****************
     ****************************************** */

    /**
     * timeout operator raises exception when there are no events incoming before it's predecessor in the specified
     * time limit
     *
     * retry() resubscribes in case of exception to the Observable
     */
    @Test
    public void timeoutWithRetry() {
        Flux<String> colors = Flux.just("red", "blue", "green", "yellow")
                .concatMap(color ->  delayedByLengthEmitter(ChronoUnit.SECONDS, color)
                                        .timeout(Duration.of(6, ChronoUnit.SECONDS))
                                        .retry(2)
                                        .onErrorResumeWith(exception -> Flux.just("blank"))
                );

        subscribeWithLogWaiting(colors);
        //there is also
    }

    /**
     * When you want to retry based on the number considering the thrown exception type
     */
/*
    @Test
    public void retryBasedOnAttemptsAndExceptionType() {
        Flux<String> colors = Flux.just("blue", "red", "black", "yellow");

        colors = colors
                    .flatMap(colorName -> simulateRemoteOperation(colorName)
                              .retryWhen((exceptionStream) -> exceptionStream
                                            .flatMap(exception ->  {
                                if (exception instanceof IllegalArgumentException) {
                                    log.error("{} encountered non retry exception ", colorName);
                                    return Flux.error(exception);
                                }

                                Flux.range(1, 3).
                                log.info("Retry attempt {} for {}", retryAttempt, colorName);
                                return retryAttempt <= 2;
                              })
                              .onErrorResumeNext(Flux.just("generic color"))
                    );

        subscribeWithLogWaiting(colors);
    }
*/

    /**
     * A more complex retry logic like implementing a backoff strategy in case of exception
     * This can be obtained with retryWhen(exceptionObservable -> Observable)
     *
     * retryWhen resubscribes when an event from an Observable is emitted. It receives as parameter an exception stream
     *
     * we zip the exceptionsStream with a .range() stream to obtain the number of retries,
     * however we want to wait a little before retrying so in the zip function we return a delayed event - .timer()
     *
     * The delay also needs to be subscribed to be effected so we also need flatMap
     *
     */
    @Test
    public void retryWhenUsedForRetryWithBackoff() {
        Flux<String> colors = Flux.just("blue", "green", "red", "black", "yellow");

        colors = colors.flatMap(colorName ->
                                  simulateRemoteOperation(colorName, 2)
                                    .retryWhen(exceptionStream -> exceptionStream
                                                .zipWith(Flux.range(1, 3), (exc, attempts) -> {
                                                    //don't retry for IllegalArgumentException
                                                    if(exc instanceof IllegalArgumentException) {
                                                        return Flux.error(exc);
                                                    }

                                                    if(attempts < 3) {
                                                        return Flux.
                                                                interval(Duration.of(2 * attempts, ChronoUnit.SECONDS));
                                                    }
                                                    return Flux.error(exc);
                                                })
                                                .flatMap(val -> val)
                                    )
                                  .onErrorResumeWith((th) -> Flux.just("generic color")
                        )
        );

        subscribeWithLogWaiting(colors);
    }

    private Flux<String> simulateRemoteOperation(String color, int workAfterAttempts) {
        return Flux.create(subscriber -> {
            AtomicInteger attemptsHolder = attemptsMap.computeIfAbsent(color, (colorKey) -> new AtomicInteger(0));
            int attempts = attemptsHolder.incrementAndGet();

            if ("red".equals(color)) {
                if(attempts < workAfterAttempts) {
                    log.info("Emitting RuntimeException for {}", color);
                    throw new RuntimeException("Color red raises exception");
                } else {
                    log.info("After attempt {} we don't throw exception", attempts);
                }
            }
            if ("black".equals(color)) {
                if(attempts < workAfterAttempts) {
                    log.info("Emitting IllegalArgumentException for {}", color);
                    throw new IllegalArgumentException("Black is not a color");
                } else {
                    log.info("After attempt {} we don't throw exception", attempts);
                }
            }

            String value = "**" + color + "**";

            log.info("Emitting {}", value);
            subscriber.next(value);
            subscriber.complete();
        });
    }

    private Flux<String> simulateRemoteOperation(String color) {
        return simulateRemoteOperation(color, Integer.MAX_VALUE);
    }


}
