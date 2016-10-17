package com.balamaci.rx;

import javafx.util.Pair;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import rx.Observable;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * @author sbalamaci
 */
public class Part05AdvancedOperators implements BaseTestObservables {

    @Test
    public void buffer() {
        Flux<Long> numbers = Flux.interval(Duration.of(1, ChronoUnit.SECONDS));

        Flux<List<Long>> delayedNumbersWindow = numbers.buffer(5);

        subscribeWithLog(delayedNumbersWindow);
    }

    /**
     * Split the stream into multiple windows.
     * The window size can be specified as a number of events
     */
    @Test
    public void simpleWindow() {
        Flux<Long> numbers = Flux.interval(Duration.of(1, ChronoUnit.SECONDS));

        Flux<Long> delayedNumbersWindow = numbers
                .window(5) //size of events
                .flatMap(window -> window.doOnComplete(() -> log.info("Window completed"))
                                         .doOnSubscribe((sub) -> log.info("Window started"))
                        );

        subscribeWithLogWaiting(delayedNumbersWindow);
    }


    /**
     * Split the stream into multiple windows
     * When the period to start the windows(timeshit) is bigger than the window timespan, some events will be lost
     */
    @Test
    public void windowWithLoosingEvents() {
        Flux<Long> numbers = Flux.interval(Duration.of(1, ChronoUnit.SECONDS));

        Duration timespan = Duration.of(5, ChronoUnit.SECONDS);
        Duration timeshift = Duration.of(10, ChronoUnit.SECONDS);

        subscribeTimeWindow(numbers, timespan, timeshift);
    }


    @Test
    public void windowWithDuplicateEvents() {
        Flux<Long> numbers = Flux.interval(Duration.of(1, ChronoUnit.SECONDS));

        Duration timespan = Duration.of(9, ChronoUnit.SECONDS);
        Duration timeshift = Duration.of(3, ChronoUnit.SECONDS);

        subscribeTimeWindow(numbers, timespan, timeshift);
    }

    private void subscribeTimeWindow(Flux<Long> numbersStream, Duration timespan, Duration timeshift) {
        Flux<Long> delayedNumbersWindow = numbersStream
                .window(timespan, timeshift)
                .flatMap(window -> window
                        .doOnComplete(() -> log.info("Window completed"))
                        .doOnSubscribe((sub) -> log.info("Window started"))
                );

        subscribeWithLogWaiting(delayedNumbersWindow);
    }



    @Test
    public void groupBy() {
        Flux<String> colors = Flux.fromArray(new String[]{"red", "green", "blue",
                "red", "yellow", "green", "green"});

        Flux<GroupedFlux<String, String>> groupedColorsStream = colors
                .groupBy(val -> val);

        Flux<Pair<String, Long>> colorCountStream = groupedColorsStream
                .flatMap(groupedColor -> groupedColor
                                            .count()
                                            .map(count -> new Pair<>(groupedColor.key(), count))
                );

        subscribeWithLogWaiting(colorCountStream);
    }

    @Test
    public void bufferWithLimitTriggeredByObservable() {
        Observable<String> colors = Observable.from(new String[]{"red", "green", "blue",
                "red", "yellow", "#", "green", "green"});


        colors.publish(p -> p.filter(val -> ! val.equals("#"))
                             .buffer(() -> p.filter(val -> val.equals("#")))
                )
                .toBlocking()
                .subscribe(list -> {
            String listCommaSeparated = String.join(",", list);
            log.info("List {}", listCommaSeparated);
        });
    }
}
