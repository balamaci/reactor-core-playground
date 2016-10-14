package com.balamaci.rx;

import javafx.util.Pair;
import org.junit.Test;
import reactor.core.publisher.Flux;
import rx.Observable;
import rx.observables.GroupedObservable;

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

    @Test
    public void simpleWindow() {
        Flux<Long> numbers = Flux.interval(Duration.of(1, ChronoUnit.SECONDS));

        Flux<Long> delayedNumbersWindow = numbers
                .window(5) //size of events
                .flatMap(window -> window.doOnComplete(() -> log.info("Window completed")));

        subscribeWithLogWaiting(delayedNumbersWindow);
    }


    @Test
    public void window() {
        Flux<Long> numbers = Flux.interval(Duration.of(1, ChronoUnit.SECONDS));

        Duration windowSpan = Duration.of(10, ChronoUnit.SECONDS);
        Duration windowTimespan = Duration.of(10, ChronoUnit.SECONDS);

        Flux<Long> delayedNumbersWindow = numbers
                            .window(windowSpan, windowTimespan)
                            .flatMap(window -> window.doOnComplete(() -> log.info("Window completed")));

        subscribeWithLog(delayedNumbersWindow);
    }

    @Test
    public void groupBy() {
        Observable<String> colors = Observable.from(new String[]{"red", "green", "blue",
                "red", "yellow", "green", "green"});

        Observable<GroupedObservable<String, String>> groupedColorsStream = colors
                .groupBy(val -> val);

        Observable<Pair<String, Integer>> colorCountStream = groupedColorsStream
                .flatMap(groupedColor -> groupedColor
                                            .count()
                                            .map(count -> new Pair<>(groupedColor.getKey(), count))
                );

        subscribeWithLog(colorCountStream.toBlocking());
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
