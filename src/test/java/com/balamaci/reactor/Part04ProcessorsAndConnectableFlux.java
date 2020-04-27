package com.balamaci.reactor;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;

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


}
