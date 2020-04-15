# Reactor Core

## Contents 
 
   - [Flux and Mono](#flux-and-mono)
   - [Simple Operators](#simple-operators)
   - [Merging Streams](#merging-streams)
   - [FlatMap Operator](#flatmap-operator)
   - [Schedulers](#schedulers)
   - [Error Handling](#error-handling)
   - [Backpressure](#backpressure)

## Reactive Streams
Reactive Streams is a programming concept for handling asynchronous data streams in a non-blocking manner while providing backpressure to stream publishers.
It has evolved into a [specification](https://github.com/reactive-streams/reactive-streams-jvm) that is based on the concept of **Publisher**&lt;T&gt;** and **Subscriber<T>**.
A **Publisher** is the source of events **T** in the stream, and a **Subscriber** is the consumer for those events.
A **Subscriber** subscribes to a **Publisher** by invoking a "factory method" :
```
public interface Publisher<T> {
    public void subscribe(Subscriber<? super T> s);
}
```

When the Subscriber is ready to start handling events, it signals this via a request to the Publisher through **Subscription** 
```
public interface Subscription {
    public void request(long n); //request n items
    public void cancel();
}
```
This is an important thing to be aware of, that the **Subscriber** is signalling demand to the **Publisher** that is ready to receive X items from upstream.
After it finishes processing them, it might ask for another set or even more. 
Now, some subscribers begin by requesting an infinite amount(you'd see this a request(unbounded)) of items from the Publisher and this why it.

Upon receiving this signal, the Publisher begins to invoke **Subscriber::onNext(T)** for each event **T**. 
This continues until either completion of the stream (**Subscriber::onComplete()**) 
or an error occurs during processing (**Subscriber::onError(Throwable)**).
```
public interface Subscriber<T> {
    //signals to the Publisher to start sending events
    public void onSubscribe(Subscription s);     
    
    public void onNext(T t);
    public void onError(Throwable t);
    public void onComplete();
}
```
Between the **Subscriber** and **Publisher** there normally are the **Operators** chains (**.map()**, **.filter()**). 
Each **Operator** acts as a **Subscriber** to the previous **Operator** in the chain so forth until it reaches the actual **Publisher**. 
The subscription and demand signals travels upstream back through each Operator in the chain to the original **Publisher**. 

There are mechanisms of 
Any difference between 

This is nicely explained by the "Movers" analogy that I 
![](images/movers.gif?raw=true)

## Flux and Mono
Reactor provides two main types of publishers: 
   - **Flux** publisher that emits 0..N elements, and then completes (successfully or with an error)
   - **Mono** a specialized publisher that can contain only 0 or 1 events and then completes (successfully or with an error)
    Can be useful to express in an API that a single/no value(just completion notification) value is expected.

### Simple operators to create Flux

```
Flux<Integer> flux = Flux.just(1, 5, 10);
Flux<Integer> flux = Flux.range(1, 10);

Flux<String> flux = Flux.fromArray(new String[] {"red", "green", "blue", "black"});
Flux<String> flux = Flux.fromIterable(List.of("red", "green", "blue"));

/* from Java Stream */
Stream<String> stream = Stream.of("red", "green");
Flux<String> flux = Flux.fromStream(stream);
```

Streams that just complete without emitting a value are valid - useful for example to say that a DB update completed(or failed)-.
```
Flux.empty(); / Mono.empty();
```

so are streams that just complete with an error
```
Flux.error(new IllegalStateException()); / Mono.error(new IllegalStateException());
```

Code is available at [Part01CreateFluxAndMono.java](https://github.com/balamaci/reactor-core-playground/blob/master/src/test/java/com/balamaci/reactor/Part01CreateFluxAndMono.java)

### Creating your own Flux

Using **Flux.create** to handle the actual emissions of events with the events like **onNext**, **onCompleted**, **onError**

``` 
Flux<Integer> flux = Flux.create(subscriber -> {
    log.info("Started emitting");

    log.info("Emitting 1st");
    subscriber.next(1);

    log.info("Emitting 2nd");
    subscriber.next(2);

    subscriber.onCompleted();
});

Cancellation cancellation = flux.subscribe(
        val -> log.info("Subscriber received: {}", val),    //--> onNext
        err -> log.error("Subscriber received error", err), //--> onError
        () -> log.info("Subscriber got Completed event")    //--> onComplete
);
```

When subscribing to the Flux with flux.subscribe(...) the lambda code inside **create()** gets executed.
Flux.subscribe(...) can take 3 handlers for each type of event - **onNext, onError** and **onComplete**.

We can get a more clear picture of what is happening by calling the **log()** operator: 

Now we're also seeing the subscribe request being done and the request upstream signaling demand for an unlimited number of items to be published.
the **request(unbounded)** message.

```
17:54:53:755 [main] INFO 1 - onSubscribe(FluxCreate.BufferAsyncSink)
17:54:53:757 [main] INFO 1 - request(unbounded)
17:54:53:759 [main] INFO BaseTestFlux - Started emitting
17:54:53:759 [main] INFO BaseTestFlux - Emitting 1st
17:54:53:760 [main] INFO 1 - onNext(1)
17:54:53:760 [main] INFO BaseTestFlux - Subscriber received: 1
17:54:53:760 [main] INFO BaseTestFlux - Emitting 2nd
17:54:53:761 [main] INFO 1 - onNext(2)
17:54:53:761 [main] INFO BaseTestFlux - Subscriber received: 2
17:54:53:762 [main] INFO 1 - onComplete()
17:54:53:762 [main] INFO BaseTestFlux - Subscriber got Completed event
```


### First look at backpressure
It is not obvious above, but we've not been fair in our **Publisher**, we didn't care what the subscriber demand was, we just pushed on subscribing 2 items downstream.
If we look in our 


## Working with Legacy code - Mono from Future
Lets imagine we want to incrementally make our legacy code more reactive. 
We can create a stream from Future, making easier to switch from legacy code to reactive so that's it right? 
Since **CompletableFuture** can only return a single result, **Mono** is the returned type when converting
from a Future.


```java
    @Test
    public void fromFuture() {
        CompletableFuture<String> completableFuture = CompletableFuture.
                supplyAsync(() -> { //starts a background thread the ForkJoin common pool
                    log.info("About to sleep and return a value");
                    Helpers.sleepMillis(100);
                    return "red";
                });

        Mono<String> mono = Mono.fromFuture(completableFuture);
        mono
                .subscribe(val -> log.info("Subscriber received: {}", val));
    }
```

There are some things unexpected with this simple example.



### Flux and Mono are lazy 
Flux and Mono are lazy, meaning that the code inside create() doesn't get executed without subscribing to the Flux.
So even if we sleep for a long time inside create() method(to simulate a costly operation),
without subscribing to this Publisher(Flux or Mono) the code is not executed and the method returns immediately.

```
public void fluxIsLazy() {
    Flux<Integer> flux = Flux.create(subscriber -> {
        log.info("Started emitting but sleeping for 5 secs"); //this is not executed
        Helpers.sleepMillis(5000);
        subscriber.next(1);
    });
    log.info("Finished"); 
}
```

### Multiple subscriptions to the same Flux 
When subscribing to a Flux, the *create()* method gets executed for each subscription. This means that the events 
inside create are re-emitted to each subscriber. 

So every subscriber will get the same events and will not lose any events - this is named a **Cold Publisher'**

```
Flux<Integer> flux = Flux.create(subscriber -> {
   log.info("Started emitting");

   log.info("Emitting 1st event");
   subscriber.next(1);

   log.info("Emitting 2nd event");
   subscriber.next(2);

   subscriber.onCompleted();
});

log.info("Subscribing 1st subscriber");
flux.subscribe(val -> log.info("First Subscriber received: {}", val));

log.info("=======================");

log.info("Subscribing 2nd subscriber");
flux.subscribe(val -> log.info("Second Subscriber received: {}", val));
```

will output

```
[main] - Subscribing 1st subscriber
[main] - Started emitting
[main] - Emitting 1st event
[main] - First Subscriber received: 1
[main] - Emitting 2nd event
[main] - First Subscriber received: 2
[main] - =======================
[main] - Subscribing 2nd subscriber
[main] - Started emitting
[main] - Emitting 1st event
[main] - Second Subscriber received: 1
[main] - Emitting 2nd event
[main] - Second Subscriber received: 2
```

### Checking if there are any active subscribers 
Inside the create() method, we can check is there are still active subscribers to our Flux.
It's a way to prevent to do extra work(like for ex. querying a datasource for entries) if no one is listening
In the following example we'd expect to have an infinite stream, but because we stop if there are no active subscribers we stop producing events.
The **take()** operator for ex. just unsubscribes from the Flux after it's received the specified amount of events.

```
Flux<Integer> flux = Flux.create(subscriber -> {

    int i = 1;
    while(true) {
        if(subscriber.isCancelled()) {
             break;
        }

        subscriber.onNext(i++);
    }
    //subscriber.completed(); too late to emit Complete event since subscriber already unsubscribed
});

flux
    .take(5) //unsubscribes after the 5th event
    .subscribe(val -> log.info("Subscriber received: {}", val),
               err -> log.error("Subscriber received error", err),
               () -> log.info("Subscriber got Completed event") //The Complete event 
               //is triggered by 'take()' operator

```

## Simple Operators

### delay
Delay operator - the Thread.sleep of the reactive world, it's pausing for a particular increment of time
before emitting the events which are thus shifted by the specified time amount.

![delay](https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/delayonnext.png)

The delay operator uses a [Scheduler](#schedulers) by default, which actually means it's
running the operators and the subscribe operations on a different thread and so the test method
will terminate before we see the text from the log.(Not doing so would have blocked )


### interval
Periodically emits a number starting from 0 and then increasing the value on each emission.

![interval](https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/interval.png)

```
log.info("Starting");
CountDownLatch latch = new CountDownLatch(1);

Flux.interval(Duration.of(1, ChronoUnit.SECONDS))
    .take(5)
    .subscribe(tick -> log.info("Tick {}", tick),
                  (ex) -> log.info("Error emitted"),
                  () -> {
                          log.info("Completed");
                          latch.countDown();
                  });

Helpers.wait(latch);
```
produces
```
15:44:43 [main]  - Starting
15:44:44 [timer-1]  - Tick 0
15:44:45 [timer-1]  - Tick 1
15:44:46 [timer-1]  - Tick 2
15:44:47 [timer-1]  - Tick 3
15:44:48 [timer-1]  - Tick 4
15:44:48 [timer-1]  - Completed
```

### scan
Takes an initial value and a function(accumulator, currentValue). It goes through the events 
sequence and combines the current event value with the previous result(accumulator) emitting downstream the
The initial value is used for the first event

```
Flux<Integer> numbers = 
                Flux.just(3, 5, -2, 9)
                    .scan(0, (totalSoFar, currentValue) -> {
                               log.info("TotalSoFar={}, currentValue={}", totalSoFar, currentValue);
                               return totalSoFar + currentValue;
                    });
```

```
16:09:17 [main] - Subscriber received: 0
16:09:17 [main] - TotalSoFar=0, currentValue=3
16:09:17 [main] - Subscriber received: 3
16:09:17 [main] - TotalSoFar=3, currentValue=5
16:09:17 [main] - Subscriber received: 8
16:09:17 [main] - TotalSoFar=8, currentValue=-2
16:09:17 [main] - Subscriber received: 6
16:09:17 [main] - TotalSoFar=6, currentValue=9
16:09:17 [main] - Subscriber received: 15
16:09:17 [main] - Subscriber got Completed event
```

### reduce
reduce operator acts like the scan operator but it only passes downstream the final result 
(doesn't pass the intermediate results downstream) so the subscriber receives just one event

```
Mono<Integer> numbers = Flux.just(3, 5, -2, 9)
                            .reduce(0, (totalSoFar, val) -> {
                                         log.info("totalSoFar={}, emitted={}", totalSoFar, val);
                                         return totalSoFar + val;
                            });
```

```
17:08:29 [main] - totalSoFar=0, emitted=3
17:08:29 [main] - totalSoFar=3, emitted=5
17:08:29 [main] - totalSoFar=8, emitted=-2
17:08:29 [main] - totalSoFar=6, emitted=9
17:08:29 [main] - Subscriber received: 15
17:08:29 [main] - Subscriber got Completed event
```

### collect
collect operator acts similar to the _reduce_ operator, but while the _reduce_ operator uses a reduce function
which returns a value, the _collect_ operator takes a container supplier and a function which doesn't return
anything(a consumer). The mutable container is passed for every event and thus you get a chance to modify it
in this collect consumer function.

```
Mono<List<Integer>> numbers = Flux.just(3, 5, -2, 9)
                                  .collect(ArrayList::new, (container, value) -> {
                                        log.info("Adding {} to container", value);
                                        container.add(value);
                                        //notice we don't need to return anything
                                  });
```
```
17:40:18 [main] - Adding 3 to container
17:40:18 [main] - Adding 5 to container
17:40:18 [main] - Adding -2 to container
17:40:18 [main] - Adding 9 to container
17:40:18 [main] - Subscriber received: [3, 5, -2, 9]
17:40:18 [main] - Subscriber got Completed event
```

because the usecase to store to a List container is so common, there is a **.toList()** operator that is just a collector adding to a List. 


## Merging Streams
Operators for working with multiple streams

### zip
Zip operator operates sort of like a zipper in the sense that it takes an event from one stream and waits
for an event from another other stream. Once an event for the other stream arrives, it uses the zip function
to merge the two events.

This is an useful scenario when for example you want to make requests to remote services in parallel and
wait for both responses before continuing. It also takes a function which will produce the combined result
of the zipped streams once each has emitted a value.

![Zip](https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/zip.png)


Zip operator besides the streams to zip, also takes as parameter a function which will produce the 
combined result of the zipped streams once each stream emitted it's value

```
Mono<Boolean> isUserBlockedStream = Mono.
                                     fromFuture(CompletableFuture.supplyAsync(() -> {
            Helpers.sleepMillis(200);
            return Boolean.FALSE;
}));

Mono<Integer> userCreditScoreStream = Mono.
                                       fromFuture(CompletableFuture.supplyAsync(() -> {
            Helpers.sleepMillis(2300);
            return 5;
}));

Flux<Pair<Boolean, Integer>> userCheckStream = Flux.
           zip(isUserBlockedStream, userCreditScoreStream, 
                      (blocked, creditScore) -> new Pair<Boolean, Integer>(blocked, creditScore));

userCheckStream.subscribe(pair -> log.info("Received " + pair));
```
Even if the 'isUserBlockedStream' finishes after 200ms, 'userCreditScoreStream' is slow at 2.3secs, 
the 'zip' method applies the combining function(new Pair(x,y)) after it received both values and passes it 
to the subscriber.

Another good example of 'zip' is to slow down a stream by another basically **implementing a periodic emitter of events**:
```  
Flux<String> colors = Flux.just("red", "green", "blue");
Flux<Long> timer = Flux.interval(2, TimeUnit.SECONDS);

Flux<String> periodicEmitter = Flux.zip(colors, timer, (key, val) -> key);
```
Since the zip operator needs a pair of events, the slow stream will work like a timer by periodically emitting 
with zip setting the pace of emissions downstream every 2 seconds.

**Zip is not limited to just two streams**, it can merge 2,3,4,.. streams and wait for groups of 2,3,4 'pairs' of events which it combines with the zip function and sends downstream.

### merge
Merge operator combines one or more stream and passes events downstream as soon as they appear.
![merge](https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/merge.png)
```
Flux<String> colors = periodicEmitter("red", "green", "blue", 2, TimeUnit.SECONDS);

Flux<Long> numbers = Flux.interval(Duration.of(1, ChronoUnit.SECONDS))
                         .take(5);
```

```
21:32:15 - Subscriber received: 0
21:32:16 - Subscriber received: red
21:32:16 - Subscriber received: 1
21:32:17 - Subscriber received: 2
21:32:18 - Subscriber received: green
21:32:18 - Subscriber received: 3
21:32:19 - Subscriber received: 4
21:32:20 - Subscriber received: blue
```

### concat
Concat operator appends another streams at the end of another
![concat](https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/concat.png)

```
Flux<String> colors = periodicEmitter("red", "green", "blue", 2, TimeUnit.SECONDS);

Flux<Long> numbers = Flux.interval(Duration.of(1, ChronoUnit.SECONDS))
                         .take(4);

Flux events = Flux.concat(colors, numbers);
```

```
22:48:23 - Subscriber received: red
22:48:25 - Subscriber received: green
22:48:27 - Subscriber received: blue
22:48:28 - Subscriber received: 0
22:48:29 - Subscriber received: 1
22:48:30 - Subscriber received: 2
22:48:31 - Subscriber received: 3
```

Even if the 'numbers' streams should start early, the 'colors' stream emits fully its events
before we see any 'numbers'.
This is because 'numbers' stream is actually subscribed only after the 'colors' complete.
Should the second stream be a 'hot' emitter, its events would be lost until the first one finishes
and the seconds stream is subscribed.


### Schedulers
RxJava provides some high level concepts for concurrent execution, like ExecutorService we're not dealing
with the low level constructs like creating the Threads ourselves. Instead we're using a **Scheduler** which create
Workers who are responsible for scheduling and running code. By default RxJava will not introduce concurrency 
and will run the operations on the subscription thread.

There are two methods through which we can introduce Schedulers into our chain of operations:

   - **subscribeOn** allows to specify which Scheduler invokes the code contained in the lambda code for Observable.create()
   - **observeOn** allows control to which Scheduler executes the code in the downstream operators

RxJava provides some general use Schedulers:
 
  - **Schedulers.computation()** - to be used for CPU intensive tasks. A threadpool. Should not be used for tasks involving blocking IO.
  - **Schedulers.io()** - to be used for IO bound tasks  
  - **Schedulers.from(Executor)** - custom ExecutorService
  - **Schedulers.newThread()** - always creates a new thread when a worker is needed. Since it's not thread pooled and 
  always creates a new thread instead of reusing one, this scheduler is not very useful 
 
Although we said by default RxJava doesn't introduce concurrency, lots of operators involve waiting like **delay**,
**interval**, **zip** need to run on a Scheduler, otherwise they would just block the subscribing thread. 
By default **Schedulers.computation()** is used, but the Scheduler can be passed as a parameter.


## Flatmap operator
The flatMap operator is so important and has so many different uses it deserves it's own category to explain it.

I like to think of it as a sort of **fork-join** operation because what flatMap does is it takes individual stream items
and maps each of them to an Observable(so it creates new Streams from each object) and then 'flattens' the events from 
these Streams back as coming from a single stream.

Why this looks like fork-join because for each element you can fork some jobs that keeps emitting results,
and these results are emitted back as elements to the subscribers downstream
![flatmap](https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/flatmap.png)


Rules of thumb to consider before getting comfortable with flatMap: 
   
   - When you have an 'item' and you'll get Flux&lt;X&gt;, instead of X, you need flatMap. Most common example is when you want 
   to make a remote call that returns an Observable. For ex if you have a stream of customerIds, and downstream you
    want to work with actual Customer objects:
   ```   
   Flux<Customer> getCustomer(Integer customerId) {..}
    ...
   
   Flux<Integer> customerIds = Flux.of(1,2,3,4);
   Flux<Customer> customers = customerIds
                                   .flatMap(customerId -> getCustomer(customerId));
   ```
   
   - When you have Flux&lt;Flux&lt;T&gt;&gt; you probably need flatMap.

We use a simulated remote call that might return asynchronous as many events as the length of the color string
```
    private Flux<String> simulateRemoteOperation(String color) {
        return Flux.<String>create(subscriber -> {
                    Runnable asyncRun = () -> {
                        for (int i = 0; i < color.length(); i++) {
                            subscriber.onNext(color + i);
                            Helpers.sleepMillis(200);
                        }
        
                        subscriber.onCompleted();
                    };
                    new Thread(asyncRun).start();
                });
    }
```

If we have a stream of color names:
```
Flux<String> colors = Flux.just("orange", "red", "green")
```
to invoke the remote operation: 
```
Flux<String> colors = Flux.just("orange", "red", "green")
         .flatMap(colorName -> simulateRemoteOperation(colorName));

colors.subscribe(val -> log.info("Subscriber received: {}", val));         
```
returns
```
16:44:15 [Thread-0]- Subscriber received: orange0
16:44:15 [Thread-2]- Subscriber received: green0
16:44:15 [Thread-1]- Subscriber received: red0
16:44:15 [Thread-0]- Subscriber received: orange1
16:44:15 [Thread-2]- Subscriber received: green1
16:44:15 [Thread-1]- Subscriber received: red1
16:44:15 [Thread-0]- Subscriber received: orange2
16:44:15 [Thread-2]- Subscriber received: green2
16:44:15 [Thread-1]- Subscriber received: red2
16:44:15 [Thread-0]- Subscriber received: orange3
16:44:15 [Thread-2]- Subscriber received: green3
16:44:16 [Thread-0]- Subscriber received: orange4
16:44:16 [Thread-2]- Subscriber received: green4
16:44:16 [Thread-0]- Subscriber received: orange5
```


Notice how the results are coming intertwined. This is because flatMap actually subscribes to it's inner Flux 
returned from 'simulateRemoteOperation'. You can specify the concurrency level of flatMap as a parameter. Meaning 
you can say how many of the substreams should be subscribed "concurrently" - aka before the onComplete 
event is triggered on the substreams.
By setting the concurrency to **1** we don't subscribe to other substreams until the current one finishes:


```
Flux<String> colors = Flux.just("orange", "red", "green")
                     .flatMap(val -> simulateRemoteOperation(val), 1); //

```
Notice now there is a sequence from each color before the next one appears
```
17:15:24 [Thread-0]- Subscriber received: orange0
17:15:24 [Thread-0]- Subscriber received: orange1
17:15:25 [Thread-0]- Subscriber received: orange2
17:15:25 [Thread-0]- Subscriber received: orange3
17:15:25 [Thread-0]- Subscriber received: orange4
17:15:25 [Thread-0]- Subscriber received: orange5
17:15:25 [Thread-1]- Subscriber received: red0
17:15:26 [Thread-1]- Subscriber received: red1
17:15:26 [Thread-1]- Subscriber received: red2
17:15:26 [Thread-2]- Subscriber received: green0
17:15:26 [Thread-2]- Subscriber received: green1
17:15:26 [Thread-2]- Subscriber received: green2
17:15:27 [Thread-2]- Subscriber received: green3
17:15:27 [Thread-2]- Subscriber received: green4
```

There is actually an operator which is basically this flatMap with 1 concurrency called **concatMap**.
![concatMap](https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/concatmap.png)

### flatMap substreams are still streams
Inside the flatMap we can operate on the substream with the same stream operators
```
Flux<Pair<String, Integer>> colorsCounted = colors
    .flatMap(colorName -> {
               Flux<Long> timer = Flux.interval(Duration.of(2, ChronoUnit.SECONDS));

               return simulateRemoteOperation(colorName) // <- Still a stream
                              .zipWith(timer, (val, timerVal) -> val)
                              .count()
                              .map(counter -> new Pair<>(colorName, counter));
               }
    );
```

### flatMap can override 'error' and 'complete' event
**flatMap** in it's most complex form allows to override the 'error' and 'complete' event not just the 'next' event

Here we introduce another event on window stream completion
```
Flux<String> colors = Flux.just("red", "green", "blue", "red", "yellow", "green", "green");
Flux remastered = colors
                    .window(2)
                    .flatMap(window -> window.flatMap(Flux::just,
                                                      Flux::error,
                                                      () -> Flux.just("===")
                                              )
                    );
```
produces
```
[main] - Subscriber received: red
[main] - Subscriber received: green
[main] - Subscriber received: ===
[main] - Subscriber received: blue
[main] - Subscriber received: red
[main] - Subscriber received: ===
[main] - Subscriber received: yellow
[main] - Subscriber received: green
[main] - Subscriber received: ===
[main] - Subscriber received: green
[main] - Subscriber received: ===
[main] - Subscriber got Completed event
```

### Schedulers
Reactor provides some high level concepts for concurrent execution, like ExecutorService we're not dealing
with the low level constructs like creating the Threads ourselves. Instead we're using a **Scheduler** which create
Workers who are responsible for scheduling and running code. By default Reactor will not introduce concurrency 
and will run the operations on the subscription thread(the one that calls flux.subscribe(...)).

There are two methods through which we can introduce Schedulers into our chain of operations:

   - **subscribeOn** allows to specify which Scheduler invokes the code contained in the lambda code for Flux.create() -where the source runs
    it's event producing code-.
   - **publishOn** allows control to which Scheduler executes the code in the downstream operators

Reactor provides some general use scheduler strategies:
 
  - **Schedulers.parallel()** - good for parallelization in general. It uses a fixed threadpool, so backlog can increase
  if there is congestion.
  - **Schedulers.elastic()** - scheduler that adjusts its capacity according to demand. Good for IO/blocking as it will 
  pool threads and keep increasing the size of the pool if current threads are in use. Threads are cleared after some time being idle.     
  - **Schedulers.from(Executor)** - custom ExecutorService
  - **Schedulers.single()** - uses a single thread - equivalent of parallel(1)- should be good enough if the whole code is non blocking.
 
Although we said by default Reactor doesn't introduce concurrency, lots of operators involve waiting like **delay**,
**interval** need to run on a Scheduler, otherwise they would just block the subscribing thread. 
By default **Schedulers.timer()** is used, but the Scheduler can be passed as a parameter.

## Advanced operators

### groupBy
groupBy splits a stream into multiple streams(a stream of streams), by a grouping function which provides the key
for the grouping. These substreams are GroupedFlux which consists of the grouping key and a value.

```java
//the stream of events is transformed into a stream of streams
Flux<T>  ->  Flux<GroupedFlux<K, T>>
```

![groupBy](https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/groupby.png)


In the following example we use the identity function _.groupBy(val -> val)_ when generating the grouping keys
meaning that the keys are also the color values: 
```
Flux<String> colors = Flux.fromArray(new String[]{"red", "green", "blue",
                "red", "yellow", "green", "green"});

Flux<GroupedFlux<String, String>> groupedColorsStream = colors
                .groupBy(val -> val); //identity function
//                .groupBy(val -> "length" + val.length());


Flux<Pair<String, Long>> colorCountStream = groupedColorsStream
                .flatMap(groupedColor -> groupedColor
                                            .count()
                                            .map(count -> new Pair<>(groupedColor.key(), count))
                );
```

```
16:10:59 - Subscriber received: red=2
16:10:59 - Subscriber received: green=3
16:10:59 - Subscriber received: blue=1
16:10:59 - Subscriber received: yellow=1
16:10:59 - Subscriber got Completed event
```

## Error handling
Exceptions are for exceptional situations.
The Reactive Streams specification says that exceptions are terminal operations. 
That means in case an error reaches the Subscriber, after invoking the 'onError' handler, it also unsubscribes:

```
Flux<String> colors = Flux.just("green", "blue", "red", "yellow")
       .map(color -> {
              if ("red".equals(color)) {
                        throw new RuntimeException("Encountered red");
              }
              return color + "*";
       })
       .map(val -> val + "XXX");

colors.subscribe(
         val -> log.info("Subscriber received: {}", val),
         exception -> log.error("Subscriber received error '{}'", exception.getMessage()),
         () -> log.info("Subscriber completed")
);
```
returns:
```
23:30:17 [main] INFO - Subscriber received: green*XXX
23:30:17 [main] INFO - Subscriber received: blue*XXX
23:30:17 [main] ERROR - Subscriber received error 'Encountered red'
```
After the map() operator encounters an error, it triggers the error handler in the subscriber which also unsubscribes(cancels the subscription) from the stream,
therefore 'yellow' is not even sent downstream.

However there are operators to deal with error flow control. 

Let's introduce a more realcase scenario of a simulated remote request that might fail 
```
private Flux<String> simulateRemoteOperation(String color) {
    return Flux.<String>create(subscriber -> {
         if ("red".equals(color)) {
              log.info("Emitting RuntimeException for {}", color);
              throw new RuntimeException("Color red raises exception");
         }
         if ("black".equals(color)) {
              log.info("Emitting IllegalArgumentException for {}", color);
              throw new IllegalArgumentException("Black is not a color");
         }

         String value = "**" + color + "**";

         log.info("Emitting {}", value);
         subscriber.next(value);
         subscriber.complete();
    });
}
```


### onErrorReturn

The 'onErrorReturn' operator replaces an exception with a value:

```
Flux<String> colors = Flux.just("green", "blue", "red", "white", "blue")
                .flatMap(color -> simulateRemoteOperation(color))
                .onErrorReturn(throwable -> "-blank-");
                
subscribeWithLog(colors);
```
returns:
```
22:15:51 [main] INFO - Emitting **green**
22:15:51 [main] INFO - Subscriber received: **green**
22:15:51 [main] INFO - Emitting **blue**
22:15:51 [main] INFO - Subscriber received: **blue**
22:15:51 [main] INFO - Emitting RuntimeException for red
22:15:51 [main] INFO - Subscriber received: -blank-
22:15:51 [main] INFO - Subscriber got Completed event
```
flatMap encounters an error when it subscribes to 'red' substreams and thus still unsubscribe from 'colors' 
stream and the remaining colors are not longer emitted

```
Flux<String> colors = Flux.just("green", "blue", "red", "white", "blue")
                .flatMap(color -> simulateRemoteOperation(color)
                                    .onErrorReturn(throwable -> "-blank-")
                );
```
onErrorReturn() is applied to the flatMap substream and thus translates the exception to a value and so flatMap 
continues on with the other colors after red

returns:
```
22:15:51 [main] INFO - Emitting **green**
22:15:51 [main] INFO - Subscriber received: **green**
22:15:51 [main] INFO - Emitting **blue**
22:15:51 [main] INFO - Subscriber received: **blue**
22:15:51 [main] INFO - Emitting RuntimeException for red
22:15:51 [main] INFO - Subscriber received: -blank-
22:15:51 [main] INFO - Emitting **white**
22:15:51 [main] INFO - Subscriber received: **white**
22:15:51 [main] INFO - Emitting **blue**
22:15:51 [main] INFO - Subscriber received: **blue**
22:15:51 [main] INFO - Subscriber got Completed event
```

### onErrorResumeWith
onErrorResumeWith() returns a stream instead of an exception, useful for example to invoke a fallback 
method that returns also a stream

```
Observable<String> colors = Observable.just("green", "blue", "red", "white", "blue")
     .flatMap(color -> simulateRemoteOperation(color)
                        .onErrorResumeNext(th -> {
                            if (th instanceof IllegalArgumentException) {
                                return Observable.error(new RuntimeException("Fatal, wrong arguments"));
                            }
                            return fallbackRemoteOperation();
                        })
     );
...
private Observable<String> fallbackRemoteOperation() {
        return Observable.just("blank");
}
```

## Retrying

### timeout()
Timeout operator raises exception when there are no events incoming before it's predecessor in the specified time limit.

### retry()
**retry()** - resubscribes in case of exception to the Observable

```
Observable<String> colors = Observable.just("red", "blue", "green", "yellow")
       .concatMap(color -> delayedByLengthEmitter(TimeUnit.SECONDS, color) //we're delaying by string length secs
                             .timeout(6, TimeUnit.SECONDS) //if there are no events flowing in the timeframe 
                             .retry(2)
                             .onErrorResumeNext(Observable.just("blank"))
       );

subscribeWithLog(colors.toBlocking());
```

returns
```
12:40:16 [main] INFO - Received red delaying for 3 
12:40:19 [main] INFO - Subscriber received: red
12:40:19 [RxComputationScheduler-2] INFO - Received blue delaying for 4 
12:40:23 [main] INFO - Subscriber received: blue
12:40:23 [RxComputationScheduler-4] INFO - Received green delaying for 5 
12:40:28 [main] INFO - Subscriber received: green
12:40:28 [RxComputationScheduler-6] INFO - Received yellow delaying for 6 
12:40:34 [RxComputationScheduler-7] INFO - Received yellow delaying for 6 
12:40:40 [RxComputationScheduler-1] INFO - Received yellow delaying for 6 
12:40:46 [main] INFO - Subscriber received: blank
12:40:46 [main] INFO - Subscriber got Completed event
```

When you want to retry considering the thrown exception type:
```
Observable<String> colors = Observable.just("blue", "red", "black", "yellow")
         .flatMap(colorName -> simulateRemoteOperation(colorName)
                .retry((retryAttempt, exception) -> {
                           if (exception instanceof IllegalArgumentException) {
                               log.error("{} encountered non retry exception ", colorName);
                               return false;
                           }
                           log.info("Retry attempt {} for {}", retryAttempt, colorName);
                           return retryAttempt <= 2;
                })
                .onErrorResumeNext(Observable.just("generic color"))
         );
```

```
13:21:37 [main] INFO - Emitting **blue**
13:21:37 [main] INFO - Emitting RuntimeException for red
13:21:37 [main] INFO - Retry attempt 1 for red
13:21:37 [main] INFO - Emitting RuntimeException for red
13:21:37 [main] INFO - Retry attempt 2 for red
13:21:37 [main] INFO - Emitting RuntimeException for red
13:21:37 [main] INFO - Retry attempt 3 for red
13:21:37 [main] INFO - Emitting IllegalArgumentException for black
13:21:37 [main] ERROR - black encountered non retry exception 
13:21:37 [main] INFO - Emitting **yellow**
13:21:37 [main] INFO - Subscriber received: **blue**
13:21:37 [main] INFO - Subscriber received: generic color
13:21:37 [main] INFO - Subscriber received: generic color
13:21:37 [main] INFO - Subscriber received: **yellow**
13:21:37 [main] INFO - Subscriber got Completed event
```

### retryWhen
A more complex retry logic like implementing a backoff strategy in case of exception
This can be obtained with **retryWhen**(exceptionObservable -> Observable)

retryWhen resubscribes when an event from an Observable is emitted. It receives as parameter an exception stream
     
we zip the exceptionsStream with a .range() stream to obtain the number of retries,
however we want to wait a little before retrying so in the zip function we return a delayed event - .timer()

The delay also needs to be subscribed to be effected so we also flatMap

```
Flux<String> colors = Flux.just("blue", "green", "red", "black", "yellow");

colors.flatMap(colorName -> 
                    simulateRemoteOperation(colorName)
                           .retryWhen(exceptionStream -> exceptionStream
                                        .zipWith(Observable.range(1, 3), (exc, attempts) -> {
                                                //don't retry for IllegalArgumentException
                                                if(exc instanceof IllegalArgumentException) {
                                                     return Observable.error(exc);
                                                }

                                                if(attempts < 3) {
                                                     return Observable.timer(2 * attempts, TimeUnit.SECONDS);
                                                }
                                                return Observable.error(exc);
                                          })
                                          .flatMap(val -> val)
                               )
                               .onErrorResumeNext(Observable.just("generic color")
                        )
            );
```

```
15:20:23 [main] INFO - Emitting **blue**
15:20:23 [main] INFO - Emitting **green**
15:20:23 [main] INFO - Emitting RuntimeException for red
15:20:23 [main] INFO - Emitting IllegalArgumentException for black
15:20:23 [main] INFO - Emitting **yellow**
15:20:23 [main] INFO - Subscriber received: **blue**
15:20:23 [main] INFO - Subscriber received: **green**
15:20:23 [main] INFO - Subscriber received: generic color
15:20:23 [main] INFO - Subscriber received: **yellow**
15:20:25 [RxComputationScheduler-1] INFO - Emitting RuntimeException for red
15:20:29 [RxComputationScheduler-2] INFO - Emitting RuntimeException for red
15:20:29 [main] INFO - Subscriber received: generic color
15:20:29 [main] INFO - Subscriber got Completed event
```

## Backpressure

It can be the case of a slow consumer that cannot keep up with the producer that is producing too many events
that the subscriber cannot process. 

Backpressure relates to a feedback mechanism through which the subscriber can signal to the producer how much data 
it can consume.

The [reactive-streams](https://github.com/reactive-streams/reactive-streams-jvm) section above we saw that besides the onNext, onError and onComplete handlers, the Subscriber
has an **onSubscribe(Subscription)**
 
```
public interface Subscriber<T> {
    //signals to the Publisher to start sending events
    public void onSubscribe(Subscription s);     
    
    public void onNext(T t);
    public void onError(Throwable t);
    public void onComplete();
}
```
Subscription through which it can signal upstream it's ready to receive a number of items and after it
processes the items request another batch.

```
public interface Subscription {
    public void request(long n); //request n items
    public void cancel();
}
```
So in theory the Subscriber can prevent being overloaded by requesting an initial number of items. The Publisher would
send those items downstream and not produce any more, until the Subscriber would request more. We say in theory because
until now we did not see a custom **onSubscribe** request being implemented. This is because if not specified explicitly,
there is a default implementation which requests of **Integer.MAX_VALUE** which basically means "send all you have".

Neither did we see the code in the producer that takes consideration of the number of items requested by the subscriber. 
```
Flux.create(subscriber -> {
      log.info("Started emitting");

      for(int i=0; i < 300; i++) {
           if(subscriber.isCanceled()) {
              return;
           }
           log.info("Emitting {}", i);
           subscriber.next(i);
      }

      subscriber.complete();
}
```
Looks like it's not possible to slow down production based on request(as there is no reference to the requested items),
we can at most stop production if the subscriber canceled subscription. 

This can be done if we extend Flux/Mono so we can pass our custom Subscription type to the downstream subscriber:  
```
class CustomRangeFlux extends Flux<Integer> {
    private int startFrom;
    private int count;

    CustomRangeFlux(int startFrom, int count) {
        this.startFrom = startFrom;
        this.count = count;
    }

    @Override
    public void subscribe(Subscriber<? super Integer> subscriber) {
        subscriber.onSubscribe(
                    new CustomRangeSubscription(startFrom, count, subscriber));
    }
}

private class CustomRangeSubscription implements Subscription {

     volatile boolean cancelled;
     private int count;
     private int currentCount;
     private int startFrom;

     private Subscriber<? super Integer> actualSubscriber;

     CustomRangeSubscription(int startFrom, int count, 
                                    Subscriber<? super Integer> actualSubscriber) {
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
```

and using our custom Flux that knows to produce just as many items that the Subscriber **requests**:
```
Flux<Integer> flux = new CustomFlux(5, 10)
                             .log();

//we request a limited number of items
flux.subscribe(val -> log.info("Subscriber received: {}", val), 3);

[main] - onSubscribe(com.balamaci.reactor.Part09BackpressureHandling
                        $CustomFlux$CustomRangeSubscription@7f560810)
[main] - request(3)
[main] - onNext(5)
[main] - Subscriber received: 5
[main] - onNext(6)
[main] - Subscriber received: 6
[main] - onNext(7)
[main] - Subscriber received: 7
[main] - request(3)
[main] - onNext(7)
[main] - onNext(8)
[main] - onNext(9)
[main] - Subscriber received: 7
[main] - Subscriber received: 8
[main] - Subscriber received: 9
[main] - request(3)
[main] - onNext(10)
[main] - onNext(11)
[main] - onNext(12)
[main] - Subscriber received: 10
[main] - Subscriber received: 11
[main] - Subscriber received: 12
[main] - request(3)
[main] - onNext(13)
[main] - onNext(14)
[main] - onComplete()
[main] - Subscriber received: 13
[main] - Subscriber received: 14
```
So does it mean that streams created with _Flux.create()_ are destined to failed for a slow subscriber?  
And what about hot publishers(that emit indifferent of any subscribers listening)?

If we look at the definition of _Flux.create()_ 
```
public static <T> Flux<T> create(Consumer<? super FluxSink<T>> emitter) {
	return create(emitter, OverflowStrategy.BUFFER);
}
```
there is this OverflowStrategy that specifies how to deal with too many events that overflow the consumers:
   - OverflowStrategy.BUFFER buffer in memory the events that overflow. Of course is we don't drop over some threshold, it might lead to OufOfMemory. 
   - OverflowStrategy.DROP just drop the overflowing events
   - OverflowStrategy.LATEST drop queued older events and keep more recent
   - OverflowStrategy.ERROR we get an error in the subscriber immediately 
    
Still what does it mean to 'overflow' in the first place? It means to emit more items than requested downstream.
But we said that by default the subscriber requests Integer.MAX_VALUE, so unless we override the request number 
it means it would never overflow. True, but between the Publisher and the Subscriber you'd have a series of operators. 
When we subscribe, a Subscriber travels up through all operators to the original Publisher and some operators override 
the requested items. One such operator is **publishOn**() which makes it's own request to the upstream Publisher(256 by default),
but can take a parameter to specify the request size.
 
Let's see:
```
Flux<Integer> publisher = Flux.create(subscriber -> {
      log.info("Started emitting");

      for(int i=0; i < 10; i++) {
           log.info("Emitting {}", i);
           subscriber.next(i);
      }

      subscriber.complete();
}, OverflowStrategy.DROP);

publisher = publisher.log()
                     .publishOn(Schedulers.newElastic("elast"), 5);


publisher.subscribe(val -> {
                      log.info("Subscriber received: {}", val);
                      Helpers.sleepMillis(50); //slow down 
                  });

=======================
[main] 1 - onSubscribe(reactor.core.publisher.FluxCreate$DropAsyncSink@50c87b21)
[main] 1 - request(5)
[main] BaseTestFlux - Started emitting
[main] BaseTestFlux - Emitting 0
[main] 1 - onNext(0)
[main] BaseTestFlux - Emitting 1
[main] 1 - onNext(1)
[main] BaseTestFlux - Emitting 2
[main] 1 - onNext(2)
[elast-2] BaseTestFlux - Subscriber received: 0
[main] BaseTestFlux - Emitting 3
[main] 1 - onNext(3)
[main] BaseTestFlux - Emitting 4
[main] 1 - onNext(4)
[main] BaseTestFlux - Emitting 5
[main] BaseTestFlux - Emitting 6
[main] BaseTestFlux - Emitting 7
[main] BaseTestFlux - Emitting 8
[main] BaseTestFlux - Emitting 9
[main] 1 - onComplete()
[elast-2] BaseTestFlux - Subscriber received: 1
[elast-2] BaseTestFlux - Subscriber received: 2
[elast-2] BaseTestFlux - Subscriber received: 3
[elast-2] 1 - request(4)
[elast-2] BaseTestFlux - Subscriber received: 4
```
Notice how the log show that the request to the source Flux.create(..) is for 5(as requested in _publishOn_), 
not the Integer.MAX_VALUE(which is the default) and also that overflown events(produced over the 5 requested), were dropped as they were not received by the
onNext callback in the Subscriber. 
You may think that publishOn was force put there just for the sake of showing an example of overriding number of requests in an operator, 
but it's because we need _publishOn_ to have the producer and consumer use different threads. 
If both of them would have used the same thread, we could not have an overflow scenario where the producer is 
emitting faster(since it has to wait for the subscriber).

