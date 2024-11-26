package gr.uoa.di.promise;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.lang.module.ResolutionException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


/*
 * > "What I cannot create, I do not understand"
 * > Richard Feynman
 * > https://en.wikipedia.org/wiki/Richard_Feynman
 * 
 * This is an incomplete implementation of the Javascript Promise machinery in Java.
 * You should expand and ultimately complete it according to the following:
 * 
 * (1) You should only use the low-level Java concurrency primitives (like 
 * java.lang.Thread/Runnable, wait/notify, synchronized, volatile, etc)
 * in your implementation. 
 * 
 * (2) The members of the java.util.concurrent package 
 * (such as Executor, Future, CompletableFuture, etc.) cannot be used.
 * 
 * (3) No other library should be used.
 * 
 * (4) Create as many threads as you think appropriate and don't worry about
 * recycling them or implementing a Thread Pool.
 * 
 * (5) I may have missed something from the spec, so please report any issues
 * in the course's e-class.
 * 
 * The Javascript Promise reference is here:
 * https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise
 * 
 * A helpful guide to help you understand Promises is available here:
 * https://javascript.info/async
 */
public class Promise<V> {

    public static enum Status {
        PENDING,
        FULFILLED,
        REJECTED
    }

    class PromiseData {

        private Status promiseStatus;
        private ValueOrError<V> returnValue;
        private final Queue<Function<V, ?>> fulfilledQueue;
        private final Queue<Consumer<Throwable>> rejectedQueue;
    
        public PromiseData() {
            promiseStatus = Status.PENDING;
            returnValue = null;
            fulfilledQueue = new LinkedList<>();
            rejectedQueue = new LinkedList<>();
        }
    
        public synchronized Status getPromiseStatus() {
            return promiseStatus;
        }
    
        public synchronized void setPromiseStatus(Status status) {
            this.promiseStatus = status;
        }
    
        public synchronized ValueOrError<V> getReturnValue() {
            return returnValue;
        }
    
        public synchronized void setReturnValue(ValueOrError<V> returnValue) {
            this.returnValue = returnValue;
        }
    
        public synchronized void addToFulfQueue(Function<V, ?> callback) {
            fulfilledQueue.add(callback);
        }
    
        public synchronized Function<V, ?> getNextResolve() {
            return fulfilledQueue.poll();
        }
    
        public synchronized boolean hasNextResolve() {
            return !fulfilledQueue.isEmpty();
        }
    
        public synchronized void addToRejQueue(Consumer<Throwable> callback) {
            rejectedQueue.add(callback);
        }
    
        public synchronized Consumer<Throwable> getNextReject() {
            return rejectedQueue.poll();
        }
    
        public synchronized boolean hasNextReject() {
            return !rejectedQueue.isEmpty();
        }
    }

    public PromiseData promData;

    public Promise(PromiseExecutor<V> executor) {
        this.promData = new PromiseData();
        startPromise(executor);
    }

    private void startPromise(PromiseExecutor<V> executor) {
        new Thread(() -> {
            executor.execute(this::onResolve, this::onReject);
        }).start();
    }

    private void onResolve(V value) {
        if (promData.getPromiseStatus() != Status.PENDING) 
            return;

        promData.setPromiseStatus(Status.FULFILLED);
        promData.setReturnValue(ValueOrError.Value.of(value));

        while (promData.hasNextResolve()) {
            Function<V, ?> callback = promData.getNextResolve();
            callback.apply(value);
        }
    }

    private void onReject(Throwable error) {
        if (promData.getPromiseStatus() != Status.PENDING) 
            return;

        promData.setPromiseStatus(Status.REJECTED);
        promData.setReturnValue(ValueOrError.Error.of(error));

        while (promData.hasNextReject()) {
            Consumer<Throwable> callback = promData.getNextReject();
            callback.accept(error);
        }
    }

    public <T> Promise<T> then(Function<V, T> onResolve, Consumer<Throwable> onReject) {
        return new Promise<>((resolve, reject) -> {
            if (promData.getPromiseStatus() == Status.PENDING) {
                
                promData.addToFulfQueue(value -> {
                    try {
                        T result = onResolve.apply(value);
                        resolve.accept(result);
                        return result;
                    } 
                    catch (Exception e) {
                        reject.accept(e);
                        return null;
                    }
                });

                promData.addToRejQueue(error -> {
                    try {
                        onReject.accept(error); 
                        reject.accept(error);
                    } 
                    catch (Exception e) {
                        reject.accept(e);
                    }
                });
            } 
            else if (promData.getPromiseStatus() == Status.FULFILLED) {
                try {
                    T result = onResolve.apply(promData.getReturnValue().value());
                    resolve.accept(result);
                } 
                catch (Exception e) {
                    reject.accept(e);
                }
            }
            else if (promData.getPromiseStatus() == Status.REJECTED) {
                try {
                    onReject.accept(promData.getReturnValue().error());
                    reject.accept(promData.getReturnValue().error());
                } 
                catch (Exception e) {
                    reject.accept(e);
                }
            }
        });
    }

    public <T> Promise<T> then(Function<V, T> onResolve) {
        return then(onResolve, null);
    }

    // catch is a reserved word in Java.
    public Promise<?> catchError(Consumer<Throwable> onReject) {
        return then(null, onReject);
    }

    public Promise<V> andFinally(Consumer<ValueOrError<V>> onSettle) {
        return new Promise<>((resolve, reject) -> {
            if (promData.getPromiseStatus() == Status.PENDING) {

                promData.addToFulfQueue(value -> {
                    onSettle.accept(ValueOrError.Value.of(value));
                    resolve.accept(value);
                    return null;
                });

                promData.addToRejQueue(error -> {
                    onSettle.accept(ValueOrError.Error.of(error));
                    reject.accept(error);
                });
            } 
            else {
                if (promData.getPromiseStatus() == Status.FULFILLED) {
                    V value = promData.getReturnValue().value();
                    onSettle.accept(ValueOrError.Value.of(value));
                    resolve.accept(value);
                } 
                else if (promData.getPromiseStatus() == Status.REJECTED) {
                    Throwable error = promData.getReturnValue().error();
                    onSettle.accept(ValueOrError.Error.of(error));
                    reject.accept(error);
                }
            }
        });
    }

    public static <T> Promise<T> resolve(T value) {
        return new Promise<>((resolve, reject) -> {
            resolve.accept(value);
        });
    }

    public static Promise<Void> reject(Throwable error) {
        return new Promise<>((resolve, reject) -> {
            reject.accept(error);
        });
    }

    public static Promise<ValueOrError<?>> race(List<Promise<?>> promises) {
        return new Promise<>((resolve, reject) -> {
            for (Promise<?> promise : promises) {
                promise.then((result) -> {
                    resolve.accept(ValueOrError.Value.of(result));
                    return result;
                },
                (result) -> {
                    reject.accept(result);
                });
            }
        });
    }

    public static Promise<?> any(List<Promise<?>> promises) {
        if (promises.isEmpty()) {
            // Immediately reject if the input list is empty
            return new Promise<>((resolve, reject) -> {
                reject.accept(new Throwable("No promises provided"));
            });
        }
    
        AtomicBoolean resolved = new AtomicBoolean(false);
        AtomicInteger remaining = new AtomicInteger(promises.size());
    
        return new Promise<>((resolve, reject) -> {
            for (Promise<?> promise : promises) {
                promise.then(
                    (result) -> {
                        // Resolve as soon as one promise fulfills
                        if (resolved.compareAndSet(false, true)) {
                            resolve.accept(result);
                        }
                        return result;
                    },
                    (error) -> {
                        // Check if all promises are rejected
                        if (remaining.decrementAndGet() == 0 && resolved.compareAndSet(false, true)) {
                            reject.accept(new Throwable("All promises were rejected"));
                        }
                    }
                );
            }
        });
    }
    public static Promise<List<?>> all(List<Promise<?>> promises) {
    if (promises.isEmpty()) {
        // Return immediately with an empty list if no promises are provided
        return new Promise<>((resolve, reject) -> resolve.accept(new LinkedList<>()));
    }

    List<Object> retList = Collections.synchronizedList(new ArrayList<>());
    AtomicInteger remaining = new AtomicInteger(promises.size());
    AtomicBoolean rejected = new AtomicBoolean(false);

    return new Promise<>((resolve, reject) -> {
        for (int i = 0; i < promises.size(); i++) {
            final int index = i; // Capture index for ordered results
            Promise<?> promise = promises.get(i);

            promise.then(
                (result) -> {
                    synchronized (retList) {
                        // Store the result in the correct order
                        while (retList.size() <= index) {
                            retList.add(null); // Ensure capacity
                        }
                        retList.set(index, result);
                    }

                    // Check if all promises have resolved
                    if (remaining.decrementAndGet() == 0 && !rejected.get()) {
                        resolve.accept(retList);
                    }
                    return result;
                },
                (error) -> {
                    // Reject immediately on the first error
                    if (rejected.compareAndSet(false, true)) {
                        reject.accept(error);
                    }
                }
            );
        }
    });
}

public static Promise<List<ValueOrError<?>>> allSettled(List<Promise<?>> promises) {
    return new Promise<>((resolve, reject) -> {
        if (promises.isEmpty()) {
            // If no promises are provided, resolve immediately with an empty list
            resolve.accept(Collections.emptyList());
            return;
        }

        List<ValueOrError<?>> results = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger remaining = new AtomicInteger(promises.size());

        for (int i = 0; i < promises.size(); i++) {
            final int index = i; // Capture index for preserving order
            Promise<?> promise = promises.get(i);

            promise.then(
                (result) -> {
                    synchronized (results) {
                        // Ensure list size matches the input size
                        while (results.size() <= index) {
                            results.add(null);
                        }
                        results.set(index, ValueOrError.Value.of(result));
                    }
                    if (remaining.decrementAndGet() == 0) {
                        resolve.accept(results);
                    }
                    return result;
                },
                (error) -> {
                    synchronized (results) {
                        // Ensure list size matches the input size
                        while (results.size() <= index) {
                            results.add(null);
                        }
                        results.set(index, ValueOrError.Error.of(error));
                    }
                    if (remaining.decrementAndGet() == 0) {
                        resolve.accept(results);
                    }
                }
            );
        }
    });
}

}
