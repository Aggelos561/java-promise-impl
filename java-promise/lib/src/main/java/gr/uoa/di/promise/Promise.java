package gr.uoa.di.promise;

import java.util.List;
import java.util.ArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
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

    // Promise state update
    private volatile Status promiseStatus;

    // Get the return value of each settled promise
    private volatile ValueOrError<V> returnValue;

    public Promise(PromiseExecutor<V> executor) {
        // Init promise status as 'pending' and create a new thread for each promise
        promiseStatus = Status.PENDING;
        makePromise(executor);
    }

    private void makePromise(PromiseExecutor<V> executor) {
        // New promise thread
        new Thread(() -> {
            executor.execute(this::resolvePromise, this::rejectPromise);
        }).start();
    }

    private synchronized void resolvePromise(V value) {

        // Resolve/reject once
        if (promiseStatus != Status.PENDING)
            return;

        // Update status and store return value
        promiseStatus = Status.FULFILLED;
        returnValue = ValueOrError.Value.of(value);
        
        // Source promise thread needs to wake up all the pending promises
        notifyAll();
    }

    private synchronized void rejectPromise(Throwable error) {

        // Resolve/reject once
        if (promiseStatus != Status.PENDING)
            return;

        // Update status and store return value
        promiseStatus = Status.REJECTED;
        returnValue = ValueOrError.Error.of(error);

        // Source promise thread needs to wake up all the pending promises
        notifyAll();
    }

    // Wait for the promise to be settled before continuing down the 'chain'
    private synchronized void waitPendingPromise() {
        while (promiseStatus == Status.PENDING) {
            try {
                wait();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // Used in 'then' function. Based on the promise status call resolve/reject methods
    private <T> void settlePromise(Function<V, T> onResolve, Consumer<Throwable> onReject, 
                                Consumer<T> resolve, Consumer<Throwable> reject)
    {
        try {
            if (promiseStatus == Status.FULFILLED) {
                V value = returnValue.value();
                T result = onResolve.apply(value);
                resolve.accept(result);
            }
            else if (promiseStatus == Status.REJECTED) {
                Throwable error = returnValue.error();
                onReject.accept(error);
                reject.accept(error);
            }
        }
        catch (Exception e) {
            reject.accept(e);
        }
    }

    public <T> Promise<T> then(Function<V, T> onResolve, Consumer<Throwable> onReject) {
        return new Promise<>((resolve, reject) -> {
            waitPendingPromise(); // Wait if needed
            settlePromise(onResolve, onReject, resolve, reject); // Then settle the promise!
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
        return then
            ((value) -> {
                onSettle.accept(ValueOrError.Value.of(value));
                return value;
            },
            (error) -> {
                onSettle.accept(ValueOrError.Error.of(error));
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

    // Return first promise that settles
    public static Promise<ValueOrError<?>> race(List<Promise<?>> promises) {
        return new Promise<>((resolve, reject) -> {
            for (Promise<?> promise : promises) {
                promise.then((value) -> {
                    resolve.accept(ValueOrError.Value.of(value));
                    return value;
                },
                (error) -> {
                    reject.accept(error);
                });
            }
        });
    }

    // Get first fulfillment value. Reject only when all promises reject
    public static Promise<?> any(List<Promise<?>> promises) {
        if (promises.isEmpty())
            return new Promise<>((resolve, reject) -> {
                reject.accept(new Throwable("Empty List Of Promises"));
            });
        
        // Count remaining non-rejected promises
        AtomicInteger remPromises = new AtomicInteger(promises.size());
        return new Promise<>((resolve, reject) -> {
            for (Promise<?> promise : promises) {
                promise.then(
                    (value) -> {
                        resolve.accept(value);
                        return value;
                    },
                    (error) -> {
                        int promisesLeft = remPromises.decrementAndGet();
                        if (promisesLeft == 0) {
                            reject.accept(new Throwable("All Promises Got Rejected"));
                            return;
                        }
                    }
                );
            }
        });
    }

    // Fulfills when all promises fulfill. If any promise rejects then returns a rejected promise.
    public static Promise<List<?>> all(List<Promise<?>> promises) {
        if (promises.isEmpty())
            return new Promise<>((resolve, reject) -> resolve.accept(new ArrayList<>()));

        // Returned promises need to be in passed order, regardless of the completition order
        List<Object> retPromisesList = new ArrayList<>();
        promises.stream()
                .forEach(promise -> retPromisesList.add(null));
        
        // Count if all promises fulfilled
        AtomicInteger remainingPromises = new AtomicInteger(promises.size());

        return new Promise<>((resolve, reject) -> {
            for (int i = 0; i < promises.size(); i++){
                final int promIndex = i;
                promises.get(i).then(
                    (result) -> {
                        synchronized (retPromisesList) {
                            retPromisesList.set(promIndex, result);
                        }
                        int remaining = remainingPromises.decrementAndGet();
                        if (remaining == 0)
                            resolve.accept(retPromisesList);
                        
                        return result;
                    },
                    (error) -> {
                        reject.accept(error);
                    }
                );
            }
        });
    }

    // Promise fulfills when all promises settle
    public static Promise<List<ValueOrError<?>>> allSettled(List<Promise<?>> promises) {

        if (promises.isEmpty())
            return new Promise<>((resolve, reject) -> {
                resolve.accept(new ArrayList<>());
            });
        
        // Returned promises need to be in passed order, regardless of the completition order
        List<ValueOrError<?>> retPromisesList = new ArrayList<>();
        promises.stream()
                .forEach(promise -> retPromisesList.add(null));
        
        AtomicInteger remainingPromises = new AtomicInteger(promises.size());

        return new Promise<>((resolve, reject) -> {
            for (int i = 0; i < promises.size(); i++) {
                final int promIndex = i;
                promises.get(i).then(
                    (result) -> {
                        synchronized (retPromisesList) {
                            retPromisesList.set(promIndex, ValueOrError.Value.of(result));
                        }
                        int remaining = remainingPromises.decrementAndGet();

                        if (remaining == 0)
                            resolve.accept(retPromisesList);
                        
                        return result;
                    },
                    (error) -> {
                        synchronized (retPromisesList) {
                            retPromisesList.set(promIndex, ValueOrError.Error.of(error));
                        }
                        int remaining = remainingPromises.decrementAndGet();

                        if (remaining == 0)
                            resolve.accept(retPromisesList);
                    }
                );
            }
        });
    }

}
