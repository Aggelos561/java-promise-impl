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

    private volatile Status promiseStatus;
    private volatile ValueOrError<V> returnValue;

    public Promise(PromiseExecutor<V> executor) {
        promiseStatus = Status.PENDING;
        makePromise(executor);
    }

    private void makePromise(PromiseExecutor<V> executor) {
        new Thread(() -> {
            executor.execute(this::resolvePromise, this::rejectPromise);
        }).start();
    }

    private synchronized void resolvePromise(V value) {
        if (promiseStatus != Status.PENDING)
            return;

        promiseStatus = Status.FULFILLED;
        returnValue = ValueOrError.Value.of(value);

        notifyAll();
    }

    private synchronized void rejectPromise(Throwable error) {
        if (promiseStatus != Status.PENDING)
            return;

        promiseStatus = Status.REJECTED;
        returnValue = ValueOrError.Error.of(error);

        notifyAll();
    }

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
            waitPendingPromise();
            settlePromise(onResolve, onReject, resolve, reject);
        });
    }

    public <T> Promise<T> then(Function<V, T> onResolve) {
        return then(onResolve, (error) -> {});
    }

    // catch is a reserved word in Java.
    public Promise<?> catchError(Consumer<Throwable> onReject) {
        return then((value) -> {return value;}, onReject);
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

    public static Promise<?> any(List<Promise<?>> promises) {
        if (promises.isEmpty())
            return new Promise<>((resolve, reject) -> {
                reject.accept(new Throwable("Empty List Of Promises"));
            });
    
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

    public static Promise<List<?>> all(List<Promise<?>> promises) {
        if (promises.isEmpty())
            return new Promise<>((resolve, reject) -> resolve.accept(new ArrayList<>()));

        List<Object> retPromisesList = new ArrayList<>();
        promises.stream()
                .forEach(promise -> {
                    retPromisesList.add(null);
                });
        
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

    public static Promise<List<ValueOrError<?>>> allSettled(List<Promise<?>> promises) {

        if (promises.isEmpty())
            return new Promise<>((resolve, reject) -> {
                resolve.accept(new ArrayList<>());
            });

        List<ValueOrError<?>> retPromisesList = new ArrayList<>();
        promises.stream()
                .forEach(promise -> {
                    retPromisesList.add(null);
                });
        
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
