package gr.uoa.di.promise;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.LinkedList;
import java.util.Queue;


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

    class promiseState {

        public Status promiseStatus;
        public ValueOrError<V> returnValue;

        private Queue<Function<V, ?>> fulfilledQueue;
        private Queue<Consumer<Throwable>> rejectedQueue;

        public promiseState() {
            promiseStatus = Status.PENDING;
            fulfilledQueue = new LinkedList<>();
            rejectedQueue = new LinkedList<>();
            returnValue = null;
        }

        public void addToFulfQueue(Function<V, ?> onResolve) {
            fulfilledQueue.add(onResolve);
        }

        public Function<V, ?> getNextResolve() {
            return fulfilledQueue.poll();
        }

        public boolean hasNextResolve() {
            return !fulfilledQueue.isEmpty();
        }

        public void addToRejQueue(Consumer<Throwable> onReject) {
            rejectedQueue.add(onReject);
        }

        public Consumer<Throwable> getNextReject() {
            return rejectedQueue.poll();
        }

        public boolean hasNextReject() {
            return !rejectedQueue.isEmpty();
        }

    }

    private promiseState promState;

    public Promise(PromiseExecutor<V> executor) {
        this.promState = new promiseState();
        initNewPromise(executor);
    }

    private void initNewPromise(PromiseExecutor<V> executor) {
        new Thread(() -> {
                executor.execute(this::onResolve, this::onReject);
        }).start();
    }

    private void onResolve(V value) {
        synchronized(promState) {
            if (promState.promiseStatus != Status.PENDING) {
                return;
            }
            promState.promiseStatus = Status.FULFILLED;
            promState.returnValue = ValueOrError.Value.of(value);
            
            while (promState.hasNextResolve()) {
                Function<V, ?> callback = promState.getNextResolve();
                try {
                    callback.apply(value);
                } 
                catch (Exception e) {
                    System.err.println("Error in resolve callback: " + e.getMessage());
                }
            }
        }
    }
    
    private void onReject(Throwable error) {
        synchronized(promState) {
        if (promState.promiseStatus != Status.PENDING) {
                return;
            }
            promState.promiseStatus = Status.REJECTED;
            promState.returnValue = ValueOrError.Error.of(error);

            while (promState.hasNextReject()) {
                Consumer<Throwable> callback = promState.getNextReject();
                try {
                    callback.accept(error);
                } 
                catch (Exception e) {
                    System.err.println("Error in reject callback: " + e.getMessage());
                }
            }
        }
    }

    public <T> Promise<T> then(Function<V, T> onResolve, Consumer<Throwable> onReject) {
        return new Promise<>((resolve, reject) -> {
            synchronized(promState) {
                if (promState.promiseStatus == Status.PENDING) {
                    promState.addToFulfQueue(value -> {
                        try {
                            T result = onResolve.apply(value);
                            resolve.accept(result);
                            return result;
                        } catch (Exception e) {
                            reject.accept(e);
                            throw e;
                        }
                    });

                    promState.addToRejQueue(error -> {
                        try {
                            onReject.accept(error); 
                            reject.accept(error);
                        } 
                        catch (Exception e) {
                            reject.accept(e);
                            throw e;
                        }
                    });
                } 
                else if (promState.promiseStatus == Status.FULFILLED) {
                    try {
                        T result = onResolve.apply(promState.returnValue.value());
                        resolve.accept(result);
                    } 
                    catch (Exception e) {
                        reject.accept(e);
                    }
                }
                else if (promState.promiseStatus == Status.REJECTED) {
                    try {
                        onReject.accept(promState.returnValue.error());
                        reject.accept(promState.returnValue.error());
                    } 
                    catch (Exception e) {
                        reject.accept(e);
                    }
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
            synchronized(promState) {
                if (promState.promiseStatus == Status.PENDING) {

                    promState.addToFulfQueue(value -> {
                        onSettle.accept(ValueOrError.Value.of(value));
                        resolve.accept(value);
                        return null;
                    });
    
                    promState.addToRejQueue(error -> {
                        onSettle.accept(ValueOrError.Error.of(error));
                        reject.accept(error);
                    });
                } 
                else {
                    if (promState.promiseStatus == Status.FULFILLED) {
                        V value = promState.returnValue.value();
                        onSettle.accept(ValueOrError.Value.of(value));
                        resolve.accept(value);
                    } 
                    else if (promState.promiseStatus == Status.REJECTED) {
                        Throwable error = promState.returnValue.error();
                        onSettle.accept(ValueOrError.Error.of(error));
                        reject.accept(error);
                    }
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
        throw new UnsupportedOperationException("IMPLEMENT ME");
    }

    public static Promise<?> any(List<Promise<?>> promises) {
        throw new UnsupportedOperationException("IMPLEMENT ME");
    }

    public static Promise<List<?>> all(List<Promise<?>> promises) {
        throw new UnsupportedOperationException("IMPLEMENT ME");
    }

    public static Promise<List<ValueOrError<?>>> allSettled(List<Promise<?>> promises) {
        throw new UnsupportedOperationException("IMPLEMENT ME");
    }
}
