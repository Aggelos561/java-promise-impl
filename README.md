# java-promise-impl

A Java implementation of JavaScript-style Promise behavior using low-level concurrency primitives. This project is built as a small Gradle library and focuses on asynchronous value handling, rejection flow, and promise composition without using the Java concurrency utilities like `CompletableFuture` or `ExecutorService`.

## Project overview

The library provides a custom `Promise<V>` type with behavior inspired by JavaScript promises:

- `then(...)` for success handling
- `catchError(...)` for rejection handling
- `andFinally(...)` for settling logic
- static helpers such as `resolve(...)`, `reject(...)`, `all(...)`, `allSettled(...)`, `any(...)`, and `race(...)`
- asynchronous execution through custom executor functions and thread-based promise resolution

This project lives under the `java-promise/` directory and is structured as a Gradle multi-purpose Java library with Groovy/Spock tests.

## Repository structure

```text
javaPromise/
├── README.md
├── assignment/
└── java-promise/
    ├── gradlew
    ├── gradlew.bat
    ├── settings.gradle
    ├── README.md
    ├── gradle/
    └── lib/
        ├── build.gradle
        └── src/
            ├── main/java/gr/uoa/di/promise/
            └── test/groovy/gr/uoa/di/promise/
```

## Key classes

- `Promise<V>`: the main promise abstraction
- `PromiseExecutor<V>`: callback-based executor used to resolve or reject a promise
- `ValueOrError<T>`: wrapper for either a fulfilled value or an error
- `DelayedValue<T>`: helper that resolves after a delay
- `DelayedError`: helper that rejects after a delay

## Getting started

From the project root:

```bash
cd java-promise
./gradlew test
```

To build the library and run the test suite:

```bash
cd java-promise
./gradlew build
```

## Example usage

```java
import gr.uoa.di.promise.Promise;

Promise<String> p = new Promise<>((resolve, reject) -> {
    resolve.accept("hello");
});

p.then(value -> {
    System.out.println(value.toUpperCase());
    return value.length();
}).then(length -> {
    System.out.println("Length: " + length);
}).catchError(error -> {
    System.err.println("Error: " + error.getMessage());
});
```

## Behavior and constraints

This implementation follows the assignment requirements:

- it uses low-level Java synchronization primitives such as `wait`, `notify`, `synchronized`, and `volatile`
- it avoids the `java.util.concurrent` package for promise orchestration
- it intentionally creates independent threads for promise execution where needed

## Testing

The test suite is located in:

```text
java-promise/lib/src/test/groovy/gr/uoa/di/promise/PromiseSpec.groovy
```

The project uses Spock for specification-style tests, and reports are generated under:

```text
java-promise/lib/build/reports/tests/test/index.html
```

## License

This project is intended for educational use and follows the requirements described in the assignment and project documentation.
