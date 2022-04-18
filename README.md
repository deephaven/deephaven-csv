# The Deephaven High-Performance CSV Parser

## Introduction

The Deephaven CSV Library is a high-performance, column-oriented, type inferencing CSV parser. It differs from other CSV
libraries in that it organizes data into columns rather than rows, which allows for more efficient storage and
retrieval. It also can dynamically infer the types of those columns based on the input, so the caller is not required to
specify the column types beforehand. Finally it provides a way for the caller to specify the underlying data structures
used for columnar storage, This allows the library to store its data directly in the caller's preferred data structure,
without the inefficiency of going through intermediate temporary objects.

The Deephaven CSV Library is agnostic about what data sink you use, and it works equally well with Java arrays, your own
custom column type, or perhaps even streaming to a file. But along with this flexibility comes extra programming effort
on the part of the implementor: instead of telling the library what column data structures to use, the caller provides a
"factory" capable of constructing any requested column type, and the library then dynamically decides which ones it
needs as it parses the input data. While it is tempting to just use ArrayList or some other catch-all collection, this
is not as efficient as type-specific collectors, and makes a large impact on performance as data sizes increase.
Instead, it is common practice in high-performance libraries to provide multiple, very similar but distinct
implementations, one for each primitive type. For example, your high-performance application might have
YourCharColumnType, YourIntColumnType, YourDoubleColumnType, and the like. Unfortunately this translates into a certain
amount of tedium for the implementor, who needs to provide implementations for each type and code to move data from the
CSV library to them.

With this guide we hope to make it clear what the caller needs to implement, and also to provide a reference
implementation for people to use as a starting point.

## Using the Reference Implementation

To help you get started, the library provides a "sink factory" that uses Java arrays for the underlying column
representation. This version is best suited for simple examples and for learning how to use the library. Developers of
production applications will likely want to define their own column representations and to create the sink factory that
supplies them. The documentation in [ADVANCED.md](ADVANCED.md) describes how to do this. For now, we show how to process
data using the builtin sink factory for arrays:

```java
final InputStream inputStream = ...;
final CsvSpecs specs = CsvSpecs.csv();
final CsvReader.Result result = CsvReader.read(specs, inputStream, SinkFactory.arrays());
final long numRows = result.numRows();
for (CsvReader.ResultColumn col : result) {
    switch (col.dataType()) {
        case BOOLEAN_AS_BYTE: {
            byte[] data = (byte[]) col.data();
            // Process this boolean-as-byte column.
            // Be sure to use numRows rather than data.length, because
            // the underlying array might have excess capacity.
            process(data, numRows);
            break;
        }
        case SHORT: {
            short[] data = (short[]) col.data();
            // Process this short column.
            process(data, numRows);
            break;
        }
        // etc...
    }
}
```

If your application uses reserved null sentinel values, there is an overload of SinkFactory.arrays() that allows you to
specify those values.


## Using

This project produces three JARs:

1. `deephaven-csv`: the primary dependency
2. (optional, but recommended with Java 11+) `deephaven-csv-fast-double-parser`: a fast double parser, compatible with Java 11+
3. (optional, but recommended with Java 8) `deephaven-csv-fast-double-parser-8`: a fast double parser, compatible with Java 8+

### Gradle

To depend on Deephaven CSV from Gradle, add the following dependency(s) to your build.gradle file:

```groovy
implementation 'io.deephaven:deephaven-csv:0.3.0'

// Optional dependency for faster double parsing (Java 11+ compatible)
// runtimeOnly 'io.deephaven:deephaven-csv-fast-double-parser:0.3.0'

// Optional dependency for faster double parsing (Java 8+ compatible)
// runtimeOnly 'io.deephaven:deephaven-csv-fast-double-parser-8:0.3.0'
```

### Maven

To depend on Deephaven CSV from Maven, add the following dependency(s) to your pom.xml file:

```xml
<dependency>
    <groupId>io.deephaven</groupId>
    <artifactId>deephaven-csv</artifactId>
    <version>0.3.0</version>
</dependency>

<!-- Optional dependency for faster double parsing (Java 11+ compatible) -->
<!--<dependency>-->
<!--    <groupId>io.deephaven</groupId>-->
<!--    <artifactId>deephaven-csv-fast-double-parser</artifactId>-->
<!--    <version>0.3.0</version>-->
<!--    <scope>runtime</scope>-->
<!--</dependency>-->

<!-- Optional dependency for faster double parsing (Java 8+ compatible) -->
<!--<dependency>-->
<!--    <groupId>io.deephaven</groupId>-->
<!--    <artifactId>deephaven-csv-fast-double-parser-8</artifactId>-->
<!--    <version>0.3.0</version>-->
<!--    <scope>runtime</scope>-->
<!--</dependency>-->
```

## Testing

To run the main tests:

```shell
./gradlew test
```

## Building

```shell
./gradlew build
```

## Code style

[Spotless](https://github.com/diffplug/spotless/tree/main/plugin-gradle) is used for code formatting.

To auto-format your code, you can run:
```shell
./gradlew spotlessApply
```

## Local development

If you are doing local development and want to consume `deephaven-csv` changes in other components, you can publish to maven local:

```shell
./gradlew publishToMavenLocal -x signMavenJavaPublication
```

## Benchmarks

To run the all of the [JMH](https://github.com/openjdk/jmh) benchmarks locally, you can run:

```shell
./gradlew jmh
```

This will produce a textual output to the screen, as well as machine-readable results at `build/results/jmh/results.json`.

To run specific JMH benchmarks, you can run:

```shell
./gradlew jmh -Pjmh.includes="<regex>"
```

If you prefer, you can run the benchmarks directly via the JMH jar:

```shell
./gradlew jmhJar
```

```shell
java -jar build/libs/deephaven-csv-0.4.0-SNAPSHOT-jmh.jar -prof gc -rf JSON
```

```shell
java -jar build/libs/deephaven-csv-0.4.0-SNAPSHOT-jmh.jar -prof gc -rf JSON <regex>
```

The JMH jar is the preferred way to run official benchmarks, and provides a common bytecode for sharing the benchmarks
among multiple environments.

[JMH Visualizer](https://github.com/jzillmann/jmh-visualizer) is a convenient tool for visualizing JMH results.

## Benchmark Tests

The benchmarks have tests to ensure that the benchmark implementations are producing the correct results.
To run the benchmark tests, run:

```shell
./gradlew jmhTest
```
