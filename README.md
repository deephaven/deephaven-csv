# The Deephaven High-Performance CSV Parser

## Using

This project produces three JARs:

1. `deephaven-csv`: the primary dependency
2. (optional, but recommended with Java 11+) `deephaven-csv-fast-double-parser`: a fast double parser, compatible with Java 11+
3. (optional, but recommended with Java 8) `deephaven-csv-fast-double-parser-8`: a fast double parser, compatible with Java 8+

### Gradle

To depend on Deephaven CSV from Gradle, add the following dependency(s) to your build.gradle file:

```groovy
implementation 'io.deephaven:deephaven-csv:0.2.0'

// Optional dependency for faster double parsing (Java 11+ compatible)
// runtimeOnly 'io.deephaven:deephaven-csv-fast-double-parser:0.2.0'

// Optional dependency for faster double parsing (Java 8+ compatible)
// runtimeOnly 'io.deephaven:deephaven-csv-fast-double-parser-8:0.2.0'
```

### Maven

To depend on Deephaven CSV from Maven, add the following dependency(s) to your pom.xml file:

```xml
<dependency>
    <groupId>io.deephaven</groupId>
    <artifactId>deephaven-csv</artifactId>
    <version>0.2.0</version>
</dependency>

<!-- Optional dependency for faster double parsing (Java 11+ compatible) -->
<!--<dependency>-->
<!--    <groupId>io.deephaven</groupId>-->
<!--    <artifactId>deephaven-csv-fast-double-parser</artifactId>-->
<!--    <version>0.2.0</version>-->
<!--    <scope>runtime</scope>-->
<!--</dependency>-->

<!-- Optional dependency for faster double parsing (Java 8+ compatible) -->
<!--<dependency>-->
<!--    <groupId>io.deephaven</groupId>-->
<!--    <artifactId>deephaven-csv-fast-double-parser-8</artifactId>-->
<!--    <version>0.2.0</version>-->
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
java -jar build/libs/deephaven-csv-0.2.0-jmh.jar -prof gc -rf JSON
```

```shell
java -jar build/libs/deephaven-csv-0.2.0-jmh.jar -prof gc -rf JSON <regex>
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
