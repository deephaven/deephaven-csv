# Deephaven CSV

## Testing

```shell
./gradlew check
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
java -jar build/libs/deephaven-csv-0.0.1-SNAPSHOT-jmh.jar -prof gc -rf JSON
```

```shell
java -jar build/libs/deephaven-csv-0.0.1-SNAPSHOT-jmh.jar -prof gc -rf JSON <regex>
```

The JMH jar is the preferred way to run official benchmarks, and provides a common bytecode for sharing the benchmarks
among multiple environments.

[JMH Visualizer](https://github.com/jzillmann/jmh-visualizer) is a convenient tool for visualizing JMH results.
