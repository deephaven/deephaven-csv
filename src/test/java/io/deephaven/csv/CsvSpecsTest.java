package io.deephaven.csv;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CsvSpecsTest {

    @Test
    void builderFrom() {
        checkFromEquality(CsvSpecs.csv());
        checkFromEquality(CsvSpecs.headerless());
        checkFromEquality(CsvSpecs.tsv());
        checkFromEquality(CsvSpecs.builder().customTimeZoneParser((bs, zoneId, offsetSeconds) -> false).build());
    }

    private static void checkFromEquality(CsvSpecs specs) {
        assertThat(CsvSpecs.builder().from(specs).build()).isEqualTo(specs);
    }
}
