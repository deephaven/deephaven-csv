package io.deephaven.csv;

import io.deephaven.csv.tokenization.FastCustomDoubleParser8;
import io.deephaven.csv.tokenization.Tokenizer.CustomDoubleParser;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class FastCustomDoubleParser8Test {
    @Test
    void loadFastCustomDoubleParser8() {
        final Optional<CustomDoubleParser> parser = CustomDoubleParser.load();
        assertThat(parser).isPresent();
        assertThat(parser.get().getClass()).isEqualTo(FastCustomDoubleParser8.class);
    }

    @Test
    void csvSpecsHasFastCustomDoubleParser8() {
        final CsvSpecs specs = CsvSpecs.builder().build();
        assertThat(specs.customDoubleParser().getClass()).isEqualTo(FastCustomDoubleParser8.class);
    }
}
