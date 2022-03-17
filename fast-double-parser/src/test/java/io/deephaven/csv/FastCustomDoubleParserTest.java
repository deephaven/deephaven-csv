package io.deephaven.csv;

import io.deephaven.csv.tokenization.FastCustomDoubleParser;
import io.deephaven.csv.tokenization.Tokenizer.CustomDoubleParser;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class FastCustomDoubleParserTest {
    @Test
    void loadFastCustomDoubleParser() {
        final Optional<CustomDoubleParser> parser = CustomDoubleParser.load();
        assertThat(parser).isPresent();
        assertThat(parser.get().getClass()).isEqualTo(FastCustomDoubleParser.class);
    }

    @Test
    void csvSpecsHasFastCustomDoubleParser() {
        final CsvSpecs specs = CsvSpecs.builder().build();
        assertThat(specs.customDoubleParser().getClass()).isEqualTo(FastCustomDoubleParser.class);
    }
}
