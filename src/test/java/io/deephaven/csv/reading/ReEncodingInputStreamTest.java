package io.deephaven.csv.reading;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class ReEncodingInputStreamTest {
    // Note: take care when editing this list, as certain strings may not render properly in your editor.
    private static final List<String> TEST_STRINGS = Collections.unmodifiableList(Arrays.asList(
            "",
            "a",
            "ab",
            "abc",
            "Hello, world",
            "Hello 👋 World 🌍 Test 🎉",
            "Hello, 世界",
            "Héllo, wörld! Tëst with spëcial çharacters: àéîöü",
            "😀",
            "café résumé",
            "你好世界",
            "こんにちは",
            "안녕하세요",
            "مرحبا بالعمل",
            "שלום עולם",
            "é",
            "Hello World",
            "\\uFEFFHello",
            "Test\\uFFFD",
            "Line1\\u0000Line2",
            "\\uD800\\uDC00",
            "Ñoño Ü über",
            "–—",
            "\uD835\uDD73\uD835\uDD8A\uD835\uDD91\uD835\uDD91\uD835\uDD94"));

    private static Collection<Charset> charsetsUnderTest() {
        // A wider set of charsets can be tested if desired; note though, it will take a while (at least 200,000 tests
        // on my system before I had to stop it).
        // return Charset.availableCharsets().values().stream().filter(Charset::canEncode).collect(Collectors.toList());
        return Arrays.asList(
                StandardCharsets.US_ASCII,
                StandardCharsets.ISO_8859_1,
                StandardCharsets.UTF_8,
                StandardCharsets.UTF_16,
                StandardCharsets.UTF_16BE,
                StandardCharsets.UTF_16LE);
    }

    /**
     * This is all of {@link #TEST_STRINGS}, plus a final string with a lot of {@link #TEST_STRINGS} added together.
     */
    private static List<String> testStrings() {
        final List<String> out = new ArrayList<>(TEST_STRINGS);
        final StringBuilder sb = new StringBuilder();
        int i = 0;
        while (sb.length() < 4000) {
            sb.append(TEST_STRINGS.get(i % TEST_STRINGS.size()));
            ++i;
        }
        out.add(sb.toString());
        return out;
    }

    private static Stream<Arguments> testCases() {
        List<Arguments> out = new ArrayList<>();
        for (final String testString : testStrings()) {
            final List<Charset> applicableCharsets = charsetsUnderTest().stream()
                    .filter(x -> x.newEncoder().canEncode(testString))
                    .collect(Collectors.toList());
            for (final Charset srcCharset : applicableCharsets) {
                for (final Charset dstCharset : applicableCharsets) {
                    out.add(Arguments.of(testString, srcCharset, dstCharset));
                }
            }
        }
        return out.stream();
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void testCombinations(final String input, final Charset srcCharset, final Charset dstCharset)
            throws CharacterCodingException {
        test(input, srcCharset, dstCharset);
    }

    @Test
    void testEmptyStream() throws IOException {
        try (final ReEncodingInputStream in = new ReEncodingInputStream(new ByteArrayInputStream(new byte[0]),
                StandardCharsets.UTF_8, StandardCharsets.UTF_8, 128)) {
            assertThat(in.read()).isEqualTo(-1);
        }
    }

    @Test
    void testSingleRead() throws IOException {
        try (final ReEncodingInputStream in =
                new ReEncodingInputStream(new ByteArrayInputStream("Hello".getBytes(StandardCharsets.US_ASCII)),
                        StandardCharsets.UTF_8, StandardCharsets.UTF_8, 128)) {
            assertThat(in.read()).isEqualTo('H');
            assertThat(in.read()).isEqualTo('e');
            assertThat(in.read()).isEqualTo('l');
            assertThat(in.read()).isEqualTo('l');
            assertThat(in.read()).isEqualTo('o');
            assertThat(in.read()).isEqualTo(-1);
        }
    }

    @Test
    void testBulkRead() throws IOException {
        try (final ReEncodingInputStream in =
                new ReEncodingInputStream(new ByteArrayInputStream("Hello".getBytes(StandardCharsets.US_ASCII)),
                        StandardCharsets.UTF_8, StandardCharsets.UTF_8, 128)) {
            final byte[] buffer = new byte[6];
            assertThat(in.read(buffer)).isEqualTo(5);
            assertThat(buffer).containsExactly('H', 'e', 'l', 'l', 'o', 0);
        }
    }

    /**
     * This test is a bit circular as we are creating our input data and expected output data based on one-shot batch
     * {@link java.nio.charset.CharsetEncoder#encode(CharBuffer)}. This is "okay", as we are mostly interested in
     * testing the incremental aspects of re-encoding bytes, as opposed to the literal declaration of what those bytes
     * should be.
     */
    private static void test(final String input, final Charset srcCharset, final Charset dstCharset)
            throws CharacterCodingException {
        final ByteBuffer srcBuffer = srcCharset.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .encode(CharBuffer.wrap(input));
        final ByteBuffer dstBuffer = dstCharset.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .encode(CharBuffer.wrap(input));
        final int numChars = input.length();
        // Test a variety of buffer sizes.
        for (int i = ReEncodingInputStream.MIN_BUFFER_SIZE; i < Math.min(numChars, 128); ++i) {
            doTest(srcBuffer, srcCharset, dstBuffer, dstCharset, i);
        }
        // If the input string is big, only start testing powers of 2 (and neighbors)
        for (int i = 128; i < numChars; i *= 2) {
            doTest(srcBuffer, srcCharset, dstBuffer, dstCharset, i - 1);
            doTest(srcBuffer, srcCharset, dstBuffer, dstCharset, i);
            doTest(srcBuffer, srcCharset, dstBuffer, dstCharset, i + 1);
        }
        // Also test exactly numChars (and neighbors)
        doTest(srcBuffer, srcCharset, dstBuffer, dstCharset, numChars - 1);
        doTest(srcBuffer, srcCharset, dstBuffer, dstCharset, numChars);
        doTest(srcBuffer, srcCharset, dstBuffer, dstCharset, numChars + 1);

        // Test a bigger size as well
        doTest(srcBuffer, srcCharset, dstBuffer, dstCharset, 2 * numChars + 43);
    }

    private static void doTest(
            final ByteBuffer srcBuffer,
            final Charset srcCharset,
            final ByteBuffer expectedOut,
            final Charset dstCharset,
            final int bufferSize) {
        if (bufferSize < ReEncodingInputStream.MIN_BUFFER_SIZE) {
            // Cover some edge cases without needing all of the callers to be precise
            return;
        }
        assertThat(new ReEncodingInputStream(toInputStream(srcBuffer), srcCharset, dstCharset, bufferSize))
                .hasSameContentAs(toInputStream(expectedOut));
    }

    private static InputStream toInputStream(final ByteBuffer buffer) {
        return new ByteArrayInputStream(buffer.array(), buffer.position(), buffer.limit());
    }
}
