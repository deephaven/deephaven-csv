package io.deephaven.csv.testutil;

import io.deephaven.csv.CsvReaderTest;

import java.lang.reflect.Array;

public final class Column<TARRAY> {
    private final String name;
    private final TARRAY values;
    private final int size;
    private final Class<?> reinterpretedType;

    public static Column<byte[]> ofValues(final String name, final byte... values) {
        return ofArray(name, values, values.length);
    }

    public static Column<short[]> ofValues(final String name, final short... values) {
        return ofArray(name, values, values.length);
    }

    public static Column<int[]> ofValues(final String name, final int... values) {
        return ofArray(name, values, values.length);
    }

    public static Column<long[]> ofValues(final String name, final long... values) {
        return ofArray(name, values, values.length);
    }

    public static Column<float[]> ofValues(final String name, final float... values) {
        return ofArray(name, values, values.length);
    }

    public static Column<double[]> ofValues(final String name, final double... values) {
        return ofArray(name, values, values.length);
    }

    public static Column<char[]> ofValues(final String name, final char... values) {
        return ofArray(name, values, values.length);
    }

    public static <T> Column<T[]> ofRefs(final String name, final T... values) {
        return ofArray(name, values, values.length);
    }

    public static <TARRAY> Column<TARRAY> ofArray(final String name, final TARRAY values, int size) {
        return new Column<>(name, values, size);
    }

    private Column(final String name, final TARRAY values, int size) {
        this(name, values, size, values.getClass().getComponentType());
    }

    private Column(final String name, final TARRAY values, int size, Class<?> reinterpretedType) {
        this.name = name;
        this.values = values;
        this.size = size;
        this.reinterpretedType = reinterpretedType;
    }

    public Column<TARRAY> reinterpret(Class<?> reinterpretedType) {
        return new Column<>(name, values, size, reinterpretedType);
    }

    public int size() {
        return size;
    }

    public String name() {
        return name;
    }

    public Class<?> elementType() {
        return values.getClass().getComponentType();
    }

    public Class<?> reinterpretedType() {
        return reinterpretedType;
    }

    public Object getItem(int index) {
        return Array.get(values, index);
    }
}
