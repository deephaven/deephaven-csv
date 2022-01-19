package io.deephaven.csvbench;

public final class ColumnTextAndData<TARRAY> {
    private final String[] text;
    private final TARRAY data;

    public ColumnTextAndData(String[] text, TARRAY data) {
        this.text = text;
        this.data = data;
    }

    public String[] text() {
        return text;
    }

    public TARRAY data() {
        return data;
    }
}
