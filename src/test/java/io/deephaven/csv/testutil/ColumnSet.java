package io.deephaven.csv.testutil;

import io.deephaven.csv.CsvReaderTest;
import io.deephaven.csv.util.Renderer;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

public final class ColumnSet {
    public static final ColumnSet NONE = new ColumnSet(new Column[0], 0);

    private final Column<?>[] columns;
    private final int columnSize;

    public static ColumnSet of(Column<?>... columns) {
        if (columns.length == 0) {
            throw new RuntimeException("Empty column set is not permitted");
        }
        final int c0Size = columns[0].size();
        for (int ii = 1; ii < columns.length; ++ii) { // Deliberately starting at 1.
            final int ciiSize = columns[ii].size();
            if (ciiSize != c0Size) {
                throw new RuntimeException(
                        String.format(
                                "Column %d (size %d) has a different size than column 0 (size %d)",
                                ii, ciiSize, c0Size));
            }
        }
        return new ColumnSet(columns, c0Size);
    }

    private ColumnSet(Column<?>[] columns, int columnSize) {
        this.columns = columns;
        this.columnSize = columnSize;
    }

    public Column<?>[] getColumns() {
        return columns;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        final List<Column<?>> colList = Arrays.asList(columns);

        final BiFunction<Class<?>, Class<?>, String> renderType =
                (etype, rtype) -> {
                    if (etype == rtype) {
                        return etype.getCanonicalName();
                    }
                    return etype.getCanonicalName() + "->" + rtype.getCanonicalName();
                };

        Renderer.renderList(
                sb,
                colList,
                ",",
                col -> String.format(
                        "%s(%s)",
                        col.name(), renderType.apply(col.elementType(), col.reinterpretedType())));
        for (int jj = 0; jj < columnSize; ++jj) {
            final int jjFinal = jj;
            sb.append('\n');
            Renderer.renderList(sb, colList, ",", col -> safeToString(col.getItem(jjFinal)));
        }
        return sb.toString();
    }

    private static String safeToString(Object o) {
        return o == null ? "(null)" : o.toString();
    }
}
