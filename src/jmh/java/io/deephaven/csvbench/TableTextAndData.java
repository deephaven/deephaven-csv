package io.deephaven.csvbench;

import io.deephaven.csv.util.Renderer;

import java.util.List;
import java.util.stream.IntStream;

public class TableTextAndData {
    public static TableTextAndData of(List<ColumnTextAndData<?>> inputColumns) {
        final int numCols = inputColumns.size();
        final int numRows = inputColumns.get(0).text().length;

        final StringBuilder sb = new StringBuilder();
        // Write a line of headers like Column1,Column2,...,ColumnN
        Renderer.renderList(sb, IntStream.range(0, numCols)::iterator, ",", i -> "Column" + (i + 1));
        sb.append('\n');
        for (int jj = 0; jj < numRows; ++jj) {
            final int finalJJ = jj;
            // Write a line of data like 12,-54321,...,17
            Renderer.renderList(sb, IntStream.range(0, numCols)::iterator, ",",
                    i -> inputColumns.get(i).text()[finalJJ]);
            sb.append('\n');
        }

        final String text = sb.toString();
        final Object[] columns = new Object[numCols];
        for (int ii = 0; ii < numCols; ++ii) {
            columns[ii] = inputColumns.get(ii).data();
        }
        return new TableTextAndData(text, columns);
    }

    private final String text;
    private final Object[] columns;

    public TableTextAndData(String text, Object[] columns) {
        this.text = text;
        this.columns = columns;
    }

    public String text() {
        return text;
    }

    public Object[] columns() {
        return columns;
    }
}
