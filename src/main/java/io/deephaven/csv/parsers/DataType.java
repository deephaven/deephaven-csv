package io.deephaven.csv.parsers;

/**
 * The data type of the column.
 */
public enum DataType {
    /**
     * The column represents a logical boolean type, using a physical byte value.
     */
    BOOLEAN_AS_BYTE,
    /**
     * The column represents a byte value.
     */
    BYTE,
    /**
     * The column represents a short value.
     */
    SHORT,
    /**
     * The column represents an int value.
     */
    INT,
    /**
     * The column represents a long value.
     */
    LONG,
    /**
     * The column represents a float value.
     */
    FLOAT,
    /**
     * The column represents a double value.
     */
    DOUBLE,
    /**
     * The column represents a logical DateTime type, using a physical long value.
     */
    DATETIME_AS_LONG,
    /**
     * The column represents a char value.
     */
    CHAR,
    /**
     * The column represents a String value.
     */
    STRING,
    /**
     * The column represents a logical Timestamp type, using a physical long value.
     */
    TIMESTAMP_AS_LONG,
    /**
     * The column represents a custom value parsed by the user's custom parser.
     */
    CUSTOM
}
