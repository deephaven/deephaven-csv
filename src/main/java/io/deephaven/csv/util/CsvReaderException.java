package io.deephaven.csv.util;

/** The standard Exception class for various CSV errors. */
public class CsvReaderException extends Exception {
    /**
     * Constructor.
     * 
     * @param message The exception message.
     */
    public CsvReaderException(String message) {
        super(message);
    }

    /**
     * Constructor.
     * 
     * @param message The exception message.
     * @param cause The inner exception.
     */
    public CsvReaderException(String message, Throwable cause) {
        super(message, cause);
    }
}
