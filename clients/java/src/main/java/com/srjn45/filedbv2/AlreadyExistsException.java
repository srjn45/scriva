package com.srjn45.filedbv2;

/**
 * A keyed insert used a key already held by a live record.
 *
 * <p>Raised from {@link FileDBClient#insert(String, java.util.Map, long, String)}
 * (and its keyed overloads) when the engine answers {@code ALREADY_EXISTS}.
 */
public class AlreadyExistsException extends FileDBException {

    public AlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
