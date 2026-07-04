package com.srjn45.filedbv2;

/**
 * A keyed lookup, update, or delete referenced a key that no live record holds.
 *
 * <p>Raised from {@link FileDBClient#findByKey}, {@link FileDBClient#updateByKey},
 * and {@link FileDBClient#deleteByKey} when the engine answers {@code NOT_FOUND}.
 */
public class NotFoundException extends FileDBException {

    public NotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
