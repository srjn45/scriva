package com.srjn45.scriva;

/**
 * A keyed lookup, update, or delete referenced a key that no live record holds.
 *
 * <p>Raised from {@link ScrivaDBClient#findByKey}, {@link ScrivaDBClient#updateByKey},
 * and {@link ScrivaDBClient#deleteByKey} when the engine answers {@code NOT_FOUND}.
 */
public class NotFoundException extends ScrivaDBException {

    public NotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
