package com.srjn45.filedbv2;

/**
 * Base class for FileDB client errors surfaced from engine gRPC status codes.
 *
 * <p>The keyed-CRUD surface (N1) maps the common failure codes onto typed
 * subclasses — {@link NotFoundException} for {@code NOT_FOUND} and
 * {@link AlreadyExistsException} for {@code ALREADY_EXISTS}. Any other status
 * code propagates unchanged as the original {@link io.grpc.StatusRuntimeException}.
 */
public class FileDBException extends RuntimeException {

    public FileDBException(String message) {
        super(message);
    }

    public FileDBException(String message, Throwable cause) {
        super(message, cause);
    }
}
