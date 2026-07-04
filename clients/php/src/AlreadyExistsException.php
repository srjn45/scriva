<?php

declare(strict_types=1);

namespace FileDBv2;

/**
 * Thrown when a keyed insert collides with a key already held by a live record —
 * the client mapping of the gRPC ALREADY_EXISTS status. Raised by insert() when
 * a `key` is supplied and that key is already taken. Use upsert() for
 * insert-or-replace semantics instead.
 */
class AlreadyExistsException extends FileDBException
{
}
