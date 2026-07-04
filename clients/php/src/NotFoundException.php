<?php

declare(strict_types=1);

namespace FileDBv2;

/**
 * Thrown when a record, key, or collection does not exist — the client mapping
 * of the gRPC NOT_FOUND status. Raised e.g. by findByKey(), updateByKey() and
 * deleteByKey() when no live record carries the requested key.
 */
class NotFoundException extends FileDBException
{
}
