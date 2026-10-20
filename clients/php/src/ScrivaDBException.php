<?php

declare(strict_types=1);

namespace ScrivaDB;

/**
 * Base class for all errors surfaced by the ScrivaDB client.
 *
 * Extends \RuntimeException so existing `catch (\RuntimeException $e)` blocks
 * keep working; catch the more specific NotFoundException / AlreadyExistsException
 * subclasses to handle keyed-CRUD outcomes idiomatically.
 */
class ScrivaDBException extends \RuntimeException
{
}

/** XTx failure retaining gRPC status/trailers (including serialized ErrorInfo). */
class XTxException extends ScrivaDBException
{
    public function __construct(public readonly object $status)
    {
        parent::__construct((string) ($status->details ?? 'XTx RPC failed'), (int) ($status->code ?? 0));
    }
}
