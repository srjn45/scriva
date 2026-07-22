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
