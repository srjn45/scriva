<?php

declare(strict_types=1);

namespace FileDBv2;

/**
 * Base class for all errors surfaced by the FileDB client.
 *
 * Extends \RuntimeException so existing `catch (\RuntimeException $e)` blocks
 * keep working; catch the more specific NotFoundException / AlreadyExistsException
 * subclasses to handle keyed-CRUD outcomes idiomatically.
 */
class FileDBException extends \RuntimeException
{
}
