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
    public readonly ?string $reason;
    /** @var array<string, string> */
    public readonly array $errorMetadata;
    public function __construct(public readonly object $status)
    {
        parent::__construct((string) ($status->details ?? 'XTx RPC failed'), (int) ($status->code ?? 0));
        [$this->reason, $this->errorMetadata] = self::decode($status->metadata['grpc-status-details-bin'][0] ?? null);
    }

    /** Decode google.rpc.Status/Any/ErrorInfo without requiring rpc protos at runtime. */
    public static function decode(?string $wire): array
    {
        if ($wire === null) return [null, []];
        try {
            foreach (self::fields($wire) as [$number, $value]) {
                if ($number !== 3) continue;
                $any = self::fields($value); $url = ''; $payload = null;
                foreach ($any as [$n, $v]) { if ($n === 1) $url = $v; elseif ($n === 2) $payload = $v; }
                if (!str_ends_with($url, 'google.rpc.ErrorInfo') || $payload === null) continue;
                $reason = ''; $domain = ''; $metadata = [];
                foreach (self::fields($payload) as [$n, $v]) {
                    if ($n === 1) $reason = $v; elseif ($n === 2) $domain = $v;
                    elseif ($n === 3) { $key = ''; $val = ''; foreach (self::fields($v) as [$pn, $pv]) { if ($pn === 1) $key = $pv; elseif ($pn === 2) $val = $pv; } $metadata[$key] = $val; }
                }
                if ($domain === 'scriva.xtx') return [$reason, $metadata];
            }
        } catch (\Throwable) { }
        return [null, []];
    }
    private static function fields(string $wire): array
    {
        $out = []; $i = 0; $len = strlen($wire);
        $varint = static function () use (&$i, $wire, $len): int { $n=0; $shift=0; do { if ($i >= $len) throw new \UnexpectedValueException(); $b=ord($wire[$i++]); $n |= ($b & 127) << $shift; $shift += 7; } while ($b & 128); return $n; };
        while ($i < $len) { $tag=$varint(); $field=$tag >> 3; $type=$tag & 7;
            if ($type === 0) { $value=$varint(); }
            elseif ($type === 2) { $size=$varint(); $value=substr($wire,$i,$size); $i += $size; }
            else throw new \UnexpectedValueException();
            $out[]=[$field,$value];
        } return $out;
    }
}
