<?php
declare(strict_types=1);
require __DIR__ . '/../src/ScrivaDBException.php';
$vi = static function (int $n): string { $s=''; do { $b=$n & 127; $n >>= 7; $s .= chr($n ? $b|128 : $b); } while ($n); return $s; };
$field = static fn(int $n,string $v): string => $vi(($n<<3)|2).$vi(strlen($v)).$v;
$pair=$field(1,'status_ref').$field(2,'idem-9');
$info=$field(1,'XTX_OUTCOME_UNKNOWN').$field(2,'scriva.xtx').$field(3,$pair);
$any=$field(1,'type.googleapis.com/google.rpc.ErrorInfo').$field(2,$info);
$wire=$vi(8).$vi(2).$field(3,$any);
$error = new \ScrivaDB\XTxException((object)['code'=>2,'details'=>'unknown','metadata'=>['grpc-status-details-bin'=>[$wire]]]);
assert($error->reason === 'XTX_OUTCOME_UNKNOWN');
assert($error->errorMetadata['status_ref'] === 'idem-9');
echo "ok\n";
