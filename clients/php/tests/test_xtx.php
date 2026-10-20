<?php
declare(strict_types=1);

// Run after Composer installs runtime dependencies. This hermetic fake verifies
// the public wrapper's exact BeginXTx wire fields without a server.
require __DIR__ . '/../vendor/autoload.php';

$stub = new class {
    public mixed $request;
    public int $commits = 0;
    public array $stages = [];
    public function BeginXTx($request, $metadata): object {
        $this->request = $request;
        $response = new \Scriva\V1\BeginXTxResponse();
        $response->setXtxId('x'); $response->setCollections(['a', 'b']); $response->setIdempotencyKey('key');
        return new class($response) { function __construct(private $r) {} function wait(): array { return [$this->r, (object)['code' => 0]]; } };
    }
    public function CommitXTx($request, $metadata): object {
        $this->commits++;
        $wire = $GLOBALS['xtxStatusWire'];
        return new class($wire) { function __construct(private string $wire) {} function wait(): array { return [null, (object)['code'=>2,'details'=>'unknown','metadata'=>['grpc-status-details-bin'=>[$this->wire]]]]; } };
    }
    public function StageXTx($request, $metadata): object {
        $this->stages[] = $request;
        $response = new \Scriva\V1\StageXTxResponse(); $response->setId($request->getId() ?: 17);
        return new class($response) { function __construct(private $r) {} function wait(): array { return [$this->r,(object)['code'=>0]]; } };
    }
    public function GetXTx($request, $metadata): object {
        $record = new \Scriva\V1\Record(); $record->setId($request->getId()); $record->setRev(2);
        $response = new \Scriva\V1\GetXTxResponse(); $response->setRecord($record);
        return new class($response) { function __construct(private $r) {} function wait(): array { return [$this->r,(object)['code'=>0]]; } };
    }
    public function RollbackXTx($request, $metadata): object {
        $response = new \Scriva\V1\RollbackXTxResponse(); $response->setOk(true);
        return new class($response) { function __construct(private $r) {} function wait(): array { return [$this->r,(object)['code'=>0]]; } };
    }
    public function XTxStatus($request, $metadata): object {
        $GLOBALS['statusRef'] = $request->getRef();
        $response = new \Scriva\V1\XTxStatusResponse(); $response->setStatus(\Scriva\V1\XTxState::XTX_STATUS_COMMITTED); $response->setTxId('durable');
        return new class($response) { function __construct(private $r) {} function wait(): array { return [$this->r,(object)['code'=>0]]; } };
    }
};
$db = new \ScrivaDB\ScrivaDB('localhost', 5433, '');
$property = new ReflectionProperty($db, 'stub'); $property->setValue($db, $stub);
$result = $db->beginXTx(['b', 'a'], ['idempotencyKey' => 'key', 'idleTimeoutMs' => 12, 'maxLifetimeMs' => 34]);
assert($result['xtxId'] === 'x');
assert($stub->request->getIdleTimeoutMs() === 12);
assert($stub->request->getMaxLifetimeMs() === 34);
$id = $db->stageXTx($result['xtxId'], ['collection'=>'a','op'=>'insert','data'=>['n'=>1]]);
$db->stageXTx($result['xtxId'], ['collection'=>'a','op'=>'update','id'=>(int)$id,'expectedRev'=>2,'data'=>['n'=>2]]);
$db->stageXTx($result['xtxId'], ['collection'=>'b','op'=>'delete','id'=>9,'expectedRev'=>3]);
assert($stub->stages[0]->getOp() === \Scriva\V1\XTxOpKind::XTX_OP_INSERT);
assert($stub->stages[1]->getExpectedRev() === 2);
assert(!$stub->stages[2]->hasData());
assert($db->getXTx($result['xtxId'], 'a', (int)$id)['rev'] === 2);
assert($db->rollbackXTx($result['xtxId']) === true);
$varint = static function (int $n): string { $out=''; do { $b=$n & 127; $n >>= 7; $out .= chr($n ? $b|128 : $b); } while ($n); return $out; };
$bytes = static fn(int $n, string $v): string => $varint(($n<<3)|2).$varint(strlen($v)).$v;
$str = static fn(int $n,string $v): string => $bytes($n,$v);
$pair = $str(1,'status_ref').$str(2,'idem-9');
$info = $str(1,'XTX_OUTCOME_UNKNOWN').$str(2,'scriva.xtx').$bytes(3,$pair);
$any = $str(1,'type.googleapis.com/google.rpc.ErrorInfo').$bytes(2,$info);
$GLOBALS['xtxStatusWire'] = $varint(8).$varint(2).$bytes(3,$any);
try { $db->commitXTx('x'); throw new RuntimeException('expected XTxException'); }
catch (\ScrivaDB\XTxException $e) {
    assert($e->reason === 'XTX_OUTCOME_UNKNOWN');
    assert($e->errorMetadata['status_ref'] === 'idem-9');
    assert($db->xTxStatus($e->errorMetadata['status_ref'])['txId'] === 'durable');
    assert($stub->commits === 1 && $GLOBALS['statusRef'] === 'idem-9');
}
echo "ok\n";
