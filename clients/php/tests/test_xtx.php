<?php
declare(strict_types=1);

// Run after Composer installs runtime dependencies. This hermetic fake verifies
// the public wrapper's exact BeginXTx wire fields without a server.
require __DIR__ . '/../vendor/autoload.php';

$stub = new class {
    public mixed $request;
    public function BeginXTx($request, $metadata): object {
        $this->request = $request;
        $response = new \Scriva\V1\BeginXTxResponse();
        $response->setXtxId('x'); $response->setCollections(['a', 'b']); $response->setIdempotencyKey('key');
        return new class($response) { function __construct(private $r) {} function wait(): array { return [$this->r, (object)['code' => 0]]; } };
    }
};
$db = new \ScrivaDB\ScrivaDB('localhost', 5433, '');
$property = new ReflectionProperty($db, 'stub'); $property->setValue($db, $stub);
$result = $db->beginXTx(['b', 'a'], ['idempotencyKey' => 'key', 'idleTimeoutMs' => 12, 'maxLifetimeMs' => 34]);
assert($result['xtxId'] === 'x');
assert($stub->request->getIdleTimeoutMs() === 12);
assert($stub->request->getMaxLifetimeMs() === 34);
echo "ok\n";
