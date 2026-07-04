<?php
/**
 * FileDB v2 PHP SDK — basic end-to-end example.
 *
 * Start the server first (from the repo root):
 *
 *   make run
 *
 * Then run this script:
 *
 *   cd clients/php
 *   composer install
 *   php examples/test_basic.php
 */

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use FileDBv2\FileDB;

$host   = $argv[1] ?? 'localhost';
$port   = (int)($argv[2] ?? 5433);
$apiKey = $argv[3] ?? 'dev-key';

$db = new FileDB($host, $port, $apiKey);

const COLLECTION = 'test_php';

// ── Clean up from any previous run ──────────────────────────────────────────
$existing = $db->listCollections();
if (in_array(COLLECTION, $existing, true)) {
    $db->dropCollection(COLLECTION);
    echo "Dropped pre-existing collection '" . COLLECTION . "'\n";
}

// ── Create collection ────────────────────────────────────────────────────────
$name = $db->createCollection(COLLECTION);
echo "Created collection: $name\n";

$collections = $db->listCollections();
echo 'Collections: ' . implode(', ', $collections) . "\n";

// ── Insert records ───────────────────────────────────────────────────────────
$id1 = $db->insert(COLLECTION, ['name' => 'Alice', 'age' => 30, 'role' => 'admin']);
$id2 = $db->insert(COLLECTION, ['name' => 'Bob',   'age' => 25, 'role' => 'user']);
$id3 = $db->insert(COLLECTION, ['name' => 'Carol', 'age' => 35, 'role' => 'admin']);
echo "Inserted IDs: $id1, $id2, $id3\n";

// ── Insert many ──────────────────────────────────────────────────────────────
$ids = $db->insertMany(COLLECTION, [
    ['name' => 'Dave', 'age' => 28, 'role' => 'user'],
    ['name' => 'Eve',  'age' => 22, 'role' => 'user'],
]);
echo 'InsertMany IDs: ' . implode(', ', $ids) . "\n";

// ── Find by ID ───────────────────────────────────────────────────────────────
$record = $db->findById(COLLECTION, $id1);
echo 'FindById #' . $id1 . ': ' . json_encode($record['data']) . "\n";

// ── Find with filter ─────────────────────────────────────────────────────────
$admins = $db->find(COLLECTION, ['field' => 'role', 'op' => 'eq', 'value' => 'admin']);
echo 'Admins (' . count($admins) . "): ";
foreach ($admins as $r) {
    echo $r['data']['name'] . ' ';
}
echo "\n";

// AND filter
$youngAdmins = $db->find(COLLECTION, [
    'and' => [
        ['field' => 'role', 'op' => 'eq',  'value' => 'admin'],
        ['field' => 'age',  'op' => 'lte', 'value' => '30'],
    ],
]);
echo 'Young admins (age<=30): ';
foreach ($youngAdmins as $r) {
    echo $r['data']['name'] . '(age=' . $r['data']['age'] . ') ';
}
echo "\n";

// Find with orderBy
$byAge = $db->find(COLLECTION, null, 0, 0, 'age', false);
echo 'All records sorted by age: ';
foreach ($byAge as $r) {
    echo $r['data']['name'] . '(' . $r['data']['age'] . ') ';
}
echo "\n";

// ── Update ───────────────────────────────────────────────────────────────────
$updatedId = $db->update(COLLECTION, $id1, ['name' => 'Alice', 'age' => 31, 'role' => 'superadmin']);
echo "Updated record $updatedId\n";
$updated = $db->findById(COLLECTION, $id1);
echo 'After update: ' . json_encode($updated['data']) . "\n";

// ── Secondary indexes ────────────────────────────────────────────────────────
$db->ensureIndex(COLLECTION, 'role');
$indexes = $db->listIndexes(COLLECTION);
echo 'Indexes: ' . implode(', ', $indexes) . "\n";

// Find using index (eq on indexed field)
$admins2 = $db->find(COLLECTION, ['field' => 'role', 'op' => 'eq', 'value' => 'superadmin']);
echo 'Superadmins via index: ';
foreach ($admins2 as $r) {
    echo $r['data']['name'] . ' ';
}
echo "\n";

$dropped = $db->dropIndex(COLLECTION, 'role');
echo 'Drop index: ' . ($dropped ? 'ok' : 'not found') . "\n";

// ── Transactions ─────────────────────────────────────────────────────────────
$txId = $db->beginTx(COLLECTION);
echo "BeginTx: $txId\n";
$committed = $db->commitTx($txId);
echo 'CommitTx: ' . ($committed ? 'ok' : 'failed') . "\n";

$txId2 = $db->beginTx(COLLECTION);
$rolledBack = $db->rollbackTx($txId2);
echo 'RollbackTx: ' . ($rolledBack ? 'ok' : 'failed') . "\n";

// ── Delete ───────────────────────────────────────────────────────────────────
$deleted = $db->delete(COLLECTION, $id2);
echo "Delete #$id2: " . ($deleted ? 'ok' : 'not found') . "\n";

// ── Stats ────────────────────────────────────────────────────────────────────
$stats = $db->stats(COLLECTION);
echo 'Stats: ' . json_encode($stats) . "\n";

// ── Compaction ───────────────────────────────────────────────────────────────
$compacted = $db->compact(COLLECTION);
echo 'Compact: ' . ($compacted ? 'ok' : 'failed') . "\n";

// ── Per-record TTL ───────────────────────────────────────────────────────────
$ttlId = $db->insert(COLLECTION, ['name' => 'Ephemeral', 'role' => 'temp'], ttlSeconds: 3600);
echo "Inserted record #$ttlId with a 3600s TTL\n";
// update with ttlSeconds: 0 is sticky (keeps the existing deadline)
$db->update(COLLECTION, $ttlId, ['name' => 'Ephemeral', 'role' => 'temp', 'touched' => true]);
echo "Updated the TTL record (deadline preserved)\n";

// ── Field projection (N2) ────────────────────────────────────────────────────
$slim = $db->findById(COLLECTION, $id3, fields: ['name']);
echo 'Projected findById (name only): ' . json_encode($slim['data'])
    . " (rev={$slim['rev']})\n";

// ── Keyset pagination + multi-field ordering (N3) ────────────────────────────
$page1 = $db->findPage(
    COLLECTION,
    limit: 2,
    orderByFields: [['field' => 'age', 'desc' => false]],
);
echo 'Page 1 (2 by age asc): ';
foreach ($page1['records'] as $r) {
    echo $r['data']['name'] . '(' . $r['data']['age'] . ') ';
}
echo "\n";
if ($page1['page_token'] !== '') {
    $page2 = $db->findPage(
        COLLECTION,
        limit: 2,
        orderByFields: [['field' => 'age', 'desc' => false]],
        pageToken: $page1['page_token'],
    );
    echo 'Page 2: ';
    foreach ($page2['records'] as $r) {
        echo $r['data']['name'] . '(' . $r['data']['age'] . ') ';
    }
    echo "\n";
}

// ── Keyed CRUD, upsert & compare-and-swap (N1) ───────────────────────────────
$up = $db->upsert(COLLECTION, 'user:alice', ['name' => 'Alice', 'age' => 31]);
echo "Upsert key=user:alice -> rev={$up['rev']}\n";
$up2 = $db->upsert(COLLECTION, 'user:alice', ['name' => 'Alice', 'age' => 32]);
echo "Upsert again (replace) -> rev={$up2['rev']}\n";

$fetched = $db->findByKey(COLLECTION, 'user:alice');
echo 'FindByKey user:alice: ' . json_encode($fetched['data']) . "\n";

// compare-and-swap: stale rev is a clean no-op
$stale = $db->updateIfRev(COLLECTION, 'user:alice', 1, ['name' => 'Alice', 'age' => 99]);
echo 'CAS with stale rev=1: swapped=' . ($stale['swapped'] ? 'true' : 'false') . "\n";
$fresh = $db->updateIfRev(COLLECTION, 'user:alice', $up2['rev'], ['name' => 'Alice', 'age' => 33]);
echo 'CAS with fresh rev=' . $up2['rev'] . ': swapped='
    . ($fresh['swapped'] ? 'true' : 'false') . " (rev={$fresh['record']['rev']})\n";

$db->updateByKey(COLLECTION, 'user:alice', ['name' => 'Alice', 'age' => 34]);
echo "UpdateByKey user:alice ok\n";

// keyed insert conflict maps to AlreadyExistsException
try {
    $db->insert(COLLECTION, ['name' => 'dup'], key: 'user:alice');
    echo "ERROR: expected AlreadyExistsException\n";
} catch (\FileDBv2\AlreadyExistsException $e) {
    echo "Keyed insert on taken key threw AlreadyExistsException (as expected)\n";
}

$db->deleteByKey(COLLECTION, 'user:alice');
echo "DeleteByKey user:alice ok\n";
try {
    $db->findByKey(COLLECTION, 'user:alice');
    echo "ERROR: expected NotFoundException\n";
} catch (\FileDBv2\NotFoundException $e) {
    echo "FindByKey on missing key threw NotFoundException (as expected)\n";
}

// ── Aggregations (N4) ────────────────────────────────────────────────────────
$total = $db->count(COLLECTION);
echo "Count (all): $total\n";
$admins3 = $db->count(COLLECTION, ['field' => 'role', 'op' => 'eq', 'value' => 'user']);
echo "Count (role=user): $admins3\n";

$byRole = $db->groupBy(COLLECTION, 'role', 'age', ['sum', 'avg', 'min', 'max']);
echo "Group by role (age stats):\n";
foreach ($byRole as $g) {
    $gv = $g['group_value'] ?? '(null)';
    if ($g['numeric']) {
        printf(
            "  %-10s count=%d sum=%.0f avg=%.1f min=%.0f max=%.0f\n",
            $gv, $g['count'], $g['sum'], $g['avg'], $g['min'], $g['max']
        );
    } else {
        printf("  %-10s count=%d\n", $gv, $g['count']);
    }
}

// ── Snapshot (whole-database backup) ─────────────────────────────────────────
$backup = sys_get_temp_dir() . '/filedb_php_snapshot.tar.gz';
$bytes = $db->snapshotToFile($backup);
echo "Snapshot: wrote $bytes bytes to $backup\n";
@unlink($backup);

// ── Drop collection ──────────────────────────────────────────────────────────
$ok = $db->dropCollection(COLLECTION);
echo 'dropCollection: ' . ($ok ? 'ok' : 'failed') . "\n";

echo "\nAll tests passed.\n";
