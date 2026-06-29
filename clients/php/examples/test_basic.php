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

// ── Drop collection ──────────────────────────────────────────────────────────
$ok = $db->dropCollection(COLLECTION);
echo 'dropCollection: ' . ($ok ? 'ok' : 'failed') . "\n";

echo "\nAll tests passed.\n";
