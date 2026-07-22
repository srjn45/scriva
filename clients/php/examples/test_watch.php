<?php
/**
 * ScrivaDB PHP SDK — Watch streaming example.
 *
 * Subscribes to a Watch stream on a collection, inserts records from a
 * background process (using proc_open / forked child), and prints each
 * change event as it arrives.
 *
 * Start the server first:
 *
 *   make run
 *
 * Then run:
 *
 *   cd clients/php
 *   composer install
 *   php examples/test_watch.php
 *
 * Press Ctrl+C to stop.
 */

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use ScrivaDB\ScrivaDB;

$host   = $argv[1] ?? 'localhost';
$port   = (int)($argv[2] ?? 5433);
$apiKey = $argv[3] ?? 'dev-key';

const WATCH_COLLECTION = 'test_php_watch';

$db = new ScrivaDB($host, $port, $apiKey);

// ── Setup ────────────────────────────────────────────────────────────────────
$existing = $db->listCollections();
if (in_array(WATCH_COLLECTION, $existing, true)) {
    $db->dropCollection(WATCH_COLLECTION);
}
$db->createCollection(WATCH_COLLECTION);
echo "Watching collection '" . WATCH_COLLECTION . "' — inserting 3 records from a child process ...\n\n";

// ── Fork a child to do inserts after a short delay ───────────────────────────
$pid = pcntl_fork();
if ($pid === -1) {
    // pcntl_fork not available — fall back to inserting in the same process
    // after starting the watch (this won't actually work for long-running watch,
    // but demonstrates the API shape).
    $pid = 0;
}

if ($pid === 0 && function_exists('pcntl_fork')) {
    // Child process: sleep briefly then insert records
    usleep(300_000); // 300ms

    $child = new ScrivaDB($host, $port, $apiKey);
    $child->insert(WATCH_COLLECTION, ['event' => 'one',   'ts' => time()]);
    usleep(200_000);
    $child->insert(WATCH_COLLECTION, ['event' => 'two',   'ts' => time()]);
    usleep(200_000);
    $child->update(WATCH_COLLECTION, 1, ['event' => 'one-updated', 'ts' => time()]);
    usleep(200_000);
    $child->delete(WATCH_COLLECTION, 2);
    exit(0);
}

// ── Parent: consume Watch stream (stops after 4 events) ──────────────────────
$maxEvents = 4;
$received  = 0;

foreach ($db->watch(WATCH_COLLECTION) as $event) {
    echo sprintf(
        "[%s] op=%-10s id=%s data=%s\n",
        $event['ts'] ?? 'n/a',
        $event['op'],
        $event['record']['id'] ?? '?',
        json_encode($event['record']['data'] ?? [])
    );
    ++$received;
    if ($received >= $maxEvents) {
        break;
    }
}

// ── Cleanup ───────────────────────────────────────────────────────────────────
if ($pid > 0 && function_exists('pcntl_waitpid')) {
    pcntl_waitpid($pid, $childStatus);
}

$db->dropCollection(WATCH_COLLECTION);
echo "\nWatch example complete. Received $received events.\n";
