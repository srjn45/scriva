# FileDB v2 — PHP Client

PHP 8.1+ gRPC client for [FileDB v2](../../README.md).

**Packagist package:** `srjn45/filedbv2`

---

## Requirements

- PHP 8.1+
- The [gRPC PHP extension](https://grpc.io/docs/languages/php/quickstart/) (`pecl install grpc`)
- The [Protobuf PHP extension](https://github.com/protocolbuffers/protobuf/tree/main/php) (`pecl install protobuf`) — or the pure-PHP implementation shipped with `google/protobuf`
- A running FileDB v2 server (`make run` from the repo root)

---

## Install

```bash
composer require srjn45/filedbv2
```

---

## Install from source

```bash
cd clients/php
composer install
```

To regenerate the gRPC stubs from `proto/filedb.proto`:

```bash
# Requires only buf — plugins are pulled from the Buf Schema Registry.
./generate.sh
```

---

## Quick start

```php
<?php
require 'vendor/autoload.php';

use FileDBv2\FileDB;

$db = new FileDB('localhost', 5433, 'dev-key');

$db->createCollection('users');

$id = $db->insert('users', ['name' => 'Alice', 'age' => 30, 'role' => 'admin']);

$record = $db->findById('users', $id);
// ['id' => '1', 'data' => ['name' => 'Alice', 'age' => 30, ...], 'date_added' => '...']

$admins = $db->find('users', ['field' => 'role', 'op' => 'eq', 'value' => 'admin']);

$db->update('users', $id, ['name' => 'Alice', 'age' => 31, 'role' => 'superadmin']);
$db->delete('users', $id);
$db->dropCollection('users');
```

`x-api-key` is attached as gRPC metadata on every call automatically.

---

## API reference

### Constructor

```php
// Plaintext (no TLS)
$db = new FileDB('localhost', 5433, 'dev-key');

// TLS — verify the server against a CA certificate
$db = new FileDB('myserver.example.com', 5433, 'api-key', '/path/to/ca.crt');
```

### Collection management

```php
string   $name   = $db->createCollection('col');
bool     $ok     = $db->dropCollection('col');
string[] $names  = $db->listCollections();

// Give the collection a default per-record TTL (seconds). Records inserted
// without an explicit ttl then expire this many seconds after being written.
string $name = $db->createCollection('sessions', defaultTtlSeconds: 3600);
```

### CRUD

```php
// Insert one record — returns the assigned integer ID
int $id = $db->insert('col', ['field' => 'value']);

// Insert many — returns IDs in insertion order
int[] $ids = $db->insertMany('col', [['name' => 'Alice'], ['name' => 'Bob']]);

// Find by ID — returns a record array
array $record = $db->findById('col', $id);

// Query — returns an array of record arrays (server-streaming, results collected)
array $records = $db->find(
    'col',
    filter: ['field' => 'age', 'op' => 'gte', 'value' => '18'],
    limit: 0,           // 0 = no limit
    offset: 0,
    orderBy: 'age',     // deprecated single-field sort (see orderByFields below)
    descending: false,
    fields: [],         // field projection (N2) — [] returns full records
    orderByFields: null, // multi-field sort (N3) — see "Ordering & pagination"
    pageToken: '',      // keyset cursor (N3) — see "Ordering & pagination"
);

// Update — returns the updated ID
int $id = $db->update('col', $id, ['field' => 'new value']);

// Delete — returns true if the record existed
bool $deleted = $db->delete('col', $id);
```

#### Keyed CRUD, upsert & compare-and-swap (N1)

Records can carry a caller-supplied string **key** and a monotonic **rev**ision.
These map onto the engine's keyed operations — natural primary keys, upsert, and
optimistic-concurrency updates.

```php
// Insert under an explicit key. A key already held by a live record throws
// FileDBv2\AlreadyExistsException. (Keyed inserts ignore ttlSeconds.)
int $id = $db->insert('col', ['name' => 'Alice'], key: 'user:alice');

// Upsert = insert-or-replace, atomically. Returns the resulting record array
// (with 'key' and 'rev'); rev increments on every replace.
array $rec = $db->upsert('col', 'user:alice', ['name' => 'Alice', 'age' => 30]);

// Fetch / update / delete by key. A missing key throws FileDBv2\NotFoundException.
array $rec  = $db->findByKey('col', 'user:alice');
array $res  = $db->updateByKey('col', 'user:alice', ['name' => 'Alice', 'age' => 31]);
//   $res = ['id' => '1', 'key' => 'user:alice', 'rev' => 3, 'date_modified' => '...']
bool  $ok   = $db->deleteByKey('col', 'user:alice');

// Compare-and-swap: applies the write only if the current rev matches. A stale
// rev (or missing key) is a clean no-op — swapped=false, never an error.
$cas = $db->updateIfRev('col', 'user:alice', expectedRev: 2, data: ['age' => 32]);
// $cas = ['swapped' => true|false, 'record' => [...]|null]
if ($cas['swapped']) {
    echo "new rev: {$cas['record']['rev']}\n";
}
```

#### Field projection (N2)

`find()`, `findById()` and `findByKey()` accept a `fields` argument. When
non-empty, only those top-level fields are returned in each record's `data`;
`id`, `key` and `rev` are always included. An unknown field is silently omitted.

```php
array $slim = $db->findById('col', $id, fields: ['name', 'email']);
array $rows = $db->find('col', fields: ['name']);
```

#### Ordering & pagination (N3)

Pass `orderByFields` for a multi-field, per-field-directional sort (it supersedes
the deprecated scalar `orderBy`/`descending`). The record id is always the final
tiebreaker, so the sort is total and pagination is stable.

```php
$rows = $db->find('col', orderByFields: [
    ['field' => 'age',  'desc' => true],
    ['field' => 'name', 'desc' => false],
]);
```

For keyset (cursor) pagination use `findPage()`, which returns both the records
and the opaque next-page token. Feed the token back as `pageToken` with the same
filter, ordering and limit to fetch the next page; an empty token means the last
page was reached.

```php
$page = $db->findPage('col', limit: 100, orderByFields: [['field' => 'age']]);
foreach ($page['records'] as $r) { /* ... */ }
while ($page['page_token'] !== '') {
    $page = $db->findPage(
        'col',
        limit: 100,
        orderByFields: [['field' => 'age']],
        pageToken: $page['page_token'],
    );
    // process $page['records'] ...
}
```

#### Aggregations (N4)

`aggregate()` computes a count plus optional numeric aggregations
(`sum`/`avg`/`min`/`max`) over the records matching a filter, optionally grouped
by a field. It honours the same filter format as `find()`.

```php
// Count matching records (whole collection when no filter is given).
int $n = $db->count('col');
int $m = $db->count('col', ['field' => 'role', 'op' => 'eq', 'value' => 'admin']);

// Group by a field, computing numeric aggregates over another field.
$groups = $db->groupBy('col', 'role', 'age', ['sum', 'avg', 'min', 'max']);
foreach ($groups as $g) {
    // $g = [
    //   'group_value' => 'admin',  // null for the whole-set / missing-field group
    //   'count'       => 3,
    //   'numeric'     => true,      // false => sum/avg/min/max keys are absent
    //   'sum' => 90.0, 'avg' => 30.0, 'min' => 25.0, 'max' => 35.0,
    // ]
}

// The low-level entry point; groupBy/count are thin wrappers over it.
$all = $db->aggregate('col',
    filter: ['field' => 'age', 'op' => 'gte', 'value' => '18'],
    groupBy: 'role',
    field: 'age',
    aggregations: ['avg'],
);
```

#### Per-record TTL

`insert`, `insertMany`, and `update` accept a `ttlSeconds` argument:

```php
// Expire this record 60 seconds from now, regardless of the collection default.
int $id = $db->insert('sessions', ['token' => 'abc'], ttlSeconds: 60);

// Same TTL applied to every record in the batch.
int[] $ids = $db->insertMany('sessions', [['token' => 'a'], ['token' => 'b']], ttlSeconds: 60);

// On update, ttlSeconds > 0 resets the expiry; ttlSeconds: 0 (default) is
// sticky and leaves the existing deadline untouched.
$db->update('sessions', $id, ['token' => 'abc', 'seen' => true], ttlSeconds: 120);
```

`ttlSeconds: 0` (the default) inherits the collection's default TTL on insert; a
value greater than 0 overrides it. Negative values are rejected by the server.

Record array shape:

```php
[
    'id'            => '1',                      // uint64 returned as string
    'data'          => ['name' => 'Alice', ...], // the document
    'key'           => 'user:alice',            // caller-supplied key, present only for keyed records
    'rev'           => 1,                        // monotonic per-record revision (N1)
    'date_added'    => '2026-06-29T12:00:00+00:00',  // ISO-8601, present when set
    'date_modified' => '2026-06-29T12:01:00+00:00',
]
```

`key` is omitted for records inserted without a key; `rev` starts at 1 and
increments on every write. Feed `rev` to `updateIfRev()` for compare-and-swap.

### Secondary indexes

```php
$db->ensureIndex('col', 'field');
bool     $ok     = $db->dropIndex('col', 'field');
string[] $fields = $db->listIndexes('col');
```

Once an index exists, `find()` with a single `eq` filter on that field uses the
index automatically — no query hint needed.

### Transactions

```php
string $txId = $db->beginTx('col');
bool $ok      = $db->commitTx($txId);
bool $ok      = $db->rollbackTx($txId);
```

### Watch (streaming change feed)

`watch()` returns a PHP `Generator`. Each value is an event array:

```php
foreach ($db->watch('col') as $event) {
    // $event['op']         => 'INSERTED' | 'UPDATED' | 'DELETED'
    // $event['collection'] => 'col'
    // $event['record']     => [...record array...]
    // $event['ts']         => '2026-06-29T...' (ISO-8601)
    echo $event['op'] . ' id=' . $event['record']['id'] . "\n";
}
```

With an optional filter — only matching events are delivered:

```php
foreach ($db->watch('col', ['field' => 'role', 'op' => 'eq', 'value' => 'admin']) as $event) {
    // ...
}
```

Break out of the loop to stop watching.

### Stats

```php
$s = $db->stats('col');
// [
//   'collection'    => 'col',
//   'record_count'  => 3,
//   'segment_count' => 1,
//   'dirty_entries' => 0,
//   'size_bytes'    => 512,
// ]
```

### Maintenance

```php
// Force a synchronous compaction of a collection — merges dirty segments and
// reclaims space from deleted/overwritten records. Returns true on success.
bool $ok = $db->compact('col');

// Stream a consistent gzip-compressed tar snapshot of the whole database
// straight to a file. Returns the number of bytes written; restore with
// `tar xzf backup.tar.gz`.
int $bytes = $db->snapshotToFile('backup.tar.gz');

// Or consume the raw gzip byte chunks yourself (Snapshot is server-streaming):
foreach ($db->snapshot() as $chunk) {
    // $chunk is a string of bytes
}
```

---

## Error handling

Failed RPCs throw `FileDBv2\FileDBException` (which extends `\RuntimeException`,
so existing `catch (\RuntimeException)` blocks keep working). Two gRPC status
codes get their own subclass for idiomatic keyed-CRUD handling:

| Exception | gRPC status | Raised by |
|---|---|---|
| `FileDBv2\NotFoundException`      | `NOT_FOUND`      | `findByKey`, `updateByKey`, `deleteByKey` on a missing key |
| `FileDBv2\AlreadyExistsException` | `ALREADY_EXISTS` | `insert(..., key: ...)` when the key is already taken |
| `FileDBv2\FileDBException`        | any other        | all other RPC failures |

```php
use FileDBv2\NotFoundException;

try {
    $rec = $db->findByKey('col', 'user:missing');
} catch (NotFoundException $e) {
    // handle the missing key
}
```

Note `updateIfRev()` does **not** throw on a stale revision or missing key — it
returns `['swapped' => false, 'record' => null]`.

---

## Filter syntax

Filters are plain PHP arrays.

### Field filter

```php
['field' => 'age',   'op' => 'gt',       'value' => '30']
['field' => 'name',  'op' => 'contains', 'value' => 'alice']
['field' => 'email', 'op' => 'regex',    'value' => '.*@gmail\\.com']
```

### AND composite

```php
['and' => [
    ['field' => 'age',  'op' => 'gte', 'value' => '18'],
    ['field' => 'city', 'op' => 'eq',  'value' => 'Berlin'],
]]
```

### OR composite

```php
['or' => [
    ['field' => 'role', 'op' => 'eq', 'value' => 'admin'],
    ['field' => 'role', 'op' => 'eq', 'value' => 'superadmin'],
]]
```

Composites can be nested arbitrarily.

### Supported `op` values

| `op`       | Meaning                     |
|------------|-----------------------------|
| `eq`       | equal                       |
| `neq`      | not equal                   |
| `gt`       | greater than                |
| `gte`      | greater than or equal       |
| `lt`       | less than                   |
| `lte`      | less than or equal          |
| `contains` | string contains (substring) |
| `regex`    | regular expression match    |

---

## TLS

```php
$db = new FileDB('myserver.example.com', 5433, 'api-key', '/path/to/ca.crt');
```

When no CA cert path is supplied the client connects over plaintext.

---

## Regenerating proto stubs

```bash
# Install buf: https://buf.build/docs/installation
./generate.sh
```

The script reads `../../proto/filedb.proto` and writes to `src/Proto/`. It uses
`buf` with the `protocolbuffers/php` and `grpc/php` remote plugins (pinned to
versions matching the `google/protobuf: ^3.25` runtime), so no local `protoc` or
`grpc_php_plugin` install is needed.

---

## Running the examples

Start the server first (from the repo root):

```bash
make run
```

Then, in `clients/php`:

```bash
composer install
php examples/test_basic.php
php examples/test_watch.php
```

---

## Publish to Packagist

1. Push a tag: `git tag clients/php/v0.1.0 && git push origin clients/php/v0.1.0`
2. Submit the repository on [packagist.org](https://packagist.org/packages/submit)
   using the GitHub repository URL.
3. Packagist auto-detects the `composer.json` at `clients/php/composer.json`
   when the package root is set to `clients/php/` in the Packagist submission form.
