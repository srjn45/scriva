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
# Requires protoc and grpc_php_plugin — see generate.sh for details
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
    orderBy: 'age',
    descending: false,
);

// Update — returns the updated ID
int $id = $db->update('col', $id, ['field' => 'new value']);

// Delete — returns true if the record existed
bool $deleted = $db->delete('col', $id);
```

Record array shape:

```php
[
    'id'            => '1',                      // uint64 returned as string
    'data'          => ['name' => 'Alice', ...], // the document
    'date_added'    => '2026-06-29T12:00:00+00:00',  // ISO-8601, present when set
    'date_modified' => '2026-06-29T12:01:00+00:00',
]
```

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
# Install protoc: https://github.com/protocolbuffers/protobuf/releases
# Install grpc_php_plugin: https://github.com/grpc/grpc

./generate.sh
```

The script reads `../../proto/filedb.proto` and writes to `src/Proto/`.

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
