# FileDB v2 — TypeScript / JavaScript Client

Node.js 18+ gRPC client for [FileDB v2](../../README.md).

**npm package:** `filedbv2`

---

## Requirements

- Node.js 18+
- TypeScript 5+ (optional — plain JavaScript works too)
- A running FileDB v2 server (`make run` from the repo root)

---

## Install

```bash
npm install filedbv2
```

---

## Build (from source)

```bash
cd clients/js
npm install
npm run build      # compiles TypeScript → dist/
```

---

## Quick start

```typescript
import { FileDB } from 'filedbv2';

const db = new FileDB('localhost', 5433, 'dev-key');

await db.createCollection('users');

const id = await db.insert('users', { name: 'Alice', age: 30, role: 'admin' });

const record = await db.findById('users', id);
console.log(record); // { id: '1', data: { name: 'Alice', age: 30, role: 'admin' }, ... }

const admins = await db.findAll('users', {
  filter: { field: 'role', op: 'eq', value: 'admin' },
  orderBy: 'name',
});

await db.update('users', id, { name: 'Alice', age: 31, role: 'superadmin' });
await db.delete('users', id);
await db.dropCollection('users');

db.close();
```

CommonJS also works:

```javascript
const { FileDB } = require('filedbv2');
```

---

## API reference

### Constructor

```typescript
// Plaintext (no TLS)
const db = new FileDB(host: string, port: number, apiKey: string);

// TLS — verify server against a CA certificate PEM buffer
const db = new FileDB(host, port, apiKey, tlsCaCert: Buffer);

// TLS — load CA certificate from file path
const db = FileDB.fromTlsCertPath(host, port, apiKey, '/path/to/ca.crt');
```

`x-api-key` is attached as gRPC metadata on every call automatically.

---

### Collection management

```typescript
const name: string      = await db.createCollection('col');
const ok: boolean       = await db.dropCollection('col');
const names: string[]   = await db.listCollections();

// Optional per-collection default TTL (seconds) — records without their own TTL
// expire after this long. Persisted; overrides the server-wide default.
await db.createCollection('sessions', 3600);
```

---

### CRUD

```typescript
// Insert one record — returns the assigned ID (string)
const id: string = await db.insert('col', { field: 'value' });

// Insert multiple records — returns IDs in insertion order
const ids: string[] = await db.insertMany('col', [
  { name: 'Alice' },
  { name: 'Bob' },
]);

// Find by ID
const record: DBRecord = await db.findById('col', id);

// Streaming find — use `for await`
for await (const record of db.find('col', { filter, limit, offset, orderBy, descending })) {
  console.log(record);
}

// Convenience: collect all results into an array
const results: DBRecord[] = await db.findAll('col', { filter });

// Update — returns the updated ID
const updatedId: string = await db.update('col', id, { field: 'new value' });

// Delete — returns true if record existed
const deleted: boolean = await db.delete('col', id);
```

Each write takes an optional trailing `ttlSeconds` argument. When greater than
0 the record expires that many seconds from the write, overriding the
collection default:

```typescript
await db.insert('col', { field: 'value' }, 60);          // expires in 60s
await db.insertMany('col', [{ n: 1 }, { n: 2 }], 120);   // whole batch in 120s
await db.update('col', id, { field: 'v' }, 30);          // reset expiry to 30s
// ttlSeconds omitted (or 0): insert/insertMany apply the collection default;
// update leaves any existing deadline untouched (a plain update is sticky).
```

`DBRecord` shape:

```typescript
interface DBRecord {
  id: string;                       // uint64 returned as string
  data: Record<string, unknown>;
  date_added?: string;
  date_modified?: string;
}
```

---

### Secondary indexes

```typescript
await db.ensureIndex('col', 'fieldName');
const ok: boolean       = await db.dropIndex('col', 'fieldName');
const fields: string[]  = await db.listIndexes('col');
```

Once an index exists, `findAll` / `find` with a single `eq` filter on that field
uses the index automatically — no query hint needed.

---

### Transactions

```typescript
const txId: string  = await db.beginTx('col');
const ok: boolean   = await db.commitTx(txId);
const ok: boolean   = await db.rollbackTx(txId);
```

---

### Watch (streaming change feed)

```typescript
for await (const event of db.watch('col')) {
  console.log(event.op, event.record.id, event.record.data);
  // event.op: 'INSERTED' | 'UPDATED' | 'DELETED'
}
```

With an optional filter — only matching events are delivered:

```typescript
for await (const event of db.watch('col', { field: 'role', op: 'eq', value: 'admin' })) {
  // ...
}
```

Break out of the `for await` loop to stop watching.

---

### Stats

```typescript
const s = await db.stats('col');
// s.collection, s.record_count, s.segment_count, s.dirty_entries, s.size_bytes (all strings)
```

---

### Maintenance

```typescript
// Force a synchronous compaction pass — merges/deduplicates sealed segments and
// reclaims space from deleted or expired records. Resolves true on success.
const ok: boolean = await db.compact('col');
```

---

### Backup

```typescript
// Stream a consistent gzip snapshot of the whole database straight to a file.
// Resolves with the number of bytes written; restore with `tar xzf backup.tar.gz`.
const bytes: number = await db.snapshotToFile('backup.tar.gz');

// Or consume the raw gzip byte chunks yourself (Snapshot is server-streaming):
for await (const chunk of db.snapshot()) {
  // chunk: Buffer
}
```

---

### Lifecycle

```typescript
db.close(); // shuts down the gRPC channel
```

---

## Filter syntax

Filters are plain JavaScript objects.

### Field filter

```typescript
{ field: 'age',  op: 'gt',       value: '30'    }
{ field: 'name', op: 'contains', value: 'alice' }
{ field: 'email',op: 'regex',    value: '.*@gmail\\.com' }
```

### AND composite

```typescript
{
  and: [
    { field: 'age',  op: 'gte', value: '18'    },
    { field: 'city', op: 'eq',  value: 'Berlin' },
  ],
}
```

### OR composite

```typescript
{
  or: [
    { field: 'role', op: 'eq', value: 'admin'     },
    { field: 'role', op: 'eq', value: 'superadmin' },
  ],
}
```

### Supported `op` values

| `op`       | Meaning                    |
|------------|---------------------------|
| `eq`       | equal                      |
| `neq`      | not equal                  |
| `gt`       | greater than               |
| `gte`      | greater than or equal      |
| `lt`       | less than                  |
| `lte`      | less than or equal         |
| `contains` | string contains (substring)|
| `regex`    | regular expression match   |

---

## TLS

```typescript
import * as fs from 'fs';

// From buffer
const db = new FileDB('myserver.example.com', 5433, 'my-api-key',
  fs.readFileSync('/path/to/ca.crt'));

// From path (convenience static factory)
const db = FileDB.fromTlsCertPath('myserver.example.com', 5433, 'my-api-key',
  '/path/to/ca.crt');
```

When no CA cert is supplied the client connects over plaintext (insecure channel).

---

## Running the examples

Start the server first:

```bash
# From repo root
make run
```

Then in a separate terminal:

```bash
cd clients/js

# Install deps
npm install

# Basic CRUD example
npx ts-node examples/test_basic.ts

# Watch streaming example
npx ts-node examples/test_watch.ts
```

---

## Unix socket

Node.js can connect over the Unix domain socket for zero-overhead local connections:

```typescript
import * as grpc from '@grpc/grpc-js';
import { FileDB } from 'filedbv2';

// Pass the socket path as a grpc URI:
//   'unix:///tmp/filedb.sock'
// Use the internal constructor signature with a pre-built stub for advanced use.
```

For the common case, TCP (`localhost:5433`) is sufficient. Unix socket support via
a custom channel address is available using `@grpc/grpc-js` directly.
