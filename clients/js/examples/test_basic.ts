/**
 * test_basic.ts — end-to-end example for the FileDB v2 TypeScript client.
 *
 * Prerequisites:
 *   - FileDB server running: `make run` from the repo root
 *
 * Run:
 *   npx ts-node examples/test_basic.ts
 */

import { FileDB } from '../src';

async function main(): Promise<void> {
  const db = new FileDB('localhost', 5433, 'dev-key');

  // --- Collection management ---
  console.log('=== Collections ===');
  await db.createCollection('test_js');
  console.log('Created collection. All collections:', await db.listCollections());

  // --- Insert ---
  console.log('\n=== Insert ===');
  const id1 = await db.insert('test_js', { name: 'Alice', age: 30, role: 'admin' });
  const id2 = await db.insert('test_js', { name: 'Bob',   age: 25, role: 'user'  });
  const id3 = await db.insert('test_js', { name: 'Carol', age: 35, role: 'admin' });
  console.log('Inserted IDs:', id1, id2, id3);

  const ids = await db.insertMany('test_js', [
    { name: 'Dave', age: 28, role: 'user' },
    { name: 'Eve',  age: 22, role: 'user' },
  ]);
  console.log('InsertMany IDs:', ids);

  // --- Find by ID ---
  console.log('\n=== FindById ===');
  const record = await db.findById('test_js', id1);
  console.log('Record:', record);

  // --- Find with filter + multi-field sort (N3) ---
  console.log('\n=== Find (filter: role=admin, order by name) ===');
  const admins = await db.findAll('test_js', {
    filter: { field: 'role', op: 'eq', value: 'admin' },
    orderBy: [{ field: 'name' }],
  });
  console.log('Admins:', admins.map(r => `${r.id}: ${JSON.stringify(r.data)}`));

  // --- AND filter ---
  console.log('\n=== Find (AND: role=user AND age>=25) ===');
  const filtered = await db.findAll('test_js', {
    filter: {
      and: [
        { field: 'role', op: 'eq',  value: 'user' },
        { field: 'age',  op: 'gte', value: '25'   },
      ],
    },
  });
  console.log('Filtered:', filtered.map(r => r.data));

  // --- Streaming find ---
  console.log('\n=== Find (streaming, limit 2) ===');
  for await (const r of db.find('test_js', { limit: 2 })) {
    console.log(' -', r.id, r.data);
  }

  // --- Projection (N2): only return selected fields ---
  console.log('\n=== Find (projection: name only) ===');
  const projected = await db.findAll('test_js', {
    fields: ['name'],
    orderBy: [{ field: 'name' }],
  });
  console.log('Projected (id/key/rev always included):', projected.map(r => r.data));

  // --- Keyset pagination (N3): page through by cursor ---
  console.log('\n=== Find (keyset pagination, page size 2) ===');
  let token = '';
  let page = 0;
  do {
    const p = await db.findPage('test_js', {
      orderBy: [{ field: 'age' }],
      limit: 2,
      pageToken: token,
    });
    console.log(` page ${++page}:`, p.records.map(r => `${r.data.name}(${r.data.age})`));
    token = p.pageToken;
  } while (token);

  // --- Keyed CRUD / Upsert / CAS (N1) ---
  console.log('\n=== Keyed CRUD (N1) ===');
  const up1 = await db.upsert('test_js', 'user:alice', { name: 'Alice', tier: 'free' });
  console.log('Upsert insert:', up1.key, 'rev', up1.rev, up1.data);
  const up2 = await db.upsert('test_js', 'user:alice', { name: 'Alice', tier: 'pro' });
  console.log('Upsert replace:', up2.key, 'rev', up2.rev, up2.data);

  const byKey = await db.findByKey('test_js', 'user:alice');
  console.log('FindByKey:', byKey?.data, 'rev', byKey?.rev);
  console.log('FindByKey (missing → null):', await db.findByKey('test_js', 'user:nobody'));

  const w = await db.updateByKey('test_js', 'user:alice', { name: 'Alice', tier: 'enterprise' });
  console.log('UpdateByKey → rev', w.rev);

  // Compare-and-swap: succeeds with the current rev, no-ops with a stale one.
  const cur = await db.findByKey('test_js', 'user:alice');
  const ok = await db.updateIfRev('test_js', 'user:alice', cur!.rev, { name: 'Alice', tier: 'vip' });
  console.log('CAS with current rev → swapped:', ok.swapped, 'newRev', ok.record?.rev);
  const stale = await db.updateIfRev('test_js', 'user:alice', cur!.rev, { name: 'Alice', tier: 'x' });
  console.log('CAS with stale rev → swapped:', stale.swapped);

  console.log('DeleteByKey:', await db.deleteByKey('test_js', 'user:alice'));
  console.log('DeleteByKey (missing → false):', await db.deleteByKey('test_js', 'user:nobody'));

  // --- Aggregations (N4) ---
  console.log('\n=== Aggregations (N4) ===');
  console.log('Total count:', await db.count('test_js'));
  console.log('Count (role=user):', await db.count('test_js', { field: 'role', op: 'eq', value: 'user' }));
  const byRole = await db.groupBy('test_js', 'role', {
    field: 'age',
    aggregations: ['sum', 'avg', 'min', 'max'],
  });
  for (const g of byRole) {
    console.log(` group ${JSON.stringify(g.group)}: count=${g.count}` +
      (g.numeric ? ` sum=${g.sum} avg=${g.avg} min=${g.min} max=${g.max}` : ''));
  }

  // --- Update ---
  console.log('\n=== Update ===');
  await db.update('test_js', id1, { name: 'Alice', age: 31, role: 'superadmin' });
  console.log('Updated:', await db.findById('test_js', id1));

  // --- Delete ---
  console.log('\n=== Delete ===');
  const deleted = await db.delete('test_js', id2);
  console.log('Deleted id2:', deleted);

  // --- Indexes ---
  console.log('\n=== Indexes ===');
  await db.ensureIndex('test_js', 'role');
  console.log('Indexes:', await db.listIndexes('test_js'));

  // find via index (single eq on indexed field)
  const indexed = await db.findAll('test_js', {
    filter: { field: 'role', op: 'eq', value: 'user' },
  });
  console.log('Users (via index):', indexed.map(r => r.data));

  await db.dropIndex('test_js', 'role');
  console.log('Indexes after drop:', await db.listIndexes('test_js'));

  // --- Transactions ---
  console.log('\n=== Transactions ===');
  const txId = await db.beginTx('test_js');
  console.log('TX ID:', txId);
  const committed = await db.commitTx(txId);
  console.log('Committed:', committed);

  const txId2 = await db.beginTx('test_js');
  const rolledBack = await db.rollbackTx(txId2);
  console.log('Rolled back:', rolledBack);

  // --- Stats ---
  console.log('\n=== Stats ===');
  const s = await db.stats('test_js');
  console.log('Stats:', s);

  // --- TTL (per-record and per-collection default) ---
  console.log('\n=== TTL ===');
  await db.createCollection('test_js_ttl', 3600);
  await db.insert('test_js_ttl', { kind: 'inherits-collection-default' });
  await db.insert('test_js_ttl', { kind: 'own-ttl' }, 60);
  console.log('TTL collection stats:', await db.stats('test_js_ttl'));
  await db.dropCollection('test_js_ttl');

  // --- Maintenance ---
  console.log('\n=== Compact ===');
  console.log('Compacted:', await db.compact('test_js'));

  // --- Backup ---
  console.log('\n=== Snapshot ===');
  const bytes = await db.snapshotToFile('filedb-backup.tar.gz');
  console.log(`Wrote ${bytes} bytes to filedb-backup.tar.gz (restore with: tar xzf ...)`);

  // --- Cleanup ---
  console.log('\n=== Cleanup ===');
  await db.dropCollection('test_js');
  console.log('Collections after drop:', await db.listCollections());

  db.close();
  console.log('\nAll done!');
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
