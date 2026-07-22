/**
 * test_watch.ts — demonstrates the Watch streaming RPC.
 *
 * Prerequisites:
 *   - ScrivaDB server running: `make run` from the repo root
 *
 * Run:
 *   npx ts-node examples/test_watch.ts
 */

import { ScrivaDB } from '../src';

const COLLECTION = 'watch_test_js';
const INSERT_COUNT = 5;

async function main(): Promise<void> {
  const db = new ScrivaDB('localhost', 5433, 'dev-key');

  // Clean up from any previous run.
  const existing = await db.listCollections();
  if (existing.includes(COLLECTION)) {
    await db.dropCollection(COLLECTION);
  }
  await db.createCollection(COLLECTION);

  console.log(`Watching "${COLLECTION}" — will insert ${INSERT_COUNT} records...`);

  // Start background inserts with a 300ms interval.
  let insertCount = 0;
  const inserter = setInterval(async () => {
    if (insertCount >= INSERT_COUNT) {
      clearInterval(inserter);
      return;
    }
    insertCount++;
    const id = await db.insert(COLLECTION, {
      msg: `record-${insertCount}`,
      ts: new Date().toISOString(),
    });
    console.log(`  [insert] id=${id}`);
  }, 300);

  // Collect exactly INSERT_COUNT watch events, then break.
  let received = 0;
  for await (const event of db.watch(COLLECTION)) {
    console.log(`  [watch]  op=${event.op} id=${event.record.id} data=${JSON.stringify(event.record.data)}`);
    if (++received >= INSERT_COUNT) break;
  }

  clearInterval(inserter);
  await db.dropCollection(COLLECTION);
  db.close();
  console.log('Watch test done.');
}

main().catch((err: unknown) => {
  console.error(err);
  process.exit(1);
});
