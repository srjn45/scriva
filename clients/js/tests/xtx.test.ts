import assert from 'node:assert/strict';
import { ScrivaDB } from '../src/client';

const unary = (response: any) => (request: any, _metadata: any, callback: any) => { calls.push(request); callback(null, response); };
const calls: any[] = [];
const db: any = new ScrivaDB('127.0.0.1', 1, '');
db.stub = {
  BeginXTx: unary({ xtx_id: 'x', collections: ['a'], idempotency_key: 'key' }),
  StageXTx: unary({ id: '7' }), GetXTx: unary({ record: { id: '7', rev: '2', data: { fields: {} } } }),
  CommitXTx: unary({ tx_id: 'durable', replayed: true, ops: [] }), RollbackXTx: unary({ ok: true }),
  XTxStatus: unary({ status: 'XTX_STATUS_COMMITTED', tx_id: 'durable' }),
};

(async () => {
  const x = await db.beginXTx(['a'], { idempotencyKey: 'key', idleTimeoutMs: 12, maxLifetimeMs: 34 });
  assert.equal(x.xtxId, 'x'); assert.equal(calls[0].idle_timeout_ms, 12);
  assert.equal(await db.stageXTx('x', { collection: 'a', op: 'insert', data: { n: 1 } }), '7');
  await db.getXTx('x', 'a', '7'); assert.equal((await db.commitXTx('x', { lockTimeoutMs: 9 })).txId, 'durable');
  assert.equal(calls[3].lock_timeout_ms, 9); assert.equal(await db.rollbackXTx('x'), true);
  assert.equal((await db.xTxStatus('key')).txId, 'durable');
})().catch((error) => { console.error(error); process.exitCode = 1; });
