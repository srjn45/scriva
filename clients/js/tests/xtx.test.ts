import assert from 'node:assert/strict';
import { ScrivaDB, XTxError } from '../src/client';
import * as grpc from '@grpc/grpc-js';

const unary = (response: any) => (request: any, _metadata: any, callback: any) => { calls.push(request); callback(null, response); };
const calls: any[] = [];
const db: any = new ScrivaDB('127.0.0.1', 1, '');
db.stub = {
  BeginXTx: unary({ xtx_id: 'x', collections: ['a'], idempotency_key: 'key' }),
  StageXTx: unary({ id: '7' }), GetXTx: unary({ record: { id: '7', rev: '2', data: { fields: {} } } }),
  CommitXTx: unary({ tx_id: 'durable', replayed: true, ops: [] }), RollbackXTx: unary({ ok: true }),
  XTxStatus: unary({ status: 'XTX_STATUS_COMMITTED', tx_id: 'durable' }),
  close: () => {},
};

(async () => {
  const x = await db.beginXTx(['a'], { idempotencyKey: 'key', idleTimeoutMs: 12, maxLifetimeMs: 34 });
  assert.equal(x.xtxId, 'x'); assert.equal(calls[0].idle_timeout_ms, 12);
  assert.equal(await db.stageXTx('x', { collection: 'a', op: 'insert', data: { n: 1 } }), '7');
  await db.getXTx('x', 'a', '7'); assert.equal((await db.commitXTx('x', { lockTimeoutMs: 9 })).txId, 'durable');
  assert.equal(calls[3].lock_timeout_ms, 9); assert.equal(await db.rollbackXTx('x'), true);
  assert.equal((await db.xTxStatus('key')).txId, 'durable');
  // Unknown commit result carries canonical google.rpc.Status/Any/ErrorInfo
  // in grpc-status-details-bin. Verify parsed metadata and status lookup flow.
  const v = (n: number) => { const a: number[] = []; do { let b = n & 127; n >>>= 7; if (n) b |= 128; a.push(b); } while (n); return Buffer.from(a); };
  const b = (n: number, value: Buffer) => Buffer.concat([v((n << 3) | 2), v(value.length), value]);
  const s = (n: number, value: string) => b(n, Buffer.from(value));
  const entry = Buffer.concat([s(1, 'status_ref'), s(2, 'idem-9')]);
  const info = Buffer.concat([s(1, 'XTX_OUTCOME_UNKNOWN'), s(2, 'scriva.xtx'), b(3, entry)]);
  const any = Buffer.concat([s(1, 'type.googleapis.com/google.rpc.ErrorInfo'), b(2, info)]);
  const statusBytes = Buffer.concat([v(8), v(2), b(3, any)]);
  let commits = 0;
  db.stub.CommitXTx = (_req: any, _md: any, cb: any) => {
    commits++;
    const md = new grpc.Metadata(); md.set('grpc-status-details-bin', statusBytes);
    cb(Object.assign(new Error('unknown'), { code: grpc.status.UNKNOWN, metadata: md }));
  };
  let caught: any;
  try { await db.commitXTx('x'); } catch (e) { caught = e; }
  assert.ok(caught instanceof XTxError); assert.equal(caught.reason, 'XTX_OUTCOME_UNKNOWN');
  assert.equal(caught.errorMetadata.status_ref, 'idem-9');
  await db.xTxStatus(caught.errorMetadata.status_ref);
  assert.equal(commits, 1); assert.equal(calls.at(-1).ref, 'idem-9');
  db.close();
})().catch((error) => { console.error(error); process.exitCode = 1; });
