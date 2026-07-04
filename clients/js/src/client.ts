import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as fs from 'fs';
import * as path from 'path';
import {
  AggregateOptions,
  AggregateResult,
  AggregationOp,
  CasResult,
  DBRecord,
  FieldFilterInput,
  FilterInput,
  FindOptions,
  FindPage,
  GetOptions,
  StatsResult,
  WatchEvent,
  WriteResult,
} from './types';

// ---------------------------------------------------------------------------
// Proto loading — done once at module load time
// ---------------------------------------------------------------------------

const PROTO_PATH = path.join(__dirname, '..', 'proto', 'filedb.proto');
const INCLUDE_DIR = path.join(__dirname, '..', 'proto');

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
  includeDirs: [INCLUDE_DIR],
});

const protoDescriptor = grpc.loadPackageDefinition(packageDefinition) as any;
const FileDBStub = protoDescriptor.filedb.v1.FileDB as grpc.ServiceClientConstructor;

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/** Wrap a unary gRPC callback method into a Promise. */
function callUnary<Req, Res>(
  fn: (
    req: Req,
    metadata: grpc.Metadata,
    callback: (err: grpc.ServiceError | null, res: Res) => void,
  ) => void,
  req: Req,
  metadata: grpc.Metadata,
): Promise<Res> {
  return new Promise((resolve, reject) => {
    fn(req, metadata, (err, res) => {
      if (err) return reject(err);
      resolve(res as Res);
    });
  });
}

const OP_MAP: Record<string, string> = {
  eq: 'EQ',
  neq: 'NEQ',
  gt: 'GT',
  gte: 'GTE',
  lt: 'LT',
  lte: 'LTE',
  contains: 'CONTAINS',
  regex: 'REGEX',
};

const AGG_OP_MAP: Record<AggregationOp, string> = {
  count: 'AGG_COUNT',
  sum: 'AGG_SUM',
  avg: 'AGG_AVG',
  min: 'AGG_MIN',
  max: 'AGG_MAX',
};

/** True when a rejected gRPC call carries the given status code. */
function hasGrpcStatus(err: unknown, code: grpc.status): boolean {
  return (
    typeof err === 'object' && err !== null && (err as grpc.ServiceError).code === code
  );
}

/** Convert a FilterInput plain object to the proto Filter wire format. */
function filterToProto(filter: FilterInput): object {
  if ('and' in filter) {
    return { kind: 'and', and: { filters: filter.and.map(filterToProto) } };
  }
  if ('or' in filter) {
    return { kind: 'or', or: { filters: filter.or.map(filterToProto) } };
  }
  const f = filter as FieldFilterInput;
  return {
    kind: 'field',
    field: {
      field: f.field,
      op: OP_MAP[f.op] ?? 'FILTER_OP_UNSPECIFIED',
      value: String(f.value),
    },
  };
}

/** Build the wire FindRequest from FindOptions (N2 projection + N3 sort/paging). */
function buildFindRequest(collection: string, opts: FindOptions): object {
  return {
    collection,
    filter: opts.filter ? filterToProto(opts.filter) : undefined,
    limit: opts.limit ?? 0,
    offset: opts.offset ?? 0,
    // Deprecated scalar sort — honoured only when order_by_fields is empty.
    order_by: opts.orderByField ?? '',
    descending: opts.descending ?? false,
    order_by_fields: (opts.orderBy ?? []).map((o) => ({
      field: o.field,
      desc: o.desc ?? false,
    })),
    fields: opts.fields ?? [],
    page_token: opts.pageToken ?? '',
  };
}

// ---------------------------------------------------------------------------
// google.protobuf.Struct <-> plain object
//
// @grpc/proto-loader does not apply the well-known-type JSON mapping for
// google.protobuf.Struct, so a document must be converted to/from the explicit
// Value wire form (`{ fields: { key: { stringValue: ... } } }`) by hand.
// ---------------------------------------------------------------------------

/** Encode a plain JS value as a google.protobuf.Value wire object. */
function valueToProto(v: unknown): object {
  if (v === null || v === undefined) return { nullValue: 'NULL_VALUE' };
  switch (typeof v) {
    case 'number':
      return { numberValue: v };
    case 'string':
      return { stringValue: v };
    case 'boolean':
      return { boolValue: v };
  }
  if (Array.isArray(v)) {
    return { listValue: { values: v.map(valueToProto) } };
  }
  if (typeof v === 'object') {
    return { structValue: structToProto(v as Record<string, unknown>) };
  }
  return { nullValue: 'NULL_VALUE' };
}

/** Encode a plain object as a google.protobuf.Struct wire object. */
function structToProto(obj: Record<string, unknown>): object {
  const fields: Record<string, object> = {};
  for (const [k, val] of Object.entries(obj)) {
    fields[k] = valueToProto(val);
  }
  return { fields };
}

/** Decode a google.protobuf.Value wire object back to a plain JS value. */
function valueFromProto(val: any): unknown {
  if (!val) return null;
  switch (val.kind) {
    case 'numberValue':
      return val.numberValue;
    case 'stringValue':
      return val.stringValue;
    case 'boolValue':
      return val.boolValue;
    case 'structValue':
      return structFromProto(val.structValue);
    case 'listValue':
      return ((val.listValue?.values as unknown[]) ?? []).map(valueFromProto);
    case 'nullValue':
    default:
      return null;
  }
}

/** Decode a google.protobuf.Struct wire object back to a plain object. */
function structFromProto(s: any): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  const fields = (s?.fields as Record<string, any>) ?? {};
  for (const [k, v] of Object.entries(fields)) {
    out[k] = valueFromProto(v);
  }
  return out;
}

/**
 * Format a google.protobuf.Timestamp wire object (`{ seconds, nanos }`) as an
 * RFC 3339 / ISO-8601 string with nanosecond precision, matching the other
 * SDKs. Returns `undefined` when the timestamp is unset.
 */
function timestampToIso(ts: any): string | undefined {
  if (!ts || ts.seconds === undefined || ts.seconds === null) return undefined;
  const seconds = Number(ts.seconds);
  const nanos = Number(ts.nanos ?? 0);
  if (seconds === 0 && nanos === 0) return undefined;
  const base = new Date(seconds * 1000).toISOString().replace(/\.\d+Z$/, '');
  return `${base}.${String(nanos).padStart(9, '0')}Z`;
}

/** Convert a raw proto Record object to a typed DBRecord. */
function toRecord(raw: any): DBRecord {
  return {
    id: String(raw.id),
    key: (raw.key as string) ?? '',
    rev: String(raw.rev ?? '0'),
    data: raw.data ? structFromProto(raw.data) : {},
    date_added: timestampToIso(raw.date_added),
    date_modified: timestampToIso(raw.date_modified),
  };
}

// ---------------------------------------------------------------------------
// FileDB client
// ---------------------------------------------------------------------------

/**
 * TypeScript/JavaScript client for FileDB v2.
 *
 * @example
 * ```ts
 * const db = new FileDB('localhost', 5433, 'dev-key');
 *
 * await db.createCollection('users');
 * const id = await db.insert('users', { name: 'Alice', age: 30 });
 * const record = await db.findById('users', id);
 * await db.update('users', id, { name: 'Alice', age: 31 });
 * await db.delete('users', id);
 * await db.dropCollection('users');
 * db.close();
 * ```
 */
export class FileDB {
  private readonly stub: grpc.Client & Record<string, Function>;
  private readonly apiKey: string;

  /**
   * Connect to a FileDB server.
   *
   * @param host       gRPC host (e.g. `'localhost'`)
   * @param port       gRPC port (default `5433`)
   * @param apiKey     API key — sent as `x-api-key` metadata on every call
   * @param tlsCaCert  Optional PEM buffer for TLS CA certificate verification.
   *                   Omit for a plaintext (insecure) connection.
   */
  constructor(host: string, port: number, apiKey: string, tlsCaCert?: Buffer) {
    this.apiKey = apiKey;
    const credentials = tlsCaCert
      ? grpc.credentials.createSsl(tlsCaCert)
      : grpc.credentials.createInsecure();
    this.stub = new FileDBStub(`${host}:${port}`, credentials) as any;
  }

  /**
   * Connect using a TLS CA certificate loaded from a file path.
   *
   * @param host           gRPC host
   * @param port           gRPC port
   * @param apiKey         API key
   * @param tlsCaCertPath  Path to PEM CA certificate file
   */
  static fromTlsCertPath(
    host: string,
    port: number,
    apiKey: string,
    tlsCaCertPath: string,
  ): FileDB {
    return new FileDB(host, port, apiKey, fs.readFileSync(tlsCaCertPath));
  }

  private meta(): grpc.Metadata {
    const md = new grpc.Metadata();
    md.add('x-api-key', this.apiKey);
    return md;
  }

  // -------------------------------------------------------------------------
  // Collection management
  // -------------------------------------------------------------------------

  /**
   * Create a new collection. Returns the collection name.
   *
   * @param name              Collection name.
   * @param defaultTtlSeconds Optional per-collection default record TTL. When
   *                          greater than 0, records inserted without their own
   *                          TTL expire after this many seconds. Persisted and
   *                          overrides the server-wide default; 0 (the default)
   *                          inherits the server-wide TTL.
   */
  async createCollection(name: string, defaultTtlSeconds = 0): Promise<string> {
    const resp: any = await callUnary(
      this.stub.CreateCollection.bind(this.stub),
      { name, default_ttl_seconds: defaultTtlSeconds },
      this.meta(),
    );
    return resp.name as string;
  }

  /** Drop a collection and all its data. Returns `true` on success. */
  async dropCollection(name: string): Promise<boolean> {
    const resp: any = await callUnary(
      this.stub.DropCollection.bind(this.stub),
      { name },
      this.meta(),
    );
    return resp.ok as boolean;
  }

  /** List all collection names. */
  async listCollections(): Promise<string[]> {
    const resp: any = await callUnary(
      this.stub.ListCollections.bind(this.stub),
      {},
      this.meta(),
    );
    return (resp.names as string[]) ?? [];
  }

  // -------------------------------------------------------------------------
  // CRUD
  // -------------------------------------------------------------------------

  /**
   * Insert one record. Returns the assigned ID as a string.
   *
   * @param ttlSeconds Optional per-record TTL. When greater than 0 the record
   *                   expires this many seconds after insertion, overriding any
   *                   collection default; 0 (the default) applies the
   *                   collection's default TTL, if any.
   * @param key        Optional caller-supplied string primary key (keyed create,
   *                   N1). When set, a key already held by a live record is
   *                   rejected with a gRPC `ALREADY_EXISTS` error; use
   *                   {@link upsert} for insert-or-replace semantics. A keyed
   *                   insert does not participate in transactions or per-record
   *                   TTL. Empty (the default) keeps the keyless behaviour.
   */
  async insert(
    collection: string,
    data: Record<string, unknown>,
    ttlSeconds = 0,
    key = '',
  ): Promise<string> {
    const resp: any = await callUnary(
      this.stub.Insert.bind(this.stub),
      { collection, data: structToProto(data), ttl_seconds: ttlSeconds, key },
      this.meta(),
    );
    return String(resp.id);
  }

  /**
   * Insert multiple records. Returns an array of assigned IDs in insertion order.
   *
   * @param ttlSeconds Optional per-record TTL applied to every record in the
   *                   batch; see {@link insert} for the semantics.
   */
  async insertMany(
    collection: string,
    records: Array<Record<string, unknown>>,
    ttlSeconds = 0,
  ): Promise<string[]> {
    const resp: any = await callUnary(
      this.stub.InsertMany.bind(this.stub),
      { collection, records: records.map(structToProto), ttl_seconds: ttlSeconds },
      this.meta(),
    );
    return ((resp.ids as unknown[]) ?? []).map(String);
  }

  /**
   * Fetch a single record by ID.
   *
   * @param opts Optional field projection (N2): `{ fields }` limits the returned
   *             `data` to those top-level fields (`id`, `key` and `rev` are
   *             always included).
   */
  async findById(
    collection: string,
    id: string | number,
    opts: GetOptions = {},
  ): Promise<DBRecord> {
    const resp: any = await callUnary(
      this.stub.FindById.bind(this.stub),
      { collection, id: String(id), fields: opts.fields ?? [] },
      this.meta(),
    );
    return toRecord(resp.record);
  }

  /**
   * Stream records matching the given options.
   * Returns an `AsyncGenerator` — use `for await` to iterate.
   *
   * @example
   * ```ts
   * for await (const record of db.find('users', { filter: { field: 'role', op: 'eq', value: 'admin' } })) {
   *   console.log(record);
   * }
   * ```
   */
  async *find(collection: string, opts: FindOptions = {}): AsyncGenerator<DBRecord> {
    const call = this.stub.Find(
      buildFindRequest(collection, opts),
      this.meta(),
    ) as grpc.ClientReadableStream<any>;

    for await (const resp of call) {
      yield toRecord(resp.record);
    }
  }

  /**
   * Collect all results from `find` into an array.
   * Convenience wrapper for non-streaming use cases.
   */
  async findAll(collection: string, opts: FindOptions = {}): Promise<DBRecord[]> {
    const results: DBRecord[] = [];
    for await (const record of this.find(collection, opts)) {
      results.push(record);
    }
    return results;
  }

  /**
   * Fetch one page of results together with the keyset cursor for the next page
   * (N3). Pass an ordering (`orderBy`) and a `limit`, then feed the returned
   * `pageToken` back as `opts.pageToken` on the next call; keep the ordering,
   * filter and limit identical across pages. A returned `pageToken` of `''`
   * means the last page was reached.
   *
   * @example
   * ```ts
   * let token = '';
   * do {
   *   const page = await db.findPage('users', { orderBy: [{ field: 'age' }], limit: 100, pageToken: token });
   *   for (const r of page.records) console.log(r.id);
   *   token = page.pageToken;
   * } while (token);
   * ```
   */
  async findPage(collection: string, opts: FindOptions = {}): Promise<FindPage> {
    const call = this.stub.Find(
      buildFindRequest(collection, opts),
      this.meta(),
    ) as grpc.ClientReadableStream<any>;

    const records: DBRecord[] = [];
    let pageToken = '';
    for await (const resp of call) {
      if (resp.record) records.push(toRecord(resp.record));
      if (resp.page_token) pageToken = resp.page_token as string;
    }
    return { records, pageToken };
  }

  /**
   * Update a record by ID. Returns the updated ID.
   *
   * @param ttlSeconds Optional TTL. When greater than 0 it resets the record's
   *                   expiry to this many seconds from now, overriding the
   *                   collection default. 0 (the default) leaves any existing
   *                   deadline in place — a plain update is sticky and does not
   *                   clear a previously set TTL.
   */
  async update(
    collection: string,
    id: string | number,
    data: Record<string, unknown>,
    ttlSeconds = 0,
  ): Promise<string> {
    const resp: any = await callUnary(
      this.stub.Update.bind(this.stub),
      { collection, id: String(id), data: structToProto(data), ttl_seconds: ttlSeconds },
      this.meta(),
    );
    return String(resp.id);
  }

  /** Delete a record by ID. Returns `true` if the record existed. */
  async delete(collection: string, id: string | number): Promise<boolean> {
    const resp: any = await callUnary(
      this.stub.Delete.bind(this.stub),
      { collection, id: String(id) },
      this.meta(),
    );
    return resp.ok as boolean;
  }

  // -------------------------------------------------------------------------
  // Keyed CRUD, Upsert & compare-and-swap (N1)
  // -------------------------------------------------------------------------

  /**
   * Insert-or-replace by string key (N1). Inserts `data` under `key` if no live
   * record carries it, otherwise atomically replaces the existing record's data.
   * Returns the resulting record — its `rev` is `'1'` on a fresh insert and is
   * incremented on each replace.
   */
  async upsert(
    collection: string,
    key: string,
    data: Record<string, unknown>,
  ): Promise<DBRecord> {
    const resp: any = await callUnary(
      this.stub.Upsert.bind(this.stub),
      { collection, key, data: structToProto(data) },
      this.meta(),
    );
    return toRecord(resp.record);
  }

  /**
   * Fetch the record carrying the caller-supplied string `key` (N1). Resolves to
   * `null` when no live record holds the key (the server's `NOT_FOUND` is mapped
   * to a null result rather than a thrown error).
   *
   * @param opts Optional field projection (N2): `{ fields }` limits the returned
   *             `data` (`id`, `key` and `rev` are always included).
   */
  async findByKey(
    collection: string,
    key: string,
    opts: GetOptions = {},
  ): Promise<DBRecord | null> {
    try {
      const resp: any = await callUnary(
        this.stub.FindByKey.bind(this.stub),
        { collection, key, fields: opts.fields ?? [] },
        this.meta(),
      );
      return toRecord(resp.record);
    } catch (err) {
      if (hasGrpcStatus(err, grpc.status.NOT_FOUND)) return null;
      throw err;
    }
  }

  /**
   * Overwrite the record carrying `key`, preserving the key itself (N1). Returns
   * the write acknowledgement including the record's revision after the write.
   * Rejects with a gRPC `NOT_FOUND` error when no live record holds the key.
   */
  async updateByKey(
    collection: string,
    key: string,
    data: Record<string, unknown>,
  ): Promise<WriteResult> {
    const resp: any = await callUnary(
      this.stub.UpdateByKey.bind(this.stub),
      { collection, key, data: structToProto(data) },
      this.meta(),
    );
    return {
      id: String(resp.id),
      key: (resp.key as string) ?? '',
      rev: String(resp.rev ?? '0'),
      date_modified: resp.date_modified || undefined,
    };
  }

  /**
   * Delete the record carrying `key` (N1). Returns `true` if a record was
   * removed, `false` when no live record held the key (the server's `NOT_FOUND`
   * is mapped to `false` rather than a thrown error).
   */
  async deleteByKey(collection: string, key: string): Promise<boolean> {
    try {
      const resp: any = await callUnary(
        this.stub.DeleteByKey.bind(this.stub),
        { collection, key },
        this.meta(),
      );
      return resp.ok as boolean;
    } catch (err) {
      if (hasGrpcStatus(err, grpc.status.NOT_FOUND)) return false;
      throw err;
    }
  }

  /**
   * Compare-and-swap update keyed on the record's revision (N1). The write is
   * applied only if the record carrying `key` currently has `expectedRev`. A
   * stale revision — or a missing key — is a clean no-op reported as
   * `{ swapped: false, record: null }`, never an error.
   *
   * @example
   * ```ts
   * const rec = await db.findByKey('cfg', 'flags');
   * const res = await db.updateIfRev('cfg', 'flags', rec!.rev, { enabled: true });
   * if (!res.swapped) console.log('lost the race — retry');
   * ```
   */
  async updateIfRev(
    collection: string,
    key: string,
    expectedRev: string | number,
    data: Record<string, unknown>,
  ): Promise<CasResult> {
    const resp: any = await callUnary(
      this.stub.UpdateIfRev.bind(this.stub),
      {
        collection,
        key,
        expected_rev: String(expectedRev),
        data: structToProto(data),
      },
      this.meta(),
    );
    return {
      swapped: Boolean(resp.swapped),
      record: resp.swapped && resp.record ? toRecord(resp.record) : null,
    };
  }

  // -------------------------------------------------------------------------
  // Secondary indexes
  // -------------------------------------------------------------------------

  /** Create a secondary index on a field (no-op if already exists). */
  async ensureIndex(collection: string, field: string): Promise<void> {
    await callUnary(
      this.stub.EnsureIndex.bind(this.stub),
      { collection, field },
      this.meta(),
    );
  }

  /** Drop a secondary index. Returns `true` if the index existed. */
  async dropIndex(collection: string, field: string): Promise<boolean> {
    const resp: any = await callUnary(
      this.stub.DropIndex.bind(this.stub),
      { collection, field },
      this.meta(),
    );
    return resp.ok as boolean;
  }

  /** List all indexed field names for a collection. */
  async listIndexes(collection: string): Promise<string[]> {
    const resp: any = await callUnary(
      this.stub.ListIndexes.bind(this.stub),
      { collection },
      this.meta(),
    );
    return (resp.fields as string[]) ?? [];
  }

  // -------------------------------------------------------------------------
  // Transactions
  // -------------------------------------------------------------------------

  /** Begin a transaction on a collection. Returns the transaction ID. */
  async beginTx(collection: string): Promise<string> {
    const resp: any = await callUnary(
      this.stub.BeginTx.bind(this.stub),
      { collection },
      this.meta(),
    );
    return resp.tx_id as string;
  }

  /** Commit a transaction. Returns `true` on success. */
  async commitTx(txId: string): Promise<boolean> {
    const resp: any = await callUnary(
      this.stub.CommitTx.bind(this.stub),
      { tx_id: txId },
      this.meta(),
    );
    return resp.ok as boolean;
  }

  /** Roll back a transaction. Returns `true` on success. */
  async rollbackTx(txId: string): Promise<boolean> {
    const resp: any = await callUnary(
      this.stub.RollbackTx.bind(this.stub),
      { tx_id: txId },
      this.meta(),
    );
    return resp.ok as boolean;
  }

  // -------------------------------------------------------------------------
  // Watch (server-streaming change feed)
  // -------------------------------------------------------------------------

  /**
   * Subscribe to change events on a collection.
   * Returns an `AsyncGenerator` — use `for await` to receive events.
   *
   * @example
   * ```ts
   * for await (const event of db.watch('users')) {
   *   console.log(event.op, event.record.id, event.record.data);
   * }
   * ```
   *
   * Break out of the loop to stop watching.
   */
  async *watch(collection: string, filter?: FilterInput): AsyncGenerator<WatchEvent> {
    const call = this.stub.Watch(
      {
        collection,
        filter: filter ? filterToProto(filter) : undefined,
      },
      this.meta(),
    ) as grpc.ClientReadableStream<any>;

    for await (const event of call) {
      yield {
        op: event.op as WatchEvent['op'],
        collection: event.collection as string,
        record: toRecord(event.record),
        ts: timestampToIso(event.ts),
      };
    }
  }

  // -------------------------------------------------------------------------
  // Aggregations (N4)
  // -------------------------------------------------------------------------

  /**
   * Compute `count` and optional numeric aggregations (`sum`/`avg`/`min`/`max`)
   * over the live records matching `opts.filter`, optionally grouped by a field
   * (N4). Returns one {@link AggregateResult} per distinct group value (ascending
   * group order); an ungrouped aggregation returns a single result whose `group`
   * is `null`. `count` is always populated; the numeric aggregates are only
   * meaningful when `numeric` is true.
   *
   * @example
   * ```ts
   * const groups = await db.aggregate('orders', {
   *   groupBy: 'status',
   *   field: 'total',
   *   aggregations: ['sum', 'avg'],
   * });
   * for (const g of groups) console.log(g.group, g.count, g.sum);
   * ```
   */
  async aggregate(
    collection: string,
    opts: AggregateOptions = {},
  ): Promise<AggregateResult[]> {
    const call = this.stub.Aggregate(
      {
        collection,
        filter: opts.filter ? filterToProto(opts.filter) : undefined,
        group_by: opts.groupBy ?? '',
        field: opts.field ?? '',
        aggregations: (opts.aggregations ?? []).map((a) => AGG_OP_MAP[a]),
      },
      this.meta(),
    ) as grpc.ClientReadableStream<any>;

    const results: AggregateResult[] = [];
    for await (const resp of call) {
      const numeric = Boolean(resp.numeric);
      const out: AggregateResult = {
        group: valueFromProto(resp.group_value),
        count: String(resp.count ?? '0'),
        numeric,
      };
      if (numeric) {
        out.sum = Number(resp.sum);
        out.avg = Number(resp.avg);
        out.min = Number(resp.min);
        out.max = Number(resp.max);
      }
      results.push(out);
    }
    return results;
  }

  /**
   * Convenience: count the live records matching an optional filter (N4).
   * Returns the count as a number.
   */
  async count(collection: string, filter?: FilterInput): Promise<number> {
    const [res] = await this.aggregate(collection, { filter });
    return res ? Number(res.count) : 0;
  }

  /**
   * Convenience: group records by `field` and aggregate each group (N4).
   * Equivalent to `aggregate(collection, { groupBy: field, ...opts })`.
   */
  async groupBy(
    collection: string,
    field: string,
    opts: Omit<AggregateOptions, 'groupBy'> = {},
  ): Promise<AggregateResult[]> {
    return this.aggregate(collection, { ...opts, groupBy: field });
  }

  // -------------------------------------------------------------------------
  // Stats
  // -------------------------------------------------------------------------

  /** Return collection statistics. */
  async stats(collection: string): Promise<StatsResult> {
    const resp: any = await callUnary(
      this.stub.CollectionStats.bind(this.stub),
      { collection },
      this.meta(),
    );
    return {
      collection: resp.collection as string,
      record_count: String(resp.record_count),
      segment_count: String(resp.segment_count),
      dirty_entries: String(resp.dirty_entries),
      size_bytes: String(resp.size_bytes),
    };
  }

  // -------------------------------------------------------------------------
  // Maintenance
  // -------------------------------------------------------------------------

  /**
   * Force a synchronous compaction pass on a collection. Merges and
   * deduplicates sealed segments and reclaims space from deleted or expired
   * records, resolving once the pass completes. Returns `true` on success.
   */
  async compact(collection: string): Promise<boolean> {
    const resp: any = await callUnary(
      this.stub.Compact.bind(this.stub),
      { collection },
      this.meta(),
    );
    return resp.ok as boolean;
  }

  // -------------------------------------------------------------------------
  // Backup (server-streaming)
  // -------------------------------------------------------------------------

  /**
   * Stream a consistent gzip backup of the entire database.
   * Returns an `AsyncGenerator` yielding the raw gzip archive bytes chunk by
   * chunk, so large databases never have to be held in memory. Concatenate the
   * chunks (or use {@link snapshotToFile}) to reconstruct the archive, then
   * restore it with `tar xzf` into a data directory.
   */
  async *snapshot(): AsyncGenerator<Buffer> {
    const call = this.stub.Snapshot({}, this.meta()) as grpc.ClientReadableStream<any>;
    for await (const chunk of call) {
      yield chunk.data as Buffer;
    }
  }

  /**
   * Stream a gzip backup straight to a file. Resolves with the number of bytes
   * written. Convenience wrapper over {@link snapshot}; restore with
   * `tar xzf <path>`.
   */
  async snapshotToFile(filePath: string): Promise<number> {
    const out = fs.createWriteStream(filePath);
    let written = 0;
    try {
      for await (const chunk of this.snapshot()) {
        written += chunk.length;
        if (!out.write(chunk)) {
          await new Promise<void>((resolve) => out.once('drain', resolve));
        }
      }
    } finally {
      await new Promise<void>((resolve, reject) => {
        out.end((err?: Error | null) => (err ? reject(err) : resolve()));
      });
    }
    return written;
  }

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  /** Close the underlying gRPC channel. */
  close(): void {
    this.stub.close();
  }
}
