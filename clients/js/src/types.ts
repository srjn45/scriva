/** A record returned from FileDB. `id` is a string because uint64 exceeds JS's safe integer range. */
export interface DBRecord {
  id: string;
  /** Caller-supplied string primary key. Empty string for records inserted without one. */
  key: string;
  /** Monotonic per-record revision (as a string). Fresh records start at `'1'`, bumped on every write. */
  rev: string;
  data: Record<string, unknown>;
  date_added?: string;
  date_modified?: string;
}

/** An event emitted by the Watch RPC. */
export interface WatchEvent {
  op: 'INSERTED' | 'UPDATED' | 'DELETED' | 'OVERFLOW';
  collection: string;
  record: DBRecord;
  ts?: string;
}

/** A filter on a single field. */
export interface FieldFilterInput {
  field: string;
  op: 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte' | 'contains' | 'regex';
  value: string | number | boolean;
}

/** AND composite — all child filters must match. */
export interface AndFilterInput {
  and: FilterInput[];
}

/** OR composite — at least one child filter must match. */
export interface OrFilterInput {
  or: FilterInput[];
}

/** Union of all filter shapes accepted by `find` and `watch`. */
export type FilterInput = FieldFilterInput | AndFilterInput | OrFilterInput;

/** One sort key and its direction (N3 multi-field ordering). */
export interface OrderByInput {
  /** Field name to sort by. */
  field: string;
  /** Sort descending when true (default false = ascending). */
  desc?: boolean;
}

/** Options for the `find` / `findAll` / `findPage` methods. */
export interface FindOptions {
  filter?: FilterInput;
  /** Maximum number of results to return. 0 = no limit (default). */
  limit?: number;
  /** Number of leading results to skip. Prefer keyset paging (`pageToken`) for large offsets. */
  offset?: number;
  /**
   * Multi-field, per-field-directional sort (N3). When set it supersedes the
   * deprecated scalar `orderBy` / `descending`.
   */
  orderBy?: OrderByInput[];
  /**
   * @deprecated Superseded by `orderBy: OrderByInput[]`. A single field name to
   * sort by; honoured only when `orderBy` is empty.
   */
  orderByField?: string;
  /** @deprecated Sort descending when true. Pairs with the deprecated `orderByField`. */
  descending?: boolean;
  /**
   * Field projection (N2): when non-empty, only these top-level fields are
   * returned in each record's `data`. `id`, `key` and `rev` are always included.
   * Empty (the default) returns full records.
   */
  fields?: string[];
  /**
   * Opaque keyset (cursor) pagination token (N3). Empty (the default) requests
   * the first page; otherwise it must be a `pageToken` returned by a previous
   * `findPage`. Only meaningful together with an ordering.
   */
  pageToken?: string;
}

/** Options controlling field projection on single-record reads (`findById` / `findByKey`). */
export interface GetOptions {
  /**
   * Field projection (N2): when non-empty, only these top-level fields are
   * returned in the record's `data`. `id`, `key` and `rev` are always included.
   */
  fields?: string[];
}

/** One page of results returned by `findPage`, carrying the keyset cursor for the next page. */
export interface FindPage {
  records: DBRecord[];
  /** Opaque cursor for the next page. Empty string means the last page was reached. */
  pageToken: string;
}

/** Result of a keyed write acknowledgement (`updateByKey`), surfacing the new revision. */
export interface WriteResult {
  id: string;
  key: string;
  rev: string;
  date_modified?: string;
}

/** Result of a compare-and-swap update (`updateIfRev`). */
export interface CasResult {
  /** True when `expectedRev` matched and the update applied; false (never an error) when stale or missing. */
  swapped: boolean;
  /** The resulting record when `swapped` is true; `null` otherwise. */
  record: DBRecord | null;
}

/** A numeric aggregation to compute per group. `count` is always returned and need not be listed. */
export type AggregationOp = 'count' | 'sum' | 'avg' | 'min' | 'max';

/** Options for the `aggregate` / `groupBy` methods (N4). */
export interface AggregateOptions {
  /** Same composable filter as `find`; only matching live records contribute. */
  filter?: FilterInput;
  /** Group-by field. When set, one result is returned per distinct value; unset aggregates the whole set. */
  groupBy?: string;
  /** Numeric field for `sum`/`avg`/`min`/`max`. Required when any of those is requested. */
  field?: string;
  /** Which aggregations to compute. `count` is always returned; an empty list yields count-only. */
  aggregations?: AggregationOp[];
}

/** One group's aggregation result (N4). */
export interface AggregateResult {
  /**
   * The group-by field's value for this group, type-preserved. `null` for the
   * whole-set (ungrouped) result and for records missing the group field.
   */
  group: unknown;
  /** Number of records in this group (post-filter), as a string (uint64). */
  count: string;
  /** True when at least one record carried a numeric `field`; the numeric aggregates are meaningful only then. */
  numeric: boolean;
  /** Sum of numeric `field` values across the group. Present only when `numeric` is true. */
  sum?: number;
  /** Mean of numeric `field` values. Present only when `numeric` is true. */
  avg?: number;
  /** Smallest numeric `field` value. Present only when `numeric` is true. */
  min?: number;
  /** Largest numeric `field` value. Present only when `numeric` is true. */
  max?: number;
}

/** Collection statistics returned by `stats`. */
export interface StatsResult {
  collection: string;
  record_count: string;
  segment_count: string;
  dirty_entries: string;
  size_bytes: string;
}
