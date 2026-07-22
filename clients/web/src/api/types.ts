// ---- Enums (string values as sent by grpc-gateway) -------------------------

export type FilterOp =
  | 'EQ' | 'NEQ' | 'GT' | 'GTE' | 'LT' | 'LTE' | 'CONTAINS' | 'REGEX'

export type WatchOp = 'INSERTED' | 'UPDATED' | 'DELETED' | 'OVERFLOW'

// ---- Filter types -----------------------------------------------------------

export interface FieldFilter {
  field: string
  op: FilterOp
  value: string  // JSON-encoded comparison value
}

export interface AndFilter {
  filters: Filter[]
}

export interface OrFilter {
  filters: Filter[]
}

export type Filter =
  | { field: FieldFilter }
  | { and: AndFilter }
  | { or: OrFilter }

// ---- Records ----------------------------------------------------------------

/** Data payload: arbitrary JSON object */
export type RecordData = Record<string, unknown>

export interface ScrivaDBRecord {
  id: string           // uint64 serialised as string by grpc-gateway
  data: RecordData
  date_added: string   // ISO-8601 timestamp
  date_modified: string
}

// ---- Find request -----------------------------------------------------------

export interface FindRequest {
  filter?: Filter
  limit?: number
  offset?: number
  order_by?: string
  descending?: boolean
}

// ---- Stats ------------------------------------------------------------------

export interface CollectionStats {
  collection: string
  record_count: string    // uint64 → string
  segment_count: string
  dirty_entries: string
  size_bytes: string
}

// ---- Watch ------------------------------------------------------------------

export interface WatchEvent {
  op: WatchOp
  collection: string
  record: ScrivaDBRecord
  ts: string  // ISO-8601 timestamp
}

// ---- API errors -------------------------------------------------------------

export class ScrivaDBError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message)
    this.name = 'ScrivaDBError'
  }
}
