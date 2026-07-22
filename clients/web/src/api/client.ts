import type {
  CollectionStats,
  ScrivaDBRecord,
  FindRequest,
  RecordData,
  WatchEvent,
} from './types'
import { ScrivaDBError } from './types'

export class ScrivaDBClient {
  constructor(
    private readonly baseUrl: string,
    private readonly apiKey: string,
  ) {}

  // ---- helpers --------------------------------------------------------------

  private headers(): Record<string, string> {
    const h: Record<string, string> = { 'Content-Type': 'application/json' }
    if (this.apiKey) h['x-api-key'] = this.apiKey
    return h
  }

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const res = await fetch(`${this.baseUrl}${path}`, {
      method,
      headers: this.headers(),
      body: body !== undefined ? JSON.stringify(body) : undefined,
    })
    if (!res.ok) {
      const text = await res.text().catch(() => res.statusText)
      throw new ScrivaDBError(res.status, text)
    }
    return res.json() as Promise<T>
  }

  // ---- Collections ----------------------------------------------------------

  async listCollections(): Promise<string[]> {
    const res = await this.request<{ names?: string[] }>('GET', '/v1/collections')
    return res.names ?? []
  }

  async createCollection(name: string): Promise<void> {
    await this.request('POST', '/v1/collections', { name })
  }

  async dropCollection(name: string): Promise<void> {
    await this.request('DELETE', `/v1/collections/${encodeURIComponent(name)}`)
  }

  // ---- Records --------------------------------------------------------------

  async insert(collection: string, data: RecordData): Promise<{ id: string }> {
    return this.request('POST', `/v1/${encodeURIComponent(collection)}/records`, { data })
  }

  async findById(collection: string, id: string): Promise<ScrivaDBRecord> {
    const res = await this.request<{ record: ScrivaDBRecord }>(
      'GET',
      `/v1/${encodeURIComponent(collection)}/records/${encodeURIComponent(id)}`,
    )
    return res.record
  }

  async find(collection: string, req: FindRequest): Promise<ScrivaDBRecord[]> {
    // grpc-gateway streams FindResponse one per line; collect all
    const res = await fetch(
      `${this.baseUrl}/v1/${encodeURIComponent(collection)}/records/find`,
      { method: 'POST', headers: this.headers(), body: JSON.stringify(req) },
    )
    if (!res.ok) {
      const text = await res.text().catch(() => res.statusText)
      throw new ScrivaDBError(res.status, text)
    }
    const text = await res.text()
    return text
      .split('\n')
      .filter(Boolean)
      .map((line) => {
        const envelope = JSON.parse(line) as
          | { result: { record: ScrivaDBRecord } }
          | { error: { message: string; code?: number } }
        if ('error' in envelope) {
          throw new ScrivaDBError(500, envelope.error.message)
        }
        return envelope.result.record
      })
  }

  async update(collection: string, id: string, data: RecordData): Promise<void> {
    await this.request(
      'PUT',
      `/v1/${encodeURIComponent(collection)}/records/${encodeURIComponent(id)}`,
      { data },
    )
  }

  async delete(collection: string, id: string): Promise<void> {
    await this.request(
      'DELETE',
      `/v1/${encodeURIComponent(collection)}/records/${encodeURIComponent(id)}`,
    )
  }

  // ---- Indexes --------------------------------------------------------------

  async listIndexes(collection: string): Promise<string[]> {
    const res = await this.request<{ fields?: string[] }>(
      'GET',
      `/v1/${encodeURIComponent(collection)}/indexes`,
    )
    return res.fields ?? []
  }

  async ensureIndex(collection: string, field: string): Promise<void> {
    await this.request('POST', `/v1/${encodeURIComponent(collection)}/indexes`, { field })
  }

  async dropIndex(collection: string, field: string): Promise<void> {
    await this.request(
      'DELETE',
      `/v1/${encodeURIComponent(collection)}/indexes/${encodeURIComponent(field)}`,
    )
  }

  // ---- Stats ----------------------------------------------------------------

  async collectionStats(collection: string): Promise<CollectionStats> {
    return this.request(
      'GET',
      `/v1/${encodeURIComponent(collection)}/stats`,
    )
  }

  // ---- Watch (streaming) ----------------------------------------------------

  watch(
    collection: string,
    onEvent: (e: WatchEvent) => void,
    onError?: (err: Error) => void,
  ): () => void {
    const controller = new AbortController()

    const run = async () => {
      try {
        const res = await fetch(
          `${this.baseUrl}/v1/${encodeURIComponent(collection)}/watch`,
          {
            method: 'POST',
            headers: this.headers(),
            body: JSON.stringify({}),
            signal: controller.signal,
          },
        )
        if (!res.ok || !res.body) {
          const text = await res.text().catch(() => res.statusText)
          onError?.(new ScrivaDBError(res.status, text))
          return
        }
        const reader = res.body.getReader()
        const decoder = new TextDecoder()
        let buf = ''
        while (true) {
          const { done, value } = await reader.read()
          if (done) break
          buf += decoder.decode(value, { stream: true })
          const lines = buf.split('\n')
          buf = lines.pop() ?? ''
          for (const line of lines) {
            if (!line.trim()) continue
            const envelope = JSON.parse(line) as
              | { result: WatchEvent }
              | { error: { message: string } }
            if ('error' in envelope) {
              onError?.(new Error(envelope.error.message))
              return
            }
            onEvent(envelope.result)
          }
        }
      } catch (err) {
        if (err instanceof Error && err.name !== 'AbortError') {
          onError?.(err)
        }
      }
    }

    run()
    return () => controller.abort()
  }
}
