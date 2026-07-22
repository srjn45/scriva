import { useCallback, useState } from 'react'
import type { ScrivaDBRecord, FindRequest } from '../api/types'
import { useApp } from '../contexts/AppContext'

const DEFAULT_LIMIT = 20

export function useRecords(collection: string) {
  const { client } = useApp()
  const [records, setRecords] = useState<ScrivaDBRecord[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [lastReq, setLastReq] = useState<FindRequest>({ limit: DEFAULT_LIMIT, offset: 0 })
  const [hasMore, setHasMore] = useState(false)

  const fetch = useCallback(
    async (req: FindRequest) => {
      setLoading(true)
      setError(null)
      setLastReq(req)
      try {
        const results = await client.find(collection, req)
        setRecords(results)
        setHasMore(results.length === (req.limit ?? DEFAULT_LIMIT))
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to fetch records')
      } finally {
        setLoading(false)
      }
    },
    [client, collection],
  )

  const nextPage = useCallback(() => {
    void fetch({
      ...lastReq,
      offset: (lastReq.offset ?? 0) + (lastReq.limit ?? DEFAULT_LIMIT),
    })
  }, [fetch, lastReq])

  const prevPage = useCallback(() => {
    const offset = Math.max(0, (lastReq.offset ?? 0) - (lastReq.limit ?? DEFAULT_LIMIT))
    void fetch({ ...lastReq, offset })
  }, [fetch, lastReq])

  return { records, loading, error, fetch, nextPage, prevPage, lastReq, hasMore }
}
