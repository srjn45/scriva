import { useEffect } from 'react'
import type { ScrivaDBRecord, FindRequest } from '../api/types'
import { useApp } from '../contexts/AppContext'
import { useToast } from '../contexts/ToastContext'
import FilterBar from './FilterBar'
import { useRecords } from '../hooks/useRecords'

interface Props {
  collection: string
  onInsert: () => void
  onEdit: (record: ScrivaDBRecord) => void
}

/** Returns a sorted list of all unique keys across all records (excluding internal fields) */
function dataColumns(records: ScrivaDBRecord[]): string[] {
  const keys = new Set<string>()
  for (const r of records) {
    for (const k of Object.keys(r.data ?? {})) keys.add(k)
  }
  return Array.from(keys).sort()
}

/** Render a cell value: collapse objects/arrays to {…}/[…] */
function CellValue({ value }: { value: unknown }) {
  if (value === null || value === undefined) {
    return <span className="text-gray-600">—</span>
  }
  if (typeof value === 'object') {
    const label = Array.isArray(value) ? `[${(value as unknown[]).length}]` : '{…}'
    return (
      <span
        title={JSON.stringify(value, null, 2)}
        className="cursor-help text-gray-400 text-xs"
      >
        {label}
      </span>
    )
  }
  return <span>{String(value)}</span>
}

export default function BrowseTab({ collection, onInsert, onEdit }: Props) {
  const { client } = useApp()
  const { showToast } = useToast()
  const { records, loading, error, fetch, nextPage, prevPage, lastReq, hasMore } = useRecords(collection)

  // Load first page on mount
  useEffect(() => {
    void fetch({ limit: 20, offset: 0 })
  }, [fetch])

  const page = Math.floor((lastReq.offset ?? 0) / (lastReq.limit ?? 20))
  const dataCols = dataColumns(records)

  async function handleDelete(record: ScrivaDBRecord) {
    if (!window.confirm(`Delete record ${record.id}?`)) return
    try {
      await client.delete(collection, record.id)
      showToast('success', `Record ${record.id} deleted`)
      void fetch({ ...lastReq, offset: lastReq.offset ?? 0 })
    } catch (err) {
      showToast('error', err instanceof Error ? err.message : 'Delete failed')
    }
  }

  return (
    <div className="flex flex-col gap-4 h-full">
      {/* Filter bar */}
      <div className="shrink-0">
        <FilterBar onQuery={(req: FindRequest) => void fetch(req)} />
      </div>

      {/* Toolbar */}
      <div className="flex items-center gap-2 shrink-0">
        <button
          onClick={onInsert}
          className="rounded bg-blue-600 hover:bg-blue-500 px-3 py-1.5 text-sm font-medium text-white transition-colors"
        >
          + Insert
        </button>
        {loading && <span className="text-xs text-gray-500">Loading…</span>}
        {error && <span className="text-xs text-red-400">{error}</span>}
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto">
        <table className="min-w-full text-sm border-collapse">
          <thead>
            <tr className="border-b border-gray-700 text-left text-xs text-gray-500 uppercase">
              <th className="px-3 py-2 whitespace-nowrap">ID</th>
              {dataCols.map((col) => (
                <th key={col} className="px-3 py-2 whitespace-nowrap">{col}</th>
              ))}
              <th className="px-3 py-2 whitespace-nowrap">created</th>
              <th className="px-3 py-2 whitespace-nowrap">modified</th>
              <th className="px-3 py-2" />
            </tr>
          </thead>
          <tbody>
            {records.length === 0 && !loading && (
              <tr>
                <td
                  colSpan={dataCols.length + 4}
                  className="px-3 py-8 text-center text-gray-600"
                >
                  No records
                </td>
              </tr>
            )}
            {records.map((record) => (
              <tr
                key={record.id}
                className="border-b border-gray-800 hover:bg-gray-800/50 transition-colors"
              >
                <td className="px-3 py-2 font-mono text-xs text-gray-400 whitespace-nowrap">
                  {record.id}
                </td>
                {dataCols.map((col) => (
                  <td key={col} className="px-3 py-2 max-w-xs truncate">
                    <CellValue value={(record.data ?? {})[col]} />
                  </td>
                ))}
                <td className="px-3 py-2 text-xs text-gray-500 whitespace-nowrap">
                  {new Date(record.date_added).toLocaleString()}
                </td>
                <td className="px-3 py-2 text-xs text-gray-500 whitespace-nowrap">
                  {new Date(record.date_modified).toLocaleString()}
                </td>
                <td className="px-3 py-2 whitespace-nowrap">
                  <div className="flex gap-2">
                    <button
                      onClick={() => onEdit(record)}
                      className="text-gray-400 hover:text-blue-400 transition-colors"
                      aria-label="Edit"
                      title="Edit"
                    >
                      ✏
                    </button>
                    <button
                      onClick={() => void handleDelete(record)}
                      className="text-gray-400 hover:text-red-400 transition-colors"
                      aria-label="Delete"
                      title="Delete"
                    >
                      ✕
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div className="flex items-center gap-3 shrink-0 text-sm">
        <button
          onClick={prevPage}
          disabled={!lastReq.offset}
          className="rounded bg-gray-700 hover:bg-gray-600 disabled:opacity-40 px-3 py-1.5 text-gray-300 transition-colors"
        >
          ← Prev
        </button>
        <span className="text-gray-500">Page {page + 1}</span>
        <button
          onClick={nextPage}
          disabled={!hasMore}
          className="rounded bg-gray-700 hover:bg-gray-600 disabled:opacity-40 px-3 py-1.5 text-gray-300 transition-colors"
        >
          Next →
        </button>
      </div>
    </div>
  )
}
