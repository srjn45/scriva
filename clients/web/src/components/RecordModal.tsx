import { useEffect, useState } from 'react'
import type { ScrivaDBRecord, RecordData } from '../api/types'
import { useApp } from '../contexts/AppContext'
import { useToast } from '../contexts/ToastContext'

interface Props {
  collection: string
  record: ScrivaDBRecord | null  // null = insert
  onClose: () => void
  onSaved: () => void
}

interface KVRow {
  id: number
  key: string
  value: string
}

let kvId = 0

function dataToKV(data: RecordData): KVRow[] {
  return Object.entries(data).map(([key, value]) => ({
    id: kvId++,
    key,
    value: typeof value === 'string' ? value : JSON.stringify(value),
  }))
}

function kvToData(rows: KVRow[]): RecordData {
  const result: RecordData = {}
  for (const row of rows) {
    if (!row.key.trim()) continue
    try {
      result[row.key.trim()] = JSON.parse(row.value)
    } catch {
      result[row.key.trim()] = row.value
    }
  }
  return result
}

export default function RecordModal({ collection, record, onClose, onSaved }: Props) {
  const { client } = useApp()
  const { showToast } = useToast()
  const isInsert = record === null

  const initialData = isInsert ? {} : (record.data ?? {})
  const [mode, setMode] = useState<'json' | 'form'>('json')
  const [jsonText, setJsonText] = useState(JSON.stringify(initialData, null, 2))
  const [kvRows, setKvRows] = useState<KVRow[]>(
    Object.keys(initialData).length > 0 ? dataToKV(initialData) : [{ id: kvId++, key: '', value: '' }],
  )
  const [jsonError, setJsonError] = useState('')
  const [saving, setSaving] = useState(false)

  // Keep JSON and form in sync when switching modes
  function switchMode(next: 'json' | 'form') {
    if (next === mode) return
    if (next === 'form') {
      try {
        const parsed = JSON.parse(jsonText) as RecordData
        setKvRows(
          Object.keys(parsed).length > 0
            ? dataToKV(parsed)
            : [{ id: kvId++, key: '', value: '' }],
        )
        setJsonError('')
      } catch {
        setJsonError('Fix JSON errors before switching to form mode')
        return
      }
    } else {
      setJsonText(JSON.stringify(kvToData(kvRows), null, 2))
    }
    setMode(next)
  }

  function updateKV(id: number, patch: Partial<KVRow>) {
    setKvRows((prev) => prev.map((r) => (r.id === id ? { ...r, ...patch } : r)))
  }

  function removeKV(id: number) {
    setKvRows((prev) => (prev.length > 1 ? prev.filter((r) => r.id !== id) : prev))
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    let data: RecordData
    if (mode === 'json') {
      try {
        data = JSON.parse(jsonText) as RecordData
      } catch {
        setJsonError('Invalid JSON')
        return
      }
      if (typeof data !== 'object' || Array.isArray(data) || data === null) {
        setJsonError('Must be a JSON object')
        return
      }
    } else {
      data = kvToData(kvRows)
    }

    setSaving(true)
    try {
      if (isInsert) {
        await client.insert(collection, data)
        showToast('success', 'Record inserted')
      } else {
        await client.update(collection, record.id, data)
        showToast('success', `Record ${record.id} updated`)
      }
      onSaved()
      onClose()
    } catch (err) {
      showToast('error', err instanceof Error ? err.message : 'Save failed')
    } finally {
      setSaving(false)
    }
  }

  // Close on Escape
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      {/* backdrop */}
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />
      <div className="relative z-10 flex flex-col w-full max-w-lg bg-gray-900 rounded-xl border border-gray-700 shadow-2xl max-h-[90vh]">
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700 shrink-0">
          <h2 className="text-sm font-semibold text-gray-100">
            {isInsert ? 'Insert Record' : `Edit Record ${record.id}`}
          </h2>
          <div className="flex items-center gap-2">
            {/* Mode toggle */}
            <div className="flex rounded border border-gray-600 overflow-hidden text-xs">
              <button
                type="button"
                onClick={() => switchMode('json')}
                className={`px-3 py-1 transition-colors ${mode === 'json' ? 'bg-gray-600 text-gray-100' : 'bg-gray-800 text-gray-400 hover:text-gray-200'}`}
              >
                JSON
              </button>
              <button
                type="button"
                onClick={() => switchMode('form')}
                className={`px-3 py-1 transition-colors ${mode === 'form' ? 'bg-gray-600 text-gray-100' : 'bg-gray-800 text-gray-400 hover:text-gray-200'}`}
              >
                Form
              </button>
            </div>
            <button
              onClick={onClose}
              className="text-gray-400 hover:text-gray-100 transition-colors"
              aria-label="Close"
            >
              ✕
            </button>
          </div>
        </div>

        {/* Body */}
        <form onSubmit={(e) => void handleSubmit(e)} className="flex flex-col flex-1 overflow-hidden">
          <div className="flex-1 overflow-y-auto p-4">
            {mode === 'json' ? (
              <div className="flex flex-col gap-1">
                <textarea
                  value={jsonText}
                  onChange={(e) => { setJsonText(e.target.value); setJsonError('') }}
                  rows={14}
                  className="w-full rounded bg-gray-800 border border-gray-700 px-3 py-2 text-sm font-mono text-gray-100 focus:outline-none focus:border-blue-500 resize-none"
                  spellCheck={false}
                />
                {jsonError && <p className="text-xs text-red-400">{jsonError}</p>}
              </div>
            ) : (
              <div className="flex flex-col gap-2">
                {kvRows.map((row) => (
                  <div key={row.id} className="flex items-center gap-2">
                    <input
                      type="text"
                      value={row.key}
                      onChange={(e) => updateKV(row.id, { key: e.target.value })}
                      placeholder="key"
                      className="w-36 rounded bg-gray-800 border border-gray-700 px-2 py-1.5 text-sm text-gray-100 placeholder-gray-600 focus:outline-none focus:border-blue-500"
                    />
                    <input
                      type="text"
                      value={row.value}
                      onChange={(e) => updateKV(row.id, { value: e.target.value })}
                      placeholder="value"
                      className="flex-1 rounded bg-gray-800 border border-gray-700 px-2 py-1.5 text-sm text-gray-100 placeholder-gray-600 focus:outline-none focus:border-blue-500"
                    />
                    <button
                      type="button"
                      onClick={() => removeKV(row.id)}
                      className="text-gray-600 hover:text-red-400 transition-colors text-lg leading-none"
                      aria-label="Remove field"
                    >
                      ×
                    </button>
                  </div>
                ))}
                <button
                  type="button"
                  onClick={() => setKvRows((prev) => [...prev, { id: kvId++, key: '', value: '' }])}
                  className="self-start text-xs text-blue-400 hover:text-blue-300 transition-colors"
                >
                  + Add field
                </button>
              </div>
            )}
          </div>

          {/* Footer */}
          <div className="flex justify-end gap-2 px-4 py-3 border-t border-gray-700 shrink-0">
            <button
              type="button"
              onClick={onClose}
              className="rounded bg-gray-700 hover:bg-gray-600 px-4 py-2 text-sm text-gray-300 transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={saving}
              className="rounded bg-blue-600 hover:bg-blue-500 disabled:opacity-50 px-4 py-2 text-sm font-medium text-white transition-colors"
            >
              {saving ? 'Saving…' : isInsert ? 'Insert' : 'Update'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
