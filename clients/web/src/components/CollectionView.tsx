import { useState } from 'react'
import type { ScrivaDBRecord } from '../api/types'
import { useApp } from '../contexts/AppContext'
import { useToast } from '../contexts/ToastContext'
import BrowseTab from './BrowseTab'
import IndexesTab from './IndexesTab'
import RecordModal from './RecordModal'
import StatsTab from './StatsTab'
import WatchTab from './WatchTab'

type Tab = 'browse' | 'indexes' | 'stats' | 'watch'

interface Props {
  collection: string
  onDropped: () => void
}

const TABS: { id: Tab; label: string }[] = [
  { id: 'browse', label: 'Browse' },
  { id: 'indexes', label: 'Indexes' },
  { id: 'stats', label: 'Stats' },
  { id: 'watch', label: '⚡ Watch' },
]

export default function CollectionView({ collection, onDropped }: Props) {
  const { client } = useApp()
  const { showToast } = useToast()
  const [tab, setTab] = useState<Tab>('browse')
  const [modalRecord, setModalRecord] = useState<ScrivaDBRecord | null | undefined>(undefined)
  // undefined = closed, null = insert, ScrivaDBRecord = edit
  const [browseKey, setBrowseKey] = useState(0)

  async function handleDrop() {
    if (!window.confirm(`Drop collection "${collection}"? This cannot be undone.`)) return
    try {
      await client.dropCollection(collection)
      showToast('success', `Collection "${collection}" dropped`)
      onDropped()
    } catch (err) {
      showToast('error', err instanceof Error ? err.message : 'Drop failed')
    }
  }

  return (
    <div className="flex flex-col h-full">
      {/* Tab bar */}
      <div className="flex items-center border-b border-gray-800 bg-gray-900 shrink-0 px-4">
        {TABS.map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors ${
              tab === t.id
                ? 'border-blue-500 text-blue-400'
                : 'border-transparent text-gray-400 hover:text-gray-200'
            }`}
          >
            {t.label}
          </button>
        ))}
        <button
          onClick={() => void handleDrop()}
          className="ml-auto text-xs text-red-500 hover:text-red-400 transition-colors px-2 py-1 rounded"
        >
          Drop Collection
        </button>
      </div>

      {/* Tab content */}
      <div className="flex-1 overflow-hidden p-4">
        {tab === 'browse' && (
          <BrowseTab
            key={browseKey}
            collection={collection}
            onInsert={() => setModalRecord(null)}
            onEdit={(record) => setModalRecord(record)}
          />
        )}
        {tab === 'indexes' && <IndexesTab collection={collection} />}
        {tab === 'stats' && <StatsTab collection={collection} />}
        {tab === 'watch' && <WatchTab collection={collection} active={tab === 'watch'} />}
      </div>

      {/* Record modal */}
      {modalRecord !== undefined && (
        <RecordModal
          collection={collection}
          record={modalRecord}
          onClose={() => setModalRecord(undefined)}
          onSaved={() => setBrowseKey((k) => k + 1)}
        />
      )}
    </div>
  )
}
