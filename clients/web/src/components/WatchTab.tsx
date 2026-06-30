import type { WatchEvent } from '../api/types'
import { useWatch } from '../hooks/useWatch'

interface Props { collection: string; active: boolean }

const OP_STYLES = {
  INSERTED: 'bg-green-900 text-green-300',
  UPDATED:  'bg-blue-900 text-blue-300',
  DELETED:  'bg-red-900 text-red-300',
  OVERFLOW: 'bg-yellow-900 text-yellow-300',
} as const

function EventRow({ event }: { event: WatchEvent }) {
  // An OVERFLOW sentinel carries no record: the server dropped events because
  // this watcher fell behind, so the view may be stale and should be refreshed.
  if (event.op === 'OVERFLOW') {
    return (
      <div className="flex items-start gap-3 py-2 border-b border-gray-800 text-sm">
        <span className="text-xs text-gray-600 whitespace-nowrap mt-0.5 w-20 shrink-0">
          {new Date(event.ts).toLocaleTimeString()}
        </span>
        <span className={`text-xs font-medium px-2 py-0.5 rounded shrink-0 ${OP_STYLES.OVERFLOW}`}>
          OVERFLOW
        </span>
        <span className="text-xs text-yellow-500/80 flex-1">
          events dropped — watcher fell behind; reload to resync
        </span>
      </div>
    )
  }
  return (
    <div className="flex items-start gap-3 py-2 border-b border-gray-800 text-sm">
      <span className="text-xs text-gray-600 whitespace-nowrap mt-0.5 w-20 shrink-0">
        {new Date(event.ts).toLocaleTimeString()}
      </span>
      <span className={`text-xs font-medium px-2 py-0.5 rounded shrink-0 ${OP_STYLES[event.op] ?? 'bg-gray-700 text-gray-300'}`}>
        {event.op}
      </span>
      <span className="font-mono text-xs text-gray-400 shrink-0">{event.record?.id}</span>
      <span className="text-xs text-gray-500 truncate flex-1">
        {JSON.stringify(event.record?.data ?? {})}
      </span>
    </div>
  )
}

export default function WatchTab({ collection, active }: Props) {
  const { events, watching, error, stop, start, clear } = useWatch(collection, active)

  return (
    <div className="flex flex-col h-full gap-3">
      {/* Toolbar */}
      <div className="flex items-center gap-3 shrink-0">
        <span
          className={`text-xs font-medium px-2 py-1 rounded-full ${
            watching ? 'bg-green-900 text-green-300' : 'bg-gray-700 text-gray-400'
          }`}
        >
          {watching ? '● watching' : '○ stopped'}
        </span>
        {watching ? (
          <button
            onClick={stop}
            className="rounded bg-gray-700 hover:bg-gray-600 px-3 py-1 text-xs text-gray-300 transition-colors"
          >
            Stop
          </button>
        ) : (
          <button
            onClick={start}
            className="rounded bg-blue-600 hover:bg-blue-500 px-3 py-1 text-xs text-white transition-colors"
          >
            Start watching
          </button>
        )}
        <button
          onClick={clear}
          className="rounded bg-gray-700 hover:bg-gray-600 px-3 py-1 text-xs text-gray-300 transition-colors"
        >
          Clear
        </button>
        <span className="text-xs text-gray-600 ml-auto">{events.length} events</span>
        {error && <span className="text-xs text-red-400">{error}</span>}
      </div>

      {/* Event log */}
      <div className="flex-1 overflow-y-auto">
        {events.length === 0 && (
          <p className="text-xs text-gray-600 py-4">
            {watching ? 'Waiting for events…' : 'Start watching to see live events'}
          </p>
        )}
        {events.map((event) => (
          <EventRow key={`${event.ts}-${event.record?.id ?? ''}`} event={event} />
        ))}
      </div>
    </div>
  )
}
