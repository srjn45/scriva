import { useState } from 'react'
import { useApp } from '../contexts/AppContext'

export default function ConnectScreen() {
  const { saveSettings, recheckConnection } = useApp()
  const [url, setUrl] = useState('http://localhost:8080')
  const [apiKey, setApiKey] = useState('')

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    saveSettings({ url: url.trim(), apiKey: apiKey.trim() })
    recheckConnection()
  }

  return (
    <div className="min-h-screen bg-gray-950 flex items-center justify-center p-4">
      <div className="w-full max-w-sm bg-gray-900 rounded-xl border border-gray-700 shadow-2xl p-6">
        <div className="flex items-center gap-2 mb-6">
          <span className="text-2xl">⬡</span>
          <span className="text-lg font-bold text-gray-100">ScrivaDB</span>
        </div>
        <form onSubmit={handleSubmit} className="flex flex-col gap-4">
          <div className="flex flex-col gap-1">
            <label className="text-xs text-gray-400">Server URL</label>
            <input
              type="url"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              className="rounded bg-gray-800 border border-gray-600 px-3 py-2 text-sm text-gray-100 focus:outline-none focus:border-blue-500"
              required
            />
          </div>
          <div className="flex flex-col gap-1">
            <label className="text-xs text-gray-400">API Key</label>
            <input
              type="password"
              value={apiKey}
              onChange={(e) => setApiKey(e.target.value)}
              className="rounded bg-gray-800 border border-gray-600 px-3 py-2 text-sm text-gray-100 focus:outline-none focus:border-blue-500"
              placeholder="(optional)"
            />
          </div>
          <button
            type="submit"
            className="rounded bg-blue-600 hover:bg-blue-500 px-4 py-2 text-sm font-medium text-white transition-colors"
          >
            Connect
          </button>
        </form>
      </div>
    </div>
  )
}
