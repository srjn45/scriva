import { useState } from 'react'
import { AppProvider, useApp } from './contexts/AppContext'
import { ToastProvider } from './contexts/ToastContext'
import ToastContainer from './components/Toast'
import ConnectScreen from './components/ConnectScreen'
import SettingsPanel from './components/SettingsPanel'
import Sidebar from './components/Sidebar'
import CollectionView from './components/CollectionView'

function Shell() {
  const { settings, connected } = useApp()
  const [showSettings, setShowSettings] = useState(false)
  const [activeCollection, setActiveCollection] = useState<string | null>(null)

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100 flex flex-col">
      {/* Top bar */}
      <header className="flex items-center gap-3 px-4 py-2 border-b border-gray-800 bg-gray-900 shrink-0">
        <span className="text-lg">⬡</span>
        <span className="font-bold text-gray-100 mr-2">ScrivaDB</span>
        <span className="text-sm text-gray-400">{settings.url}</span>
        <span
          className={`ml-2 text-xs font-medium px-2 py-0.5 rounded-full ${
            connected
              ? 'bg-green-900 text-green-300'
              : 'bg-red-900 text-red-300'
          }`}
        >
          {connected ? '● connected' : '● disconnected'}
        </span>
        <button
          onClick={() => setShowSettings(true)}
          className="ml-auto text-gray-400 hover:text-gray-100 transition-colors text-lg"
          aria-label="Open settings"
        >
          ⚙
        </button>
      </header>

      {/* Body */}
      <div className="flex flex-1 overflow-hidden">
        <Sidebar activeCollection={activeCollection} onSelect={setActiveCollection} />
        <main className="flex-1 overflow-hidden">
          {activeCollection ? (
            <CollectionView
              collection={activeCollection}
              onDropped={() => setActiveCollection(null)}
            />
          ) : (
            <div className="flex items-center justify-center h-full text-gray-600 text-sm">
              Select a collection from the sidebar
            </div>
          )}
        </main>
      </div>

      {showSettings && <SettingsPanel onClose={() => setShowSettings(false)} />}
      <ToastContainer />
    </div>
  )
}

function AppInner() {
  const { settings } = useApp()
  // Show ConnectScreen if no URL saved yet
  if (!settings.url) return <ConnectScreen />
  return <Shell />
}

export default function App() {
  return (
    <ToastProvider>
      <AppProvider>
        <AppInner />
      </AppProvider>
    </ToastProvider>
  )
}
