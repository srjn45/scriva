import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from 'react'
import { ScrivaDBClient } from '../api/client'

const STORAGE_KEY = 'scriva_settings'

interface Settings {
  url: string
  apiKey: string
}

interface AppContextValue {
  settings: Settings
  client: ScrivaDBClient
  connected: boolean
  saveSettings: (s: Settings) => void
  recheckConnection: () => void
}

function loadSettings(): Settings {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (raw) {
      const parsed = JSON.parse(raw) as Partial<Settings>
      if (typeof parsed.url === 'string' && typeof parsed.apiKey === 'string') {
        return { url: parsed.url, apiKey: parsed.apiKey }
      }
    }
  } catch {
    // ignore
  }
  return { url: '', apiKey: '' }
}

const AppContext = createContext<AppContextValue | null>(null)

export function AppProvider({ children }: { children: React.ReactNode }) {
  const [settings, setSettings] = useState<Settings>(loadSettings)
  const [connected, setConnected] = useState(false)
  const [checkToken, setCheckToken] = useState(0)

  const client = useMemo(
    () => new ScrivaDBClient(settings.url, settings.apiKey),
    [settings.url, settings.apiKey],
  )

  // Probe connection whenever client or checkToken changes
  useEffect(() => {
    let cancelled = false
    client.listCollections().then(
      () => { if (!cancelled) setConnected(true) },
      () => { if (!cancelled) setConnected(false) },
    )
    return () => { cancelled = true }
  }, [client, checkToken])

  const saveSettings = useCallback((s: Settings) => {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(s))
    setSettings(s)
  }, [])

  const recheckConnection = useCallback(() => {
    setCheckToken((n) => n + 1)
  }, [])

  return (
    <AppContext.Provider value={{ settings, client, connected, saveSettings, recheckConnection }}>
      {children}
    </AppContext.Provider>
  )
}

export function useApp(): AppContextValue {
  const ctx = useContext(AppContext)
  if (!ctx) throw new Error('useApp must be used inside AppProvider')
  return ctx
}
