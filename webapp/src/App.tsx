import { useEffect, useState } from 'react'

declare global {
  interface Window {
    Telegram?: any
  }
}

export default function App() {
  const [tgUser, setTgUser] = useState<string | null>(null)
  const [theme, setTheme] = useState<string>('')

  useEffect(() => {
    const tg = window.Telegram?.WebApp
    try {
      tg?.ready()
      setTheme(tg?.colorScheme || '')
      const u = tg?.initDataUnsafe?.user
      if (u) setTgUser(`${u.first_name || ''} ${u.last_name || ''}`.trim())
    } catch {}
  }, [])

  return (
    <div style={{ padding: 16 }}>
      <h1>PredArb WebApp</h1>
      <p>Telegram user: {tgUser || 'Unknown / Not inside Telegram'}</p>
      <p>Theme: {theme || 'default'}</p>
      <p>
        Backend: <code>{import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000'}</code>
      </p>
      <p>Env: VITE_TELEGRAM_WEBAPP_BOT_ID = <code>{import.meta.env.VITE_TELEGRAM_WEBAPP_BOT_ID || ''}</code></p>
    </div>
  )
}

