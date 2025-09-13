import { useEffect, useState } from 'react'
import { NavLink, Outlet } from 'react-router-dom'

declare global {
  interface Window {
    Telegram?: any
  }
}

export default function App() {
  const [tgUser, setTgUser] = useState<string | null>(null)

  useEffect(() => {
    const tg = window.Telegram?.WebApp
    try {
      tg?.ready()
      const params = tg?.themeParams || {}
      Object.entries(params).forEach(([k, v]: [string, any]) => {
        document.documentElement.style.setProperty(`--tg-theme-${k.replace(/_/g, '-')}`, v)
      })
      if (tg?.colorScheme === 'dark') document.documentElement.classList.add('dark')
      else document.documentElement.classList.remove('dark')
      const u = tg?.initDataUnsafe?.user
      if (u) setTgUser(`${u.first_name || ''} ${u.last_name || ''}`.trim())
    } catch {}
  }, [])

  return (
    <div className="p-4">
      <h1 className="text-2xl font-bold mb-4">PredArb WebApp</h1>
      <p className="mb-4">Telegram user: {tgUser || 'Unknown / Not inside Telegram'}</p>
      <nav className="flex gap-4 mb-4">
        <NavLink to="/" className="underline">Feed</NavLink>
        <NavLink to="/explorer" className="underline">Explorer</NavLink>
        <NavLink to="/settings" className="underline">Settings</NavLink>
      </nav>
      <Outlet />
    </div>
  )
}

