import { useEffect, useState } from 'react'

export default function Settings() {
  const [theme, setTheme] = useState('light')

  useEffect(() => {
    setTheme(document.documentElement.classList.contains('dark') ? 'dark' : 'light')
  }, [])

  return (
    <div>
      <h2 className="text-xl font-bold mb-2">Settings</h2>
      <div className="mb-2">
        <label className="font-semibold">Alerts:</label>
        <p className="text-sm text-gray-500 dark:text-gray-400">Configure alert preferences.</p>
      </div>
      <p>Current theme: {theme}</p>
    </div>
  )
}
