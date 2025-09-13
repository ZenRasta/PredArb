import { useEffect, useState } from 'react'
import { apiFetch } from './api'

interface Opportunity {
  id: string
  name?: string
}

export default function ArbFeed() {
  const [ops, setOps] = useState<Opportunity[]>([])

  useEffect(() => {
    apiFetch('/api/opportunities')
      .then((r) => r.json())
      .then((d) => setOps(d))
      .catch(() => setOps([]))
  }, [])

  return (
    <div>
      <h2 className="text-xl font-bold mb-2">Arbitrage Opportunities</h2>
      <ul className="list-disc pl-6">
        {ops.map((o) => (
          <li key={o.id}>{o.name || o.id}</li>
        ))}
      </ul>
    </div>
  )
}
