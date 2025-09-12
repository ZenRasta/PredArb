import { useEffect, useState } from "react";

function App() {
  const [health, setHealth] = useState(null);

  useEffect(() => {
    fetch(import.meta.env.VITE_API_URL + "/health")
      .then(r => r.json()).then(setHealth).catch(()=>setHealth(null));
  }, []);

  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-100 p-6">
      <h1 className="text-2xl font-bold">PredArb WebApp</h1>
      <p className="text-sm opacity-80">Dark mode ready. Telegram WebApp integration to be added.</p>
      <div className="mt-4 p-4 rounded-xl bg-zinc-900">
        <div className="font-mono text-xs">/health → {health ? JSON.stringify(health) : "loading..."}</div>
      </div>
    </div>
  );
}
export default App;

