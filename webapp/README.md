# PredArb WebApp (React + Vite)

Run locally
----------

```bash
cp .env.example .env
pnpm install   # or npm install
pnpm dev       # or npm run dev
```

Telegram WebApp
---------------

This app can run inside Telegram via the WebApp SDK. When launched from a bot,
`window.Telegram.WebApp` will be available and the UI can read the init data
for the current user. For local iteration, you can run it in a normal browser.

