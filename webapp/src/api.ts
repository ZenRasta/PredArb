let accessToken: string | null = localStorage.getItem('accessToken')
let refreshToken: string | null = localStorage.getItem('refreshToken')

async function refresh() {
  if (!refreshToken) return false
  const resp = await fetch('/auth/refresh', {
    method: 'POST',
    headers: { Authorization: `Bearer ${refreshToken}` },
  })
  if (resp.ok) {
    const data = await resp.json()
    accessToken = data.accessToken
    refreshToken = data.refreshToken
    if (accessToken) localStorage.setItem('accessToken', accessToken)
    if (refreshToken) localStorage.setItem('refreshToken', refreshToken)
    return true
  }
  return false
}

export async function apiFetch(input: RequestInfo | URL, init: RequestInit = {}) {
  const headers = new Headers(init.headers || {})
  if (accessToken) headers.set('Authorization', `Bearer ${accessToken}`)
  const resp = await fetch(input, { ...init, headers })
  if (resp.status !== 401) return resp
  if (await refresh()) {
    const retryHeaders = new Headers(init.headers || {})
    if (accessToken) retryHeaders.set('Authorization', `Bearer ${accessToken}`)
    return fetch(input, { ...init, headers: retryHeaders })
  }
  return resp
}
