import React from 'react'
import { createRoot } from 'react-dom/client'
import { createBrowserRouter, RouterProvider } from 'react-router-dom'
import App from './App'
import ArbFeed from './ArbFeed'
import GroupView from './GroupView'
import Explorer from './Explorer'
import Settings from './Settings'
import './index.css'

const router = createBrowserRouter([
  {
    path: '/',
    element: <App />,
    children: [
      { index: true, element: <ArbFeed /> },
      { path: 'group/:id', element: <GroupView /> },
      { path: 'explorer', element: <Explorer /> },
      { path: 'settings', element: <Settings /> },
    ],
  },
])

createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <RouterProvider router={router} />
  </React.StrictMode>
)

