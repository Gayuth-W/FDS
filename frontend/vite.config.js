import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// The dashboard runs in the browser and calls each node at its host-mapped port
// (http://localhost:8001..8005). Override the node list at build time with
// VITE_DFS_NODES if your ports differ.
export default defineConfig({
  plugins: [react()],
  server: { port: 5173, host: true }
})
