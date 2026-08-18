import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Deployed as a GitHub Pages *project* site under the ledgerone repo, at
// https://trustanprice.github.io/ledgerone/walkthrough/ — base must match
// that subpath so built asset URLs resolve correctly. Local dev (`npm run
// dev`) ignores `base` and always serves from `/`.
export default defineConfig({
  base: '/ledgerone/walkthrough/',
  plugins: [react()],
})
