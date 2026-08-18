import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Default base is "/" — correct for Vercel (or any host that serves this
// app from the root of its own domain) and for local dev. GitHub Pages is
// the one target that needs a subpath, since it serves this as a project
// site under https://trustanprice.github.io/ledgerone/walkthrough/ — the
// GitHub Actions workflow sets VITE_BASE_PATH=/ledgerone/walkthrough/
// only for that build. Getting this wrong produces a blank page (every
// asset/data URL 404s) with no error in the browser console beyond failed
// network requests — check Network tab first if the page ever goes blank
// after a deploy.
export default defineConfig({
  base: process.env.VITE_BASE_PATH || '/',
  plugins: [react()],
})
