import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// Default base is "/" — correct for Vercel (or any host that serves this
// app from the root of its own domain) and for local dev. GitHub Pages is
// the one target that needs a subpath, since it serves this as a project
// site under https://trustanprice.github.io/ledgerone/ (the repo name,
// nothing more — this app IS the entire Pages site, nothing else shares
// it) — the GitHub Actions workflow sets VITE_BASE_PATH=/ledgerone/ only
// for that build. Getting this wrong produces a blank page (every
// asset/data URL 404s) with no error in the browser console beyond failed
// network requests — check Network tab first if the page ever goes blank
// after a deploy. (This exact bug shipped once already: base was set to
// /ledgerone/walkthrough/, an extra path segment nothing in the workflow
// actually created — Vite's `base` only rewrites how HTML references
// asset paths, it doesn't move the built files into a matching
// subdirectory, so the deployed site referenced URLs that never existed.)
export default defineConfig({
  base: process.env.VITE_BASE_PATH || '/',
  plugins: [react()],
})
