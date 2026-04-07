import { defineConfig } from 'vite';

// Healthspend v1.0 Standard Vite Configuration
// Standardizing the build pipeline for 10/10 technical alignment with Cloudflare Pages/Workers.
export default defineConfig({
  root: './',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      input: {
        main: './index.html',
      },
    },
  },
  server: {
    port: 5173,
    strictPort: true,
  },
});
