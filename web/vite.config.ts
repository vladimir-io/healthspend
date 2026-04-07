import { defineConfig } from 'vite';

// Healthspend v1.0 Standard Vite Configuration
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
    strictPort: false,
    headers: {
      'Accept-Ranges': 'bytes',
      'Access-Control-Expose-Headers': 'Accept-Ranges, Content-Length, Content-Range, ETag',
      'Cache-Control': 'no-cache',
    },
  },
});
