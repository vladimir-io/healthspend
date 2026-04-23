import { defineConfig } from 'vite';

export default defineConfig({
  root: './',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      input: { main: './index.html' },
      external: ['turbolite'],
    },
  },
  optimizeDeps: {
    exclude: ['turbolite'],
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
