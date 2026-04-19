import { defineConfig } from 'vite';
import { resolve } from 'node:path';

const rootDir = process.cwd();
const frontendRoot = resolve(rootDir, 'frontend');

export default defineConfig({
  root: frontendRoot,
  server: {
    host: '0.0.0.0',
    port: Number(process.env.VITE_PORT || 3000),
    open: false,
    strictPort: false,
    proxy: {
      '/api': {
        target: process.env.VITE_API_TARGET || 'http://127.0.0.1:8000',
        changeOrigin: true,
      },
      '/download': {
        target: process.env.VITE_API_TARGET || 'http://127.0.0.1:8000',
        changeOrigin: true,
      },
      '/mapping': {
        target: process.env.VITE_API_TARGET || 'http://127.0.0.1:8000',
        changeOrigin: true,
      },
      '/health': {
        target: process.env.VITE_API_TARGET || 'http://127.0.0.1:8000',
        changeOrigin: true,
      },
    },
  },
  preview: {
    host: '0.0.0.0',
    port: Number(process.env.VITE_PORT || 3000),
  },
  build: {
    outDir: resolve(rootDir, 'frontend/dist'),
    emptyOutDir: true,
  },
});
