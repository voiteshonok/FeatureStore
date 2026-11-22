import path from "node:path";
import { fileURLToPath } from "node:url";

import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

const rootDir = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@app": path.resolve(rootDir, "src/app"),
      "@pages": path.resolve(rootDir, "src/pages"),
      "@features": path.resolve(rootDir, "src/features"),
      "@entities": path.resolve(rootDir, "src/entities"),
      "@shared": path.resolve(rootDir, "src/shared"),
    },
  },
  server: {
    port: 5173,
    proxy: {
      "/api/v1/stream": {
        target: "http://127.0.0.1:8080",
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/api/, ""),
        timeout: 0,
        proxyTimeout: 0,
        configure: (proxy) => {
          proxy.on("proxyRes", (proxyRes) => {
            proxyRes.headers["cache-control"] = "no-cache";
            proxyRes.headers["x-accel-buffering"] = "no";
          });
        },
      },
      "/api": {
        target: "http://127.0.0.1:8080",
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/api/, ""),
      },
    },
  },
  test: {
    environment: "jsdom",
    setupFiles: ["./src/test/setup.ts"],
    globals: true,
    pool: "threads",
    fileParallelism: false,
  },
});
