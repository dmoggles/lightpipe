import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const dashboardDir = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
  plugins: [react()],
  base: "/assets/",
  build: {
    outDir: resolve(dashboardDir, "../src/lightpipe/dashboard"),
    emptyOutDir: true,
    assetsDir: ".",
    rollupOptions: {
      output: {
        entryFileNames: "app.js",
        chunkFileNames: "chunk-[hash].js",
        assetFileNames: "[name][extname]"
      }
    }
  }
});
