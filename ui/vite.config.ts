import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Renderer собирается vite'ом; main/preload — esbuild'ом (см. package.json).
export default defineConfig({
  plugins: [react()],
  base: "./",
  build: {
    outDir: "dist",
    target: "chrome130",
  },
  server: {
    port: 5183,
    strictPort: true,
  },
});
