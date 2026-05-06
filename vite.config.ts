import { cloudflare } from "@cloudflare/vite-plugin";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";
import path from "node:path";

export default defineConfig({
  plugins: [react(), tailwindcss(), cloudflare()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
      "@shared": path.resolve(__dirname, "./shared"),
      "@core": path.resolve(__dirname, "./worker/core"),
      "@app": path.resolve(__dirname, "./worker/app"),
    },
  },
  server: {
    host: "0.0.0.0",
    port: 4500,
    hmr: {
      clientPort: 443,
    },
    warmup: {
      clientFiles: ["./src/**/*.tsx", "./src/**/*.ts"],
    },
  },
  optimizeDeps: {
    include: [
      "react",
      "react-dom",
      "react-router-dom",
      "lucide-react",
      "framer-motion",
      "clsx",
      "tailwind-merge",
      "class-variance-authority",
      "@radix-ui/react-dialog",
      "@radix-ui/react-dropdown-menu",
      "@radix-ui/react-tooltip",
      "@radix-ui/react-tabs",
      "@radix-ui/react-progress",
      "@radix-ui/react-avatar",
      "@radix-ui/react-select",
      "@radix-ui/react-slot",
      "@radix-ui/react-separator",
      "@radix-ui/react-scroll-area",
      "@radix-ui/react-switch",
    ],
  },
});
