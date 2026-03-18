import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const repoRoot = path.dirname(__filename);

const env = {
  ...process.env,
  XDG_CONFIG_HOME: path.join(repoRoot, "else", "openclaw-qmd-xdg", "config"),
  XDG_CACHE_HOME: path.join(repoRoot, "else", "openclaw-qmd-xdg", "cache"),
};

const cliPath = path.join(repoRoot, "dist", "cli", "qmd.js");
const child = spawn(process.execPath, [cliPath, ...process.argv.slice(2)], {
  stdio: "inherit",
  env,
  windowsHide: true,
});

child.on("exit", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 0);
});

child.on("error", (error) => {
  console.error(error);
  process.exit(1);
});
