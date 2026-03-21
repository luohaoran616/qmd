import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import YAML from "yaml";

const __filename = fileURLToPath(import.meta.url);
const repoRoot = path.dirname(__filename);
const repoConfigHome = path.join(repoRoot, "else", "openclaw-qmd-xdg", "config");
const repoCacheHome = path.join(repoRoot, "else", "openclaw-qmd-xdg", "cache");
const repoTemplateConfigPath = path.join(repoConfigHome, "qmd", "index.yml");

const env = {
  ...process.env,
  XDG_CONFIG_HOME: process.env.XDG_CONFIG_HOME || repoConfigHome,
  XDG_CACHE_HOME: process.env.XDG_CACHE_HOME || repoCacheHome,
};

ensureMergedQmdConfig({
  env,
  templatePath: repoTemplateConfigPath,
});

const cliPath = path.join(repoRoot, "dist", "cli", "qmd.js");
const child = spawn(process.execPath, [cliPath, ...process.argv.slice(2)], {
  stdio: "inherit",
  env,
  windowsHide: true,
});

child.on("close", (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exitCode = code ?? 0;
});

child.on("error", (error) => {
  console.error(error);
  process.exitCode = 1;
});

function ensureMergedQmdConfig(params) {
  const templatePath = params.templatePath;
  const targetPath = resolveActiveQmdConfigPath(params.env);
  if (!targetPath) {
    return;
  }

  try {
    const templateConfig = readYamlFile(templatePath);
    if (!templateConfig || typeof templateConfig !== "object") {
      return;
    }

    const currentConfig = readYamlFile(targetPath) || {};
    const mergedConfig = mergeQmdConfig(templateConfig, currentConfig);
    const serialized = YAML.stringify(mergedConfig);

    fs.mkdirSync(path.dirname(targetPath), { recursive: true });
    const previous = fs.existsSync(targetPath) ? fs.readFileSync(targetPath, "utf8") : null;
    if (previous !== serialized) {
      fs.writeFileSync(targetPath, serialized, "utf8");
    }
  } catch (error) {
    console.error(`[qmd-openclaw] failed to prepare config: ${String(error)}`);
  }
}

function resolveActiveQmdConfigPath(env) {
  const qmdConfigDir = env.QMD_CONFIG_DIR?.trim();
  if (qmdConfigDir) {
    return path.join(qmdConfigDir, "index.yml");
  }
  const xdgConfigHome = env.XDG_CONFIG_HOME?.trim();
  if (!xdgConfigHome) {
    return null;
  }
  return path.join(xdgConfigHome, "qmd", "index.yml");
}

function readYamlFile(filePath) {
  if (!fs.existsSync(filePath)) {
    return null;
  }
  return YAML.parse(fs.readFileSync(filePath, "utf8"));
}

function mergeQmdConfig(templateConfig, currentConfig) {
  const merged = {
    ...templateConfig,
    ...currentConfig,
  };

  if (currentConfig.collections && typeof currentConfig.collections === "object") {
    merged.collections = currentConfig.collections;
  }
  if (!currentConfig.embedding && templateConfig.embedding) {
    merged.embedding = templateConfig.embedding;
  }
  if (!currentConfig.rerank && templateConfig.rerank) {
    merged.rerank = templateConfig.rerank;
  }
  if (!currentConfig.query_expansion && templateConfig.query_expansion) {
    merged.query_expansion = templateConfig.query_expansion;
  }
  if (!currentConfig.global_context && templateConfig.global_context) {
    merged.global_context = templateConfig.global_context;
  }

  return merged;
}
