import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

export const DEFAULT_LOCK_TTL_MS = 10 * 60 * 1000;

export function resolveOpenClawHome(env = process.env) {
  if (env.OPENCLAW_HOME && env.OPENCLAW_HOME.trim()) {
    return path.resolve(env.OPENCLAW_HOME);
  }
  return path.join(os.homedir(), ".openclaw");
}

export function resolveDefaultConfigPath(env = process.env) {
  if (env.OPENCLAW_CONFIG && env.OPENCLAW_CONFIG.trim()) {
    return path.resolve(env.OPENCLAW_CONFIG);
  }
  return path.join(resolveOpenClawHome(env), "openclaw.json");
}

export async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

export async function readJsonFile(filePath, fallback = null) {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw.replace(/^\uFEFF/, ""));
  } catch {
    return fallback;
  }
}

export async function writeJsonFile(filePath, value) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

export async function writeTextFileIfChanged(filePath, value) {
  const next = String(value ?? "");
  try {
    const current = await fs.readFile(filePath, "utf8");
    if (current === next) {
      return false;
    }
  } catch {}
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, next, "utf8");
  return true;
}

export async function appendJsonl(filePath, value) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.appendFile(filePath, `${JSON.stringify(value)}\n`, "utf8");
}

export async function readJsonl(filePath) {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return raw
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => {
        try {
          return JSON.parse(line);
        } catch {
          return null;
        }
      })
      .filter(Boolean);
  } catch {
    return [];
  }
}

export function resolveWorkspaceDir({ cfg, override } = {}) {
  if (override && override.trim()) {
    return path.resolve(override);
  }
  const fromConfig = cfg?.agents?.defaults?.workspace;
  if (typeof fromConfig === "string" && fromConfig.trim()) {
    return path.resolve(fromConfig);
  }
  return path.join(resolveOpenClawHome(process.env), "workspace");
}

export function resolvePipelinePaths(workspaceDir) {
  const memoryDir = path.join(workspaceDir, "memory");
  const pipelineDir = path.join(memoryDir, ".pipeline");
  return {
    workspaceDir,
    memoryDir,
    indexedDir: path.join(memoryDir, "indexed"),
    archiveDir: path.join(memoryDir, "archive"),
    pipelineDir,
    queueFile: path.join(pipelineDir, "queue.jsonl"),
    cursorFile: path.join(pipelineDir, "cursor.json"),
    canonicalCursorFile: path.join(pipelineDir, "canonical-cursor.json"),
    canonicalStoreFile: path.join(pipelineDir, "canonical-store.json"),
    lockFile: path.join(pipelineDir, "worker.lock"),
    stateFile: path.join(pipelineDir, "worker.state.json"),
    distillLogFile: path.join(pipelineDir, "distill-log.jsonl"),
    promotionLogFile: path.join(pipelineDir, "promotion-log.jsonl"),
    pendingFinalizeFile: path.join(pipelineDir, "pending-finalize.jsonl"),
    proposalsDir: path.join(pipelineDir, "proposals"),
    sessionStateFile: path.join(workspaceDir, "SESSION-STATE.md"),
    soulFile: path.join(workspaceDir, "SOUL.md"),
    userFile: path.join(workspaceDir, "USER.md"),
    identityFile: path.join(workspaceDir, "IDENTITY.md"),
    memoryRootFile: path.join(workspaceDir, "MEMORY.md"),
    learningsDir: path.join(workspaceDir, ".learnings"),
  };
}

export async function ensurePipelineLayout(paths) {
  await fs.mkdir(paths.indexedDir, { recursive: true });
  await fs.mkdir(path.join(paths.indexedDir, "preferences"), { recursive: true });
  await fs.mkdir(path.join(paths.indexedDir, "decisions"), { recursive: true });
  await fs.mkdir(path.join(paths.indexedDir, "procedures"), { recursive: true });
  await fs.mkdir(path.join(paths.indexedDir, "projects"), { recursive: true });
  await fs.mkdir(path.join(paths.indexedDir, "facts"), { recursive: true });
  await fs.mkdir(paths.archiveDir, { recursive: true });
  await fs.mkdir(paths.pipelineDir, { recursive: true });
  await fs.mkdir(paths.proposalsDir, { recursive: true });
  await fs.mkdir(paths.learningsDir, { recursive: true });
}

export function nowIso(value = Date.now()) {
  try {
    return new Date(value).toISOString();
  } catch {
    return new Date().toISOString();
  }
}

export async function readLock(lockFile) {
  return readJsonFile(lockFile, null);
}

export function isLockStale(lockData, ttlMs = DEFAULT_LOCK_TTL_MS) {
  if (!lockData || typeof lockData.startedAt !== "string") {
    return true;
  }
  const started = Date.parse(lockData.startedAt);
  if (!Number.isFinite(started)) {
    return true;
  }
  return Date.now() - started > ttlMs;
}

export async function releaseStaleLock(lockFile, ttlMs = DEFAULT_LOCK_TTL_MS) {
  const lock = await readLock(lockFile);
  if (!lock || !isLockStale(lock, ttlMs)) {
    return false;
  }
  await fs.rm(lockFile, { force: true });
  return true;
}

export function parseDurationMs(value, fallbackMs) {
  if (typeof value === "number" && Number.isFinite(value) && value > 0) {
    return Math.floor(value);
  }
  if (typeof value !== "string") {
    return fallbackMs;
  }
  const trimmed = value.trim().toLowerCase();
  if (!trimmed) {
    return fallbackMs;
  }
  const regex = /(\d+)\s*(ms|s|m|h|d)\b/g;
  let total = 0;
  let matched = false;
  for (const match of trimmed.matchAll(regex)) {
    matched = true;
    const amount = Number.parseInt(match[1], 10);
    const unit = match[2];
    if (!Number.isFinite(amount) || amount <= 0) {
      continue;
    }
    switch (unit) {
      case "ms":
        total += amount;
        break;
      case "s":
        total += amount * 1000;
        break;
      case "m":
        total += amount * 60 * 1000;
        break;
      case "h":
        total += amount * 60 * 60 * 1000;
        break;
      case "d":
        total += amount * 24 * 60 * 60 * 1000;
        break;
    }
  }
  return matched && total > 0 ? total : fallbackMs;
}

function normalizeTargetList(value, allowed, fallback) {
  if (!Array.isArray(value)) {
    return fallback;
  }
  const normalized = value
    .map((item) => String(item ?? "").trim().toLowerCase())
    .filter((item) => allowed.includes(item));
  return normalized.length > 0 ? [...new Set(normalized)] : fallback;
}

export function resolveHookSettings(cfg) {
  const hookCfg = cfg?.hooks?.internal?.entries?.["memory-pipeline"] ?? {};
  const distiller = hookCfg.distiller ?? {};
  const promoter = hookCfg.promoter ?? {};
  const promoterModel = promoter.model ?? {};
  const promoterTrigger = promoter.trigger ?? {};
  const promoterTargets = promoter.targets ?? {};
  const promoterBudgets = promoter.budgets ?? {};
  return {
    enabled: hookCfg.enabled !== false,
    lockTtlMs:
      typeof hookCfg.lockTtlMs === "number" && hookCfg.lockTtlMs > 0
        ? Math.floor(hookCfg.lockTtlMs)
        : DEFAULT_LOCK_TTL_MS,
    qmdUpdateEnabled: hookCfg.qmdUpdateEnabled !== false,
    distiller: {
      baseUrl:
        typeof distiller.baseUrl === "string" && distiller.baseUrl.trim()
          ? distiller.baseUrl.trim()
          : "https://api.siliconflow.cn/v1",
      apiKeyEnv:
        typeof distiller.apiKeyEnv === "string" && distiller.apiKeyEnv.trim()
          ? distiller.apiKeyEnv.trim()
          : "SILICONFLOW_API_KEY",
      model:
        typeof distiller.model === "string" && distiller.model.trim()
          ? distiller.model.trim()
          : "Qwen/Qwen2.5-7B-Instruct",
      temperature:
        typeof distiller.temperature === "number" ? distiller.temperature : 0.1,
      maxOutputTokens:
        typeof distiller.maxOutputTokens === "number" && distiller.maxOutputTokens > 0
          ? Math.floor(distiller.maxOutputTokens)
          : 1400,
      chunkChars:
        typeof distiller.chunkChars === "number" && distiller.chunkChars > 0
          ? Math.floor(distiller.chunkChars)
          : 12000,
      tailTurnsForState:
        typeof distiller.tailTurnsForState === "number" && distiller.tailTurnsForState > 0
          ? Math.floor(distiller.tailTurnsForState)
          : 24,
      timeoutMs:
        typeof distiller.timeoutMs === "number" && distiller.timeoutMs > 0
          ? Math.floor(distiller.timeoutMs)
          : 45000,
    },
    promoter: {
      enabled: promoter.enabled !== false,
      mode: typeof promoter.mode === "string" && promoter.mode.trim() ? promoter.mode.trim() : "mixed",
      trigger: {
        pendingBytes:
          typeof promoterTrigger.pendingBytes === "number" && promoterTrigger.pendingBytes > 0
            ? Math.floor(promoterTrigger.pendingBytes)
            : 200 * 1024,
        maxAgeMs: parseDurationMs(promoterTrigger.maxAge, 3 * 24 * 60 * 60 * 1000),
      },
      model: {
        baseUrl:
          typeof promoterModel.baseUrl === "string" && promoterModel.baseUrl.trim()
            ? promoterModel.baseUrl.trim()
            : (typeof distiller.baseUrl === "string" && distiller.baseUrl.trim()
                ? distiller.baseUrl.trim()
                : "https://api.siliconflow.cn/v1"),
        apiKeyEnv:
          typeof promoterModel.apiKeyEnv === "string" && promoterModel.apiKeyEnv.trim()
            ? promoterModel.apiKeyEnv.trim()
            : (typeof distiller.apiKeyEnv === "string" && distiller.apiKeyEnv.trim()
                ? distiller.apiKeyEnv.trim()
                : "SILICONFLOW_API_KEY"),
        model:
          typeof promoterModel.model === "string" && promoterModel.model.trim()
            ? promoterModel.model.trim()
            : (typeof distiller.model === "string" && distiller.model.trim()
                ? distiller.model.trim()
                : "Qwen/Qwen2.5-7B-Instruct"),
        temperature:
          typeof promoterModel.temperature === "number" ? promoterModel.temperature : 0.1,
        maxOutputTokens:
          typeof promoterModel.maxOutputTokens === "number" && promoterModel.maxOutputTokens > 0
            ? Math.floor(promoterModel.maxOutputTokens)
            : 1800,
        timeoutMs:
          typeof promoterModel.timeoutMs === "number" && promoterModel.timeoutMs > 0
            ? Math.floor(promoterModel.timeoutMs)
            : 45000,
      },
      targets: {
        auto: normalizeTargetList(promoterTargets.auto, ["user", "memory"], ["user", "memory"]),
        proposalOnly: normalizeTargetList(
          promoterTargets.proposalOnly,
          ["soul", "identity"],
          ["soul", "identity"],
        ),
      },
      budgets: {
        userMaxLines:
          typeof promoterBudgets.userMaxLines === "number" && promoterBudgets.userMaxLines > 0
            ? Math.floor(promoterBudgets.userMaxLines)
            : 80,
        userHardMaxLines:
          typeof promoterBudgets.userHardMaxLines === "number" && promoterBudgets.userHardMaxLines > 0
            ? Math.floor(promoterBudgets.userHardMaxLines)
            : 120,
        memoryMaxLines:
          typeof promoterBudgets.memoryMaxLines === "number" && promoterBudgets.memoryMaxLines > 0
            ? Math.floor(promoterBudgets.memoryMaxLines)
            : 120,
        memoryHardMaxLines:
          typeof promoterBudgets.memoryHardMaxLines === "number" && promoterBudgets.memoryHardMaxLines > 0
            ? Math.floor(promoterBudgets.memoryHardMaxLines)
            : 180,
        proposalMaxLines:
          typeof promoterBudgets.proposalMaxLines === "number" && promoterBudgets.proposalMaxLines > 0
            ? Math.floor(promoterBudgets.proposalMaxLines)
            : 60,
      },
      maxInputNotes:
        typeof promoter.maxInputNotes === "number" && promoter.maxInputNotes > 0
          ? Math.floor(promoter.maxInputNotes)
          : 48,
    },
  };
}

export async function logPipeline(paths, level, message, details = {}) {
  await appendJsonl(paths.distillLogFile, {
    timestamp: nowIso(),
    level,
    message,
    details,
  });
}

export function extractTextContent(content) {
  if (typeof content === "string") {
    return content;
  }
  if (!Array.isArray(content)) {
    return "";
  }
  return content
    .map((block) => {
      if (!block || typeof block !== "object") {
        return "";
      }
      if (block.type !== "text") {
        return "";
      }
      return typeof block.text === "string" ? block.text : "";
    })
    .filter(Boolean)
    .join("\n")
    .trim();
}

export function normalizeWhitespace(value) {
  return value.replace(/\r/g, "").replace(/[ \t]+\n/g, "\n").replace(/\n{3,}/g, "\n\n").trim();
}

export function cleanupVisibleText(value) {
  return normalizeWhitespace(
    value
      .replace(/\[\[reply_to_current\]\]\s*/g, "")
      .replace(/\u0000/g, "")
      .trim(),
  );
}

export function shouldIgnoreUserText(text) {
  if (!text) {
    return true;
  }
  return (
    text.startsWith("/") ||
    text.startsWith("A new session was started via /new or /reset.") ||
    text.startsWith("A session was compacted.")
  );
}

export function shouldIgnoreAssistantEntry(entry, text) {
  if (!text) {
    return true;
  }
  const provider = entry?.provider ?? entry?.message?.provider ?? null;
  const model = entry?.model ?? entry?.message?.model ?? null;
  return (
    provider === "openclaw" ||
    model === "gateway-injected" ||
    text.startsWith("OpenClaw ") ||
    text.includes("Queue: collect")
  );
}

export function slugify(value, fallback = "note") {
  const normalized = String(value ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/-{2,}/g, "-");
  return normalized || fallback;
}

export function shortDatePrefix(isoString = nowIso()) {
  return isoString.slice(0, 16).replace(/[:T-]/g, "");
}

export function parseSessionKey(sessionKey) {
  const match = /^agent:([^:]+):/.exec(sessionKey ?? "");
  return {
    agentId: match?.[1] ?? "main",
  };
}

export function pickDefined(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null && value !== "") {
      return value;
    }
  }
  return undefined;
}
