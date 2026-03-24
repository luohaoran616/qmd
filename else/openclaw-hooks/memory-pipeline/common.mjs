import fs from "node:fs/promises";
import crypto from "node:crypto";
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
          : 2200,
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

export const DURABLE_NOTE_TYPES = Object.freeze([
  "preference",
  "decision",
  "procedure",
  "project",
  "fact",
]);

export const DURABLE_NOTE_LIMITS = Object.freeze({
  evidenceMax: 6,
  detailMax: 12,
  configRefMax: 12,
  keyExcerptMax: 8,
  sourceAnchorMax: 16,
  tagMax: 5,
  messageIdMax: 8,
  previewDetailMax: 6,
  previewConfigRefMax: 6,
  previewSourceAnchorMax: 3,
  previewKeyExcerptMax: 2,
  overflowDetailCount: 10,
  overflowSourceAnchorCount: 6,
  overflowKeyExcerptCount: 6,
  overflowRenderedLineCount: 80,
  overflowRenderedCharCount: 2400,
  overflowMinimumLineSavings: 20,
  overflowMinimumCharSavings: 700,
  anchorExcerptMaxChars: 200,
  inlineTextMaxChars: 280,
});

function escapeRegExp(value) {
  return String(value ?? "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function toArray(value) {
  if (Array.isArray(value)) {
    return value;
  }
  if (value === undefined || value === null || value === "") {
    return [];
  }
  return [value];
}

export function trimNoteText(value, maxChars = DURABLE_NOTE_LIMITS.inlineTextMaxChars) {
  const normalized = String(value ?? "").replace(/\s+/g, " ").trim();
  if (!normalized) {
    return "";
  }
  if (normalized.length <= maxChars) {
    return normalized;
  }
  return `${normalized.slice(0, maxChars - 3).trimEnd()}...`;
}

function stripWrappingQuotes(value) {
  return String(value ?? "")
    .trim()
    .replace(/^["'“”‘’]+|["'“”‘’]+$/g, "")
    .trim();
}

export function dedupeStrings(values, { maxCount = Infinity, maxChars = DURABLE_NOTE_LIMITS.inlineTextMaxChars } = {}) {
  const seen = new Set();
  const result = [];
  for (const raw of toArray(values)) {
    const item = trimNoteText(raw, maxChars);
    if (!item) {
      continue;
    }
    const key = item.toLowerCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    result.push(item);
    if (result.length >= maxCount) {
      break;
    }
  }
  return result;
}

function normalizePositiveInt(value, fallback = null) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (Number.isFinite(parsed) && parsed > 0) {
    return parsed;
  }
  return fallback;
}

function bulletListFromSection(body, heading) {
  const headingMatch = new RegExp(`^##\\s+${escapeRegExp(heading)}\\s*$`, "m").exec(body);
  if (!headingMatch) {
    return [];
  }
  const remaining = body.slice(headingMatch.index + headingMatch[0].length).replace(/^\r?\n/, "");
  const nextHeadingIndex = remaining.search(/^##\s+/m);
  const block = nextHeadingIndex === -1 ? remaining : remaining.slice(0, nextHeadingIndex);
  return block
    .split(/\r?\n/)
    .map((line) => /^\s*-\s*(.+)$/.exec(line)?.[1]?.trim() ?? "")
    .filter(Boolean);
}

function bulletListFromLeadLabel(body, label) {
  const lines = body.split(/\r?\n/);
  const startIndex = lines.findIndex((line) => line.trim() === `- ${label}:`);
  if (startIndex === -1) {
    return [];
  }
  const items = [];
  for (let index = startIndex + 1; index < lines.length; index += 1) {
    const line = lines[index];
    const bulletMatch = /^\s*-\s*(.+)$/.exec(line);
    if (bulletMatch) {
      items.push(bulletMatch[1].trim());
      continue;
    }
    if (!line.trim()) {
      if (items.length > 0) {
        break;
      }
      continue;
    }
    break;
  }
  return items;
}

export function parseFrontmatter(raw) {
  const match = raw.match(/^---\r?\n([\s\S]*?)\r?\n---\r?\n?([\s\S]*)$/);
  if (!match) {
    return { frontmatter: {}, body: raw };
  }
  const [, header, body] = match;
  const frontmatter = {};
  let arrayKey = null;
  for (const rawLine of header.split(/\r?\n/)) {
    const line = rawLine.replace(/\r/g, "");
    const keyMatch = /^([A-Za-z0-9_-]+):\s*(.*)$/.exec(line.trim());
    if (keyMatch) {
      const [, key, value] = keyMatch;
      if (value === "") {
        frontmatter[key] = [];
        arrayKey = key;
      } else {
        frontmatter[key] = value.trim();
        arrayKey = null;
      }
      continue;
    }
    const itemMatch = /^\s*-\s*(.+)$/.exec(line);
    if (itemMatch && arrayKey) {
      frontmatter[arrayKey].push(itemMatch[1].trim());
    }
  }
  return { frontmatter, body };
}

export function formatSourceAnchor(anchor, { includeExcerpt = true } = {}) {
  if (!anchor) {
    return "";
  }
  const turnStart = normalizePositiveInt(anchor.turnStart);
  const turnEnd = normalizePositiveInt(anchor.turnEnd, turnStart);
  const lineStart = normalizePositiveInt(anchor.jsonlLineStart);
  const lineEnd = normalizePositiveInt(anchor.jsonlLineEnd, lineStart);
  const messageIds = dedupeStrings(anchor.messageIds, {
    maxCount: DURABLE_NOTE_LIMITS.messageIdMax,
    maxChars: 80,
  });
  const parts = [];
  if (turnStart) {
    parts.push(`turns ${turnStart}-${turnEnd || turnStart}`);
  }
  if (lineStart) {
    parts.push(`jsonl L${lineStart}-L${lineEnd || lineStart}`);
  }
  if (messageIds.length > 0) {
    parts.push(`msgIds: ${messageIds.join(", ")}`);
  }
  if (anchor.sourcePath) {
    parts.push(`source: ${String(anchor.sourcePath).replace(/\|/g, "/")}`);
  }
  if (includeExcerpt) {
    const excerpt = trimNoteText(anchor.excerpt ?? "", DURABLE_NOTE_LIMITS.anchorExcerptMaxChars).replace(/\|/g, "/");
    if (excerpt) {
      parts.push(`excerpt: ${excerpt}`);
    }
  }
  return parts.join(" | ");
}

export function parseSourceAnchor(line) {
  const value = String(line ?? "").trim();
  if (!value) {
    return null;
  }
  const parts = value.split(/\s+\|\s+/);
  const anchor = {
    sessionId: null,
    sourcePath: null,
    turnStart: null,
    turnEnd: null,
    messageIds: [],
    jsonlLineStart: null,
    jsonlLineEnd: null,
    excerpt: "",
  };
  for (const part of parts) {
    let match = /^turns\s+(\d+)-(\d+)$/i.exec(part);
    if (match) {
      anchor.turnStart = normalizePositiveInt(match[1]);
      anchor.turnEnd = normalizePositiveInt(match[2], anchor.turnStart);
      continue;
    }
    match = /^jsonl\s+L(\d+)-L(\d+)$/i.exec(part);
    if (match) {
      anchor.jsonlLineStart = normalizePositiveInt(match[1]);
      anchor.jsonlLineEnd = normalizePositiveInt(match[2], anchor.jsonlLineStart);
      continue;
    }
    match = /^msgIds:\s*(.+)$/i.exec(part);
    if (match) {
      anchor.messageIds = dedupeStrings(match[1].split(/\s*,\s*/), {
        maxCount: DURABLE_NOTE_LIMITS.messageIdMax,
        maxChars: 80,
      });
      continue;
    }
    match = /^source:\s*(.+)$/i.exec(part);
    if (match) {
      anchor.sourcePath = match[1].trim();
      continue;
    }
    match = /^excerpt:\s*(.+)$/i.exec(part);
    if (match) {
      anchor.excerpt = trimNoteText(match[1], DURABLE_NOTE_LIMITS.anchorExcerptMaxChars);
    }
  }
  if (!anchor.turnStart && !anchor.jsonlLineStart && anchor.messageIds.length === 0 && !anchor.sourcePath) {
    return null;
  }
  return anchor;
}

function sourceAnchorIdentity(anchor) {
  return JSON.stringify({
    sessionId: anchor?.sessionId ?? null,
    sourcePath: anchor?.sourcePath ?? null,
    turnStart: normalizePositiveInt(anchor?.turnStart),
    turnEnd: normalizePositiveInt(anchor?.turnEnd, normalizePositiveInt(anchor?.turnStart)),
    jsonlLineStart: normalizePositiveInt(anchor?.jsonlLineStart),
    jsonlLineEnd: normalizePositiveInt(anchor?.jsonlLineEnd, normalizePositiveInt(anchor?.jsonlLineStart)),
    messageIds: dedupeStrings(anchor?.messageIds, {
      maxCount: DURABLE_NOTE_LIMITS.messageIdMax,
      maxChars: 80,
    }),
  });
}

export function dedupeSourceAnchors(values, { maxCount = DURABLE_NOTE_LIMITS.sourceAnchorMax } = {}) {
  const result = [];
  const seen = new Set();
  for (const value of toArray(values)) {
    const normalized = normalizeSourceAnchor(value);
    if (!normalized) {
      continue;
    }
    const key = sourceAnchorIdentity(normalized);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    result.push(normalized);
    if (result.length >= maxCount) {
      break;
    }
  }
  return result;
}

export function buildFallbackSourceAnchor(turns, context = {}) {
  if (!Array.isArray(turns) || turns.length === 0) {
    return null;
  }
  const first = turns[0];
  const last = turns[turns.length - 1];
  const excerptSource =
    context.excerpt ||
    first?.text ||
    context.summary ||
    context.evidence?.[0] ||
    "";
  return normalizeSourceAnchor({
    sessionId: context.sessionId ?? null,
    sourcePath: context.sourcePath ?? null,
    turnStart: first?.turnNumber ?? 1,
    turnEnd: last?.turnNumber ?? first?.turnNumber ?? 1,
    messageIds: turns.map((turn) => turn?.id).filter(Boolean),
    jsonlLineStart: first?.jsonlLine ?? null,
    jsonlLineEnd: last?.jsonlLine ?? first?.jsonlLine ?? null,
    excerpt: excerptSource,
  });
}

export function normalizeSourceAnchor(raw, fallback = {}) {
  if (!raw && !fallback) {
    return null;
  }
  const parsed = typeof raw === "string" ? parseSourceAnchor(raw) : raw;
  const turnStart = normalizePositiveInt(parsed?.turnStart, normalizePositiveInt(fallback?.turnStart));
  const turnEnd = normalizePositiveInt(
    parsed?.turnEnd,
    normalizePositiveInt(fallback?.turnEnd, turnStart),
  );
  const jsonlLineStart = normalizePositiveInt(
    parsed?.jsonlLineStart,
    normalizePositiveInt(fallback?.jsonlLineStart),
  );
  const jsonlLineEnd = normalizePositiveInt(
    parsed?.jsonlLineEnd,
    normalizePositiveInt(fallback?.jsonlLineEnd, jsonlLineStart),
  );
  const messageIds = dedupeStrings(
    parsed?.messageIds ?? fallback?.messageIds,
    { maxCount: DURABLE_NOTE_LIMITS.messageIdMax, maxChars: 80 },
  );
  const sourcePath = String(parsed?.sourcePath ?? fallback?.sourcePath ?? "").trim() || null;
  const sessionId = String(parsed?.sessionId ?? fallback?.sessionId ?? "").trim() || null;
  const excerpt = trimNoteText(
    parsed?.excerpt ?? fallback?.excerpt ?? "",
    DURABLE_NOTE_LIMITS.anchorExcerptMaxChars,
  );
  if (!turnStart && !jsonlLineStart && messageIds.length === 0 && !sourcePath) {
    return null;
  }
  return {
    sessionId,
    sourcePath,
    turnStart,
    turnEnd: turnEnd || turnStart,
    messageIds,
    jsonlLineStart,
    jsonlLineEnd: jsonlLineEnd || jsonlLineStart,
    excerpt,
  };
}

function mergeUniqueStrings(left, right, maxCount, maxChars = DURABLE_NOTE_LIMITS.inlineTextMaxChars) {
  return dedupeStrings([...(left ?? []), ...(right ?? [])], { maxCount, maxChars });
}

function pickShorterText(left, right) {
  const a = trimNoteText(left);
  const b = trimNoteText(right);
  if (!a) {
    return b;
  }
  if (!b) {
    return a;
  }
  return b.length < a.length ? b : a;
}

function pickLongerText(left, right) {
  const a = trimNoteText(left);
  const b = trimNoteText(right);
  if (!a) {
    return b;
  }
  if (!b) {
    return a;
  }
  return b.length > a.length ? b : a;
}

function makeStableKey(raw, title, summary) {
  const keySource = typeof raw?.key === "string" ? raw.key : title || summary;
  const fallback = crypto.createHash("sha1").update(JSON.stringify(raw ?? {})).digest("hex").slice(0, 10);
  return slugify(keySource, fallback);
}

export function normalizeExtractedNote(raw, context = {}) {
  const type = DURABLE_NOTE_TYPES.includes(raw?.type) ? raw.type : "fact";
  const title = trimNoteText(raw?.title ?? "", 120);
  const summary = trimNoteText(raw?.summary ?? "", 220);
  const whyItMatters = trimNoteText(raw?.whyItMatters ?? "", 240);
  if (!title || !summary || !whyItMatters) {
    return null;
  }

  const fallbackAnchor = buildFallbackSourceAnchor(context.chunkTurns ?? [], {
    sessionId: context.sessionId,
    sourcePath: context.sourcePath,
    summary,
    evidence: raw?.evidence,
    excerpt: raw?.keyExcerpts?.[0] ?? raw?.evidence?.[0] ?? summary,
  });
  const sourceAnchors = dedupeSourceAnchors(
    toArray(raw?.sourceAnchors).map((anchor) => normalizeSourceAnchor(anchor, fallbackAnchor)),
    { maxCount: DURABLE_NOTE_LIMITS.sourceAnchorMax },
  );
  const finalAnchors = sourceAnchors.length > 0 ? sourceAnchors : (fallbackAnchor ? [fallbackAnchor] : []);
  const keyExcerpts = mergeUniqueStrings(
    toArray(raw?.keyExcerpts).map(stripWrappingQuotes),
    finalAnchors.map((anchor) => anchor.excerpt).filter(Boolean),
    DURABLE_NOTE_LIMITS.keyExcerptMax,
    DURABLE_NOTE_LIMITS.anchorExcerptMaxChars,
  );

  return {
    type,
    key: makeStableKey(raw, title, summary),
    title,
    summary,
    whyItMatters,
    evidence: dedupeStrings(raw?.evidence, {
      maxCount: DURABLE_NOTE_LIMITS.evidenceMax,
      maxChars: 220,
    }),
    project:
      typeof raw?.project === "string" && raw.project.trim() ? slugify(raw.project) : null,
    tags: toArray(raw?.tags)
      .map((tag) => slugify(tag))
      .filter(Boolean)
      .slice(0, DURABLE_NOTE_LIMITS.tagMax),
    durableDetails: dedupeStrings(raw?.durableDetails, {
      maxCount: DURABLE_NOTE_LIMITS.detailMax,
      maxChars: 320,
    }),
    configRefs: dedupeStrings(raw?.configRefs, {
      maxCount: DURABLE_NOTE_LIMITS.configRefMax,
      maxChars: 280,
    }),
    sourceAnchors: finalAnchors,
    keyExcerpts,
    sessionId: context.sessionId ?? null,
    createdAt: context.createdAt ?? nowIso(),
    updatedAt: context.updatedAt ?? context.createdAt ?? nowIso(),
    lastSeenAt: context.updatedAt ?? context.createdAt ?? nowIso(),
    sourcePath: context.sourcePath ?? null,
    sourceRange: context.sourceRange ?? null,
    sourceSessions: context.sessionId ? [context.sessionId] : [],
    sidecarPath: typeof raw?.sidecarPath === "string" && raw.sidecarPath.trim() ? raw.sidecarPath.trim() : null,
  };
}

export function mergeDurableNote(existing, incoming) {
  if (!existing) {
    return incoming ? { ...incoming } : null;
  }
  if (!incoming) {
    return { ...existing };
  }

  const merged = {
    ...existing,
    ...incoming,
    type:
      existing.type && existing.type !== "fact"
        ? existing.type
        : (incoming.type || existing.type || "fact"),
    project: existing.project || incoming.project || null,
    key: incoming.key || existing.key,
    title: pickShorterText(existing.title, incoming.title),
    summary: pickShorterText(existing.summary, incoming.summary),
    whyItMatters: pickLongerText(existing.whyItMatters, incoming.whyItMatters),
    evidence: mergeUniqueStrings(existing.evidence, incoming.evidence, DURABLE_NOTE_LIMITS.evidenceMax, 220),
    durableDetails: mergeUniqueStrings(
      existing.durableDetails,
      incoming.durableDetails,
      DURABLE_NOTE_LIMITS.detailMax,
      320,
    ),
    configRefs: mergeUniqueStrings(
      existing.configRefs,
      incoming.configRefs,
      DURABLE_NOTE_LIMITS.configRefMax,
      280,
    ),
    sourceAnchors: dedupeSourceAnchors([
      ...(existing.sourceAnchors ?? []),
      ...(incoming.sourceAnchors ?? []),
    ]),
    keyExcerpts: mergeUniqueStrings(
      existing.keyExcerpts,
      incoming.keyExcerpts,
      DURABLE_NOTE_LIMITS.keyExcerptMax,
      DURABLE_NOTE_LIMITS.anchorExcerptMaxChars,
    ),
    tags: mergeUniqueStrings(existing.tags, incoming.tags, DURABLE_NOTE_LIMITS.tagMax, 80).map((tag) => slugify(tag)),
    createdAt:
      Date.parse(existing.createdAt ?? "") <= Date.parse(incoming.createdAt ?? "")
        ? (existing.createdAt ?? incoming.createdAt ?? nowIso())
        : (incoming.createdAt ?? existing.createdAt ?? nowIso()),
    updatedAt: incoming.updatedAt ?? nowIso(),
    lastSeenAt: incoming.lastSeenAt ?? incoming.updatedAt ?? nowIso(),
    sessionId: incoming.sessionId || existing.sessionId || null,
    sourcePath: incoming.sourcePath || existing.sourcePath || null,
    sourceRange: incoming.sourceRange || existing.sourceRange || null,
    sourceSessions: mergeUniqueStrings(existing.sourceSessions, incoming.sourceSessions, 32, 120),
    sidecarPath: incoming.sidecarPath || existing.sidecarPath || null,
    filePath: existing.filePath ?? incoming.filePath ?? null,
    relativePath: existing.relativePath ?? incoming.relativePath ?? null,
    hash: incoming.hash ?? existing.hash ?? null,
    size: incoming.size ?? existing.size ?? null,
    mtimeMs: incoming.mtimeMs ?? existing.mtimeMs ?? null,
  };
  return merged;
}

function renderListSection(title, items) {
  const entries = dedupeStrings(items);
  if (entries.length === 0) {
    return [];
  }
  return [`## ${title}`, ...entries.map((item) => `- ${item}`), ""];
}

function renderAnchorSection(title, anchors, { includeExcerpt = false } = {}) {
  const entries = dedupeSourceAnchors(anchors).map((anchor) => formatSourceAnchor(anchor, { includeExcerpt }));
  if (entries.length === 0) {
    return [];
  }
  return [`## ${title}`, ...entries.map((item) => `- ${item}`), ""];
}

function renderExcerptSection(title, excerpts) {
  const entries = dedupeStrings(excerpts, {
    maxCount: DURABLE_NOTE_LIMITS.keyExcerptMax,
    maxChars: DURABLE_NOTE_LIMITS.anchorExcerptMaxChars,
  });
  if (entries.length === 0) {
    return [];
  }
  return [`## ${title}`, ...entries.map((item) => `- "${item.replace(/"/g, '\\"')}"`), ""];
}

function baseFrontmatter(note) {
  const tags = Array.isArray(note?.tags) && note.tags.length > 0 ? note.tags : ["memory"];
  const lines = [
    "---",
    `key: ${note?.key ?? ""}`,
    `type: ${note?.type ?? "fact"}`,
    `project: ${note?.project ?? ""}`,
    `sessionId: ${note?.sessionId ?? ""}`,
    `createdAt: ${note?.createdAt ?? nowIso()}`,
    `updatedAt: ${note?.updatedAt ?? note?.createdAt ?? nowIso()}`,
    `lastSeenAt: ${note?.lastSeenAt ?? note?.updatedAt ?? note?.createdAt ?? nowIso()}`,
    `sourcePath: ${note?.sourcePath ?? ""}`,
    `sourceRange: ${note?.sourceRange ?? ""}`,
  ];
  if (Array.isArray(note?.sourceSessions) && note.sourceSessions.length > 0) {
    lines.push("sourceSessions:");
    for (const sessionId of note.sourceSessions) {
      lines.push(`  - ${sessionId}`);
    }
  }
  if (note?.sidecarPath) {
    lines.push(`sidecarPath: ${note.sidecarPath}`);
  }
  lines.push("tags:");
  for (const tag of tags) {
    lines.push(`  - ${tag}`);
  }
  lines.push("---", "");
  return lines;
}

function fullRenderLines(note) {
  return [
    ...baseFrontmatter(note),
    `# ${note.title}`,
    "",
    `- Summary: ${note.summary}`,
    `- Why it matters: ${note.whyItMatters}`,
    "- Evidence:",
    ...(
      note.evidence.length > 0
        ? note.evidence.map((item) => `  - ${item}`)
        : ["  - Derived from the latest session boundary transcript."]
    ),
    "",
    ...renderListSection("Durable Details", note.durableDetails),
    ...renderListSection("Config / Commands / Paths", note.configRefs),
    ...renderAnchorSection("Source Anchors", note.sourceAnchors, { includeExcerpt: false }),
    ...renderExcerptSection("Key Excerpts", note.keyExcerpts),
  ];
}

export function getDurableNoteOverflow(note) {
  const fullContent = fullRenderLines({
    ...note,
    sidecarPath: null,
  }).join("\n").trimEnd() + "\n";
  const fullLineCount = fullContent.split(/\r?\n/).length;
  const fullCharCount = fullContent.length;
  const previewContent = fullRenderLines({
    ...note,
    sidecarPath: null,
    durableDetails: (note?.durableDetails ?? []).slice(0, DURABLE_NOTE_LIMITS.previewDetailMax),
    configRefs: (note?.configRefs ?? []).slice(0, DURABLE_NOTE_LIMITS.previewConfigRefMax),
    sourceAnchors: (note?.sourceAnchors ?? []).slice(0, DURABLE_NOTE_LIMITS.previewSourceAnchorMax),
    keyExcerpts: (note?.keyExcerpts ?? []).slice(0, DURABLE_NOTE_LIMITS.previewKeyExcerptMax),
  }).join("\n").trimEnd() + "\n";
  const previewLineCount = previewContent.split(/\r?\n/).length;
  const previewCharCount = previewContent.length;
  const lineSavings = Math.max(0, fullLineCount - previewLineCount);
  const charSavings = Math.max(0, fullCharCount - previewCharCount);
  const exceedsThreshold =
    (note?.durableDetails?.length ?? 0) > DURABLE_NOTE_LIMITS.overflowDetailCount ||
    (note?.sourceAnchors?.length ?? 0) > DURABLE_NOTE_LIMITS.overflowSourceAnchorCount ||
    (note?.keyExcerpts?.length ?? 0) > DURABLE_NOTE_LIMITS.overflowKeyExcerptCount ||
    fullLineCount > DURABLE_NOTE_LIMITS.overflowRenderedLineCount ||
    fullCharCount > DURABLE_NOTE_LIMITS.overflowRenderedCharCount;
  const hasMeaningfulSavings =
    lineSavings >= DURABLE_NOTE_LIMITS.overflowMinimumLineSavings ||
    charSavings >= DURABLE_NOTE_LIMITS.overflowMinimumCharSavings;
  return {
    overflow: exceedsThreshold && hasMeaningfulSavings,
    lineCount: fullLineCount,
    charCount: fullCharCount,
    previewLineCount,
    previewCharCount,
    lineSavings,
    charSavings,
  };
}

export function createSidecarRelativePath(sessionId, key) {
  const stableSessionId = slugify(sessionId || "unknown-session", "unknown-session");
  return `memory/archive/extracts/${stableSessionId}/${slugify(key, "note")}.md`;
}

export function renderIndexedNote(note) {
  const overflowState = getDurableNoteOverflow(note);
  const overflow = overflowState.overflow;
  const sidecarPath = overflow ? (note.sidecarPath || createSidecarRelativePath(note.sessionId, note.key)) : null;
  const previewDetails = overflow
    ? (note.durableDetails ?? []).slice(0, DURABLE_NOTE_LIMITS.previewDetailMax)
    : (note.durableDetails ?? []);
  const previewConfigRefs = overflow
    ? (note.configRefs ?? []).slice(0, DURABLE_NOTE_LIMITS.previewConfigRefMax)
    : (note.configRefs ?? []);
  const previewAnchors = overflow
    ? (note.sourceAnchors ?? []).slice(0, DURABLE_NOTE_LIMITS.previewSourceAnchorMax)
    : (note.sourceAnchors ?? []);
  const previewExcerpts = overflow
    ? (note.keyExcerpts ?? []).slice(0, DURABLE_NOTE_LIMITS.previewKeyExcerptMax)
    : (note.keyExcerpts ?? []);

  const effectiveNote = {
    ...note,
    sidecarPath,
  };

  const lines = [
    ...baseFrontmatter(effectiveNote),
    `# ${effectiveNote.title}`,
    "",
    `- Summary: ${effectiveNote.summary}`,
    `- Why it matters: ${effectiveNote.whyItMatters}`,
    "- Evidence:",
    ...(
      effectiveNote.evidence.length > 0
        ? effectiveNote.evidence.map((item) => `  - ${item}`)
        : ["  - Derived from the latest session boundary transcript."]
    ),
    "",
    ...renderListSection("Durable Details", previewDetails),
    ...renderListSection("Config / Commands / Paths", previewConfigRefs),
    ...renderAnchorSection("Source Anchors", previewAnchors, { includeExcerpt: false }),
    ...renderExcerptSection("Key Excerpts", previewExcerpts),
  ];

  if (overflow && sidecarPath) {
    lines.push("## Sidecar Excerpt");
    lines.push(`- See sidecar: ${sidecarPath}`);
    lines.push("");
  }

  return {
    content: `${lines.join("\n").trimEnd()}\n`,
    overflow,
    sidecarPath,
    metrics: overflowState,
  };
}

export function renderSidecarNote(note) {
  const effectiveNote = {
    ...note,
    sidecarPath: note.sidecarPath || createSidecarRelativePath(note.sessionId, note.key),
  };
  const lines = [
    ...baseFrontmatter(effectiveNote),
    `# ${effectiveNote.title} Sidecar`,
    "",
    `- Summary: ${effectiveNote.summary}`,
    "",
    ...renderListSection("Durable Details", effectiveNote.durableDetails),
    ...renderListSection("Config / Commands / Paths", effectiveNote.configRefs),
    ...renderAnchorSection("Full Source Anchors", effectiveNote.sourceAnchors, { includeExcerpt: true }),
    ...renderExcerptSection("Excerpt Blocks", effectiveNote.keyExcerpts),
  ];
  return `${lines.join("\n").trimEnd()}\n`;
}

export function parseIndexedNote(raw, filePath = null, relativePath = null, stats = null) {
  const { frontmatter, body } = parseFrontmatter(raw);
  const titleMatch = body.match(/^#\s+(.+)$/m);
  const summaryMatch = body.match(/^- Summary:\s*(.+)$/m);
  const whyMatch = body.match(/^- Why it matters:\s*(.+)$/m);
  const parsed = {
    filePath,
    relativePath,
    hash: crypto.createHash("sha1").update(raw).digest("hex"),
    size: stats?.size ?? Buffer.byteLength(raw, "utf8"),
    mtimeMs: stats?.mtimeMs ?? null,
    key:
      typeof frontmatter.key === "string" && frontmatter.key.trim()
        ? frontmatter.key.trim()
        : slugify(relativePath || titleMatch?.[1] || "note"),
    type:
      typeof frontmatter.type === "string" && DURABLE_NOTE_TYPES.includes(frontmatter.type.trim())
        ? frontmatter.type.trim()
        : "fact",
    project:
      typeof frontmatter.project === "string" && frontmatter.project.trim()
        ? frontmatter.project.trim()
        : null,
    sessionId:
      typeof frontmatter.sessionId === "string" && frontmatter.sessionId.trim()
        ? frontmatter.sessionId.trim()
        : null,
    createdAt:
      typeof frontmatter.createdAt === "string" && frontmatter.createdAt.trim()
        ? frontmatter.createdAt.trim()
        : nowIso(stats?.mtimeMs),
    updatedAt:
      typeof frontmatter.updatedAt === "string" && frontmatter.updatedAt.trim()
        ? frontmatter.updatedAt.trim()
        : (typeof frontmatter.createdAt === "string" && frontmatter.createdAt.trim()
            ? frontmatter.createdAt.trim()
            : nowIso(stats?.mtimeMs)),
    lastSeenAt:
      typeof frontmatter.lastSeenAt === "string" && frontmatter.lastSeenAt.trim()
        ? frontmatter.lastSeenAt.trim()
        : (typeof frontmatter.updatedAt === "string" && frontmatter.updatedAt.trim()
            ? frontmatter.updatedAt.trim()
            : nowIso(stats?.mtimeMs)),
    sourcePath:
      typeof frontmatter.sourcePath === "string" && frontmatter.sourcePath.trim()
        ? frontmatter.sourcePath.trim()
        : null,
    sourceRange:
      typeof frontmatter.sourceRange === "string" && frontmatter.sourceRange.trim()
        ? frontmatter.sourceRange.trim()
        : null,
    sourceSessions: dedupeStrings(frontmatter.sourceSessions, { maxCount: 32, maxChars: 120 }),
    sidecarPath:
      typeof frontmatter.sidecarPath === "string" && frontmatter.sidecarPath.trim()
        ? frontmatter.sidecarPath.trim()
        : null,
    title: trimNoteText(titleMatch?.[1] ?? "", 120),
    summary: trimNoteText(summaryMatch?.[1] ?? "", 220),
    whyItMatters: trimNoteText(whyMatch?.[1] ?? "", 240),
    evidence: dedupeStrings(bulletListFromLeadLabel(body, "Evidence"), {
      maxCount: DURABLE_NOTE_LIMITS.evidenceMax,
      maxChars: 220,
    }),
    tags: dedupeStrings(frontmatter.tags, { maxCount: DURABLE_NOTE_LIMITS.tagMax, maxChars: 80 }),
    durableDetails: dedupeStrings(bulletListFromSection(body, "Durable Details"), {
      maxCount: DURABLE_NOTE_LIMITS.detailMax,
      maxChars: 320,
    }),
    configRefs: dedupeStrings(bulletListFromSection(body, "Config / Commands / Paths"), {
      maxCount: DURABLE_NOTE_LIMITS.configRefMax,
      maxChars: 280,
    }),
    sourceAnchors: dedupeSourceAnchors([
      ...bulletListFromSection(body, "Source Anchors"),
      ...bulletListFromSection(body, "Full Source Anchors"),
    ]),
    keyExcerpts: dedupeStrings(
      [
        ...bulletListFromSection(body, "Key Excerpts").map(stripWrappingQuotes),
        ...bulletListFromSection(body, "Excerpt Blocks").map(stripWrappingQuotes),
      ],
      {
        maxCount: DURABLE_NOTE_LIMITS.keyExcerptMax,
        maxChars: DURABLE_NOTE_LIMITS.anchorExcerptMaxChars,
      },
    ),
  };
  if (parsed.sourceSessions.length === 0 && parsed.sessionId) {
    parsed.sourceSessions = [parsed.sessionId];
  }
  return parsed;
}

export async function loadIndexedNote(filePath, relativePath = null, { workspaceDir } = {}) {
  const raw = await fs.readFile(filePath, "utf8");
  const stats = await fs.stat(filePath);
  let note = parseIndexedNote(raw, filePath, relativePath, stats);
  if (note.sidecarPath && workspaceDir) {
    const fullSidecarPath = path.isAbsolute(note.sidecarPath)
      ? note.sidecarPath
      : path.join(workspaceDir, note.sidecarPath.replace(/\//g, path.sep));
    try {
      const sidecarRaw = await fs.readFile(fullSidecarPath, "utf8");
      const sidecarStats = await fs.stat(fullSidecarPath);
      const sidecar = parseIndexedNote(sidecarRaw, fullSidecarPath, note.sidecarPath, sidecarStats);
      note = mergeDurableNote(note, {
        ...sidecar,
        title: note.title,
        summary: note.summary,
        whyItMatters: note.whyItMatters,
        evidence: note.evidence,
        sidecarPath: note.sidecarPath,
        filePath: note.filePath,
        relativePath: note.relativePath,
        sessionId: note.sessionId,
        sourcePath: note.sourcePath,
        sourceRange: note.sourceRange,
      });
    } catch {}
  }
  return note;
}
