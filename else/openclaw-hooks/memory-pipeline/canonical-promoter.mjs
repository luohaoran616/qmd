import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import {
  ensurePipelineLayout,
  fileExists,
  logPipeline,
  nowIso,
  readJsonFile,
  releaseStaleLock,
  resolveDefaultConfigPath,
  resolveHookSettings,
  resolvePipelinePaths,
  resolveWorkspaceDir,
  slugify,
  writeJsonFile,
  writeTextFileIfChanged,
} from "./common.mjs";

const MANAGED_START = "<!-- managed:start -->";
const MANAGED_END = "<!-- managed:end -->";

function parseArgs(argv) {
  const result = {
    checkOnce: false,
    promoteOnce: false,
    configFile: resolveDefaultConfigPath(process.env),
    workspace: undefined,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--check-once") {
      result.checkOnce = true;
      continue;
    }
    if (token === "--promote-once") {
      result.promoteOnce = true;
      continue;
    }
    if (token === "--config-file") {
      result.configFile = argv[index + 1];
      index += 1;
      continue;
    }
    if (token === "--workspace") {
      result.workspace = argv[index + 1];
      index += 1;
    }
  }
  return result;
}

function extractJson(text) {
  const trimmed = String(text ?? "").trim();
  if (!trimmed) {
    throw new Error("empty model output");
  }
  const fenceMatch = trimmed.match(/```(?:json)?\s*([\s\S]*?)```/i);
  const candidate = fenceMatch?.[1]?.trim() || trimmed;
  const firstBrace = candidate.indexOf("{");
  const lastBrace = candidate.lastIndexOf("}");
  if (firstBrace === -1 || lastBrace === -1 || lastBrace <= firstBrace) {
    throw new Error("no json object found");
  }
  return JSON.parse(candidate.slice(firstBrace, lastBrace + 1));
}

function clamp(value, min, max) {
  if (!Number.isFinite(value)) {
    return min;
  }
  return Math.min(max, Math.max(min, value));
}

function limitLines(text, maxLines) {
  return String(text ?? "")
    .split(/\r?\n/)
    .map((line) => line.trimEnd())
    .slice(0, maxLines)
    .join("\n")
    .trim();
}

function trimInline(text, maxChars = 220) {
  const normalized = String(text ?? "").replace(/\s+/g, " ").trim();
  if (normalized.length <= maxChars) {
    return normalized;
  }
  return `${normalized.slice(0, maxChars - 1).trimEnd()}…`;
}

function parseFrontmatter(raw) {
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

function parseIndexedNote(raw, filePath, relativePath, stats) {
  const { frontmatter, body } = parseFrontmatter(raw);
  const summaryMatch = body.match(/^- Summary:\s*(.+)$/m);
  const whyMatch = body.match(/^- Why it matters:\s*(.+)$/m);
  const evidenceSection = body.match(/^- Evidence:\s*\n([\s\S]*?)(?:\n{2,}|$)/m);
  const evidence = evidenceSection
    ? evidenceSection[1]
        .split(/\r?\n/)
        .map((line) => /^\s*-\s*(.+)$/.exec(line)?.[1]?.trim() ?? "")
        .filter(Boolean)
    : [];

  const titleMatch = body.match(/^#\s+(.+)$/m);
  const createdAt =
    typeof frontmatter.createdAt === "string" && frontmatter.createdAt.trim()
      ? frontmatter.createdAt.trim()
      : nowIso(stats.mtimeMs);

  return {
    filePath,
    relativePath,
    hash: crypto.createHash("sha1").update(raw).digest("hex"),
    size: stats.size,
    mtimeMs: stats.mtimeMs,
    key: typeof frontmatter.key === "string" && frontmatter.key.trim() ? frontmatter.key.trim() : slugify(relativePath),
    type: typeof frontmatter.type === "string" && frontmatter.type.trim() ? frontmatter.type.trim() : "fact",
    project:
      typeof frontmatter.project === "string" && frontmatter.project.trim()
        ? frontmatter.project.trim()
        : null,
    title: titleMatch?.[1]?.trim() || slugify(relativePath),
    summary: summaryMatch?.[1]?.trim() || "",
    whyItMatters: whyMatch?.[1]?.trim() || "",
    evidence,
    tags: Array.isArray(frontmatter.tags) ? frontmatter.tags.map(String).filter(Boolean) : [],
    createdAt,
  };
}

async function scanIndexedNotes(indexedDir) {
  const notes = [];
  async function visit(dir) {
    let entries = [];
    try {
      entries = await fs.readdir(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        await visit(fullPath);
        continue;
      }
      if (!entry.isFile() || !entry.name.endsWith(".md")) {
        continue;
      }
      const raw = await fs.readFile(fullPath, "utf8");
      const stats = await fs.stat(fullPath);
      notes.push(
        parseIndexedNote(raw, fullPath, path.relative(indexedDir, fullPath).replace(/\\/g, "/"), stats),
      );
    }
  }
  await visit(indexedDir);
  return notes.sort((left, right) => left.mtimeMs - right.mtimeMs);
}

async function loadCanonicalCursor(paths) {
  return (
    (await readJsonFile(paths.canonicalCursorFile, {
      version: 1,
      processed: {},
      lastRunAt: null,
    })) ?? {
      version: 1,
      processed: {},
      lastRunAt: null,
    }
  );
}

async function loadCanonicalStore(paths) {
  return (
    (await readJsonFile(paths.canonicalStoreFile, {
      version: 1,
      updatedAt: null,
      items: [],
    })) ?? {
      version: 1,
      updatedAt: null,
      items: [],
    }
  );
}

function collectPendingNotes(notes, cursor) {
  const pending = notes.filter((note) => cursor.processed?.[note.relativePath]?.hash !== note.hash);
  const pendingBytes = pending.reduce((sum, note) => sum + note.size, 0);
  const pendingCount = pending.length;
  const oldestAgeMs =
    pending.length > 0 ? Math.max(0, Date.now() - Math.min(...pending.map((note) => note.mtimeMs))) : 0;
  return {
    pending,
    stats: {
      pendingBytes,
      pendingCount,
      oldestAgeMs,
    },
  };
}

function shouldPromote(settings, stats) {
  if (stats.pendingCount === 0) {
    return { shouldPromote: false, reason: "no-pending-notes" };
  }
  if (stats.pendingBytes >= settings.promoter.trigger.pendingBytes) {
    return { shouldPromote: true, reason: "pending-bytes-threshold" };
  }
  if (stats.oldestAgeMs >= settings.promoter.trigger.maxAgeMs) {
    return { shouldPromote: true, reason: "pending-age-threshold" };
  }
  return { shouldPromote: false, reason: "below-threshold" };
}

async function callModel(settings, messages) {
  const apiKey = process.env[settings.promoter.model.apiKeyEnv];
  if (!apiKey) {
    throw new Error(`Missing promoter API key env: ${settings.promoter.model.apiKeyEnv}`);
  }

  const response = await fetch(`${settings.promoter.model.baseUrl.replace(/\/$/, "")}/chat/completions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: settings.promoter.model.model,
      temperature: settings.promoter.model.temperature,
      max_tokens: settings.promoter.model.maxOutputTokens,
      messages,
    }),
    signal: AbortSignal.timeout(settings.promoter.model.timeoutMs),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`promoter model failed (${response.status}): ${body.slice(0, 400)}`);
  }

  const payload = await response.json();
  const content = payload?.choices?.[0]?.message?.content;
  if (typeof content === "string") {
    return content;
  }
  if (Array.isArray(content)) {
    return content
      .map((item) => (item?.type === "text" ? item.text : ""))
      .filter(Boolean)
      .join("\n");
  }
  throw new Error("promoter model returned no content");
}

function summarizeActiveItems(store, target, limit = 40) {
  return store.items
    .filter((item) => item.target === target && item.status === "active")
    .slice(0, limit)
    .map((item) => ({
      key: item.key,
      title: item.title,
      summary: item.summary,
      evidenceCount: item.evidenceCount,
      confidence: item.confidence,
    }));
}

async function loadCurrentDocs(paths) {
  const entries = await Promise.all([
    fs.readFile(paths.userFile, "utf8").catch(() => ""),
    fs.readFile(paths.memoryRootFile, "utf8").catch(() => ""),
    fs.readFile(paths.soulFile, "utf8").catch(() => ""),
    fs.readFile(paths.identityFile, "utf8").catch(() => ""),
  ]);
  return {
    user: limitLines(entries[0], 120),
    memory: limitLines(entries[1], 180),
    soul: limitLines(entries[2], 120),
    identity: limitLines(entries[3], 120),
  };
}

function chunkNotes(notes, maxInputNotes) {
  const chunks = [];
  for (let index = 0; index < notes.length; index += maxInputNotes) {
    chunks.push(notes.slice(index, index + maxInputNotes));
  }
  return chunks;
}

async function classifyPromotions(settings, pendingNotes, docs, store) {
  const chunks = chunkNotes(pendingNotes, settings.promoter.maxInputNotes);
  const items = [];
  for (const chunk of chunks) {
    const raw = await callModel(settings, [
      {
        role: "system",
        content: [
          "You are a canonical memory promoter for an OpenClaw workspace.",
          "Return JSON only.",
          "Decide which indexed durable notes should be promoted into canonical files.",
          "Allowed targets: user, memory, soul, identity.",
          "Allowed statuses: active, proposal, conflict.",
          "Rules:",
          "- user: only long-lived user preferences, work style, environment constraints, long-term interests, or communication preferences.",
          "- memory: only global stable facts, cross-session project background, stable paths/URLs/IDs/providers/config, long-lived workflows, or durable decisions.",
          "- soul / identity are proposal-only. Use them only if the user explicitly asked to change assistant style, boundaries, tone, name, role, or self-introduction.",
          "- If a note is too transient or not canonical, omit it.",
          "- Reuse canonicalKey for semantically equivalent items.",
          "- Prefer proposal or conflict over silently changing uncertain canonical facts.",
          "JSON shape:",
          '{"items":[{"sourceKey":"note-key","target":"user","status":"active","canonicalKey":"stable-kebab-key","title":"Short title","summary":"One sentence","evidence":["..."],"confidence":0.84,"reason":"short explanation"}]}',
        ].join(" "),
      },
      {
        role: "user",
        content: JSON.stringify(
          {
            currentDocs: docs,
            currentCanonicals: {
              user: summarizeActiveItems(store, "user"),
              memory: summarizeActiveItems(store, "memory"),
            },
            pendingNotes: chunk.map((note) => ({
              sourceKey: note.key,
              type: note.type,
              title: note.title,
              summary: note.summary,
              whyItMatters: note.whyItMatters,
              evidence: note.evidence,
              project: note.project,
              tags: note.tags,
              relativePath: note.relativePath,
            })),
          },
          null,
          2,
        ),
      },
    ]);
    const parsed = extractJson(raw);
    if (Array.isArray(parsed.items)) {
      items.push(...parsed.items);
    }
  }
  return items;
}

function sanitizeCandidate(raw, noteMap, settings) {
  const sourceKey = typeof raw?.sourceKey === "string" ? raw.sourceKey.trim() : "";
  const note = noteMap.get(sourceKey);
  if (!note) {
    return null;
  }

  const target = typeof raw?.target === "string" ? raw.target.trim().toLowerCase() : "";
  if (!["user", "memory", "soul", "identity"].includes(target)) {
    return null;
  }

  let status = typeof raw?.status === "string" ? raw.status.trim().toLowerCase() : "active";
  if (!["active", "proposal", "conflict"].includes(status)) {
    status = target === "user" || target === "memory" ? "active" : "proposal";
  }
  if (target === "soul" || target === "identity") {
    status = "proposal";
  }
  if (!(settings.promoter.targets.auto.includes(target) || settings.promoter.targets.proposalOnly.includes(target))) {
    return null;
  }

  const canonicalKey = slugify(raw?.canonicalKey || sourceKey || note.title || note.summary);
  const title = trimInline(raw?.title || note.title, 80);
  const summary = trimInline(raw?.summary || note.summary, 220);
  if (!title || !summary) {
    return null;
  }

  const evidence = Array.isArray(raw?.evidence) ? raw.evidence.map(String).filter(Boolean).slice(0, 4) : note.evidence;
  const confidence = clamp(Number(raw?.confidence ?? 0.65), 0, 1);

  return {
    target,
    status,
    key: canonicalKey,
    title,
    summary,
    evidence: evidence.length > 0 ? evidence : note.evidence,
    confidence,
    reason: trimInline(raw?.reason || note.whyItMatters || "Derived from indexed durable memory.", 220),
    sourceKey,
    sourceFiles: [note.relativePath],
    firstSeenAt: note.createdAt || nowIso(note.mtimeMs),
    lastSeenAt: nowIso(),
  };
}

function mergeStoreItem(existing, candidate) {
  const sourceFiles = [...new Set([...(existing.sourceFiles ?? []), ...(candidate.sourceFiles ?? [])])];
  const evidence = [...new Set([...(existing.evidence ?? []), ...(candidate.evidence ?? [])])].slice(0, 6);
  return {
    ...existing,
    title: candidate.confidence >= (existing.confidence ?? 0) ? candidate.title : existing.title,
    summary: candidate.confidence >= (existing.confidence ?? 0) ? candidate.summary : existing.summary,
    reason: candidate.reason || existing.reason,
    evidence,
    evidenceCount: Math.max(existing.evidenceCount ?? sourceFiles.length, sourceFiles.length),
    confidence: Math.max(existing.confidence ?? 0, candidate.confidence ?? 0),
    sourceFiles,
    lastSeenAt: candidate.lastSeenAt,
  };
}

function upsertCandidate(store, candidate) {
  if (candidate.status === "active") {
    const existing = store.items.find(
      (item) => item.target === candidate.target && item.key === candidate.key && item.status === "active",
    );
    if (existing) {
      Object.assign(existing, mergeStoreItem(existing, candidate));
      return existing;
    }
  } else {
    const existing = store.items.find(
      (item) =>
        item.target === candidate.target &&
        item.key === candidate.key &&
        item.status === candidate.status,
    );
    if (existing) {
      Object.assign(existing, mergeStoreItem(existing, candidate));
      return existing;
    }
  }

  const item = {
    id: crypto
      .createHash("sha1")
      .update(`${candidate.target}:${candidate.status}:${candidate.key}`)
      .digest("hex")
      .slice(0, 16),
    target: candidate.target,
    key: candidate.key,
    title: candidate.title,
    summary: candidate.summary,
    evidence: candidate.evidence,
    evidenceCount: candidate.sourceFiles.length,
    confidence: candidate.confidence,
    firstSeenAt: candidate.firstSeenAt,
    lastSeenAt: candidate.lastSeenAt,
    sourceFiles: candidate.sourceFiles,
    status: candidate.status,
    reason: candidate.reason,
  };
  store.items.push(item);
  return item;
}

function scoreCanonicalItem(item) {
  const recencyDays = Math.max(0, (Date.now() - Date.parse(item.lastSeenAt ?? 0)) / (24 * 60 * 60 * 1000));
  const recencyScore = Math.max(0, 30 - recencyDays) / 30;
  return (item.confidence ?? 0) * 100 + Math.min(item.evidenceCount ?? 1, 5) * 10 + recencyScore * 5;
}

function selectRenderableItems(store, target, maxLines) {
  const items = store.items
    .filter((item) => item.target === target && item.status === "active")
    .sort((left, right) => scoreCanonicalItem(right) - scoreCanonicalItem(left));
  const maxItems = Math.max(0, maxLines - 4);
  return items.slice(0, maxItems);
}

function renderManagedBlock(title, items) {
  const lines = [
    MANAGED_START,
    `## ${title}`,
    `_Auto-promoted canonical memory. Edit outside this block for manual notes._`,
    ...(items.length > 0
      ? items.map((item) => `- ${trimInline(item.summary, 240)}`)
      : ["- No promoted canonical notes yet."]),
    MANAGED_END,
  ];
  return `${lines.join("\n")}\n`;
}

function upsertManagedBlock(content, block) {
  const current = String(content ?? "");
  const startIndex = current.indexOf(MANAGED_START);
  const endIndex = current.indexOf(MANAGED_END);
  if (startIndex !== -1 && endIndex !== -1 && endIndex > startIndex) {
    const before = current.slice(0, startIndex).trimEnd();
    const after = current.slice(endIndex + MANAGED_END.length).trimStart();
    return `${before}\n\n${block}${after ? `\n${after}` : ""}`.replace(/\n{3,}/g, "\n\n").trimEnd() + "\n";
  }
  const trimmed = current.trimEnd();
  return `${trimmed}${trimmed ? "\n\n" : ""}${block}`;
}

async function writeManagedDoc(filePath, title, items) {
  const existing = await fs.readFile(filePath, "utf8").catch(() => "");
  const next = upsertManagedBlock(existing, renderManagedBlock(title, items));
  return writeTextFileIfChanged(filePath, next);
}

async function writeProposal(paths, item, settings) {
  const targetDir =
    item.target === "soul" || item.target === "identity"
      ? path.join(paths.proposalsDir, item.target)
      : path.join(paths.proposalsDir, "conflicts");
  const filePath = path.join(targetDir, `${slugify(item.key)}.md`);
  const body = [
    "---",
    `target: ${item.target}`,
    `status: ${item.status}`,
    `canonicalKey: ${item.key}`,
    `updatedAt: ${item.lastSeenAt}`,
    "sourceFiles:",
    ...(item.sourceFiles.length > 0 ? item.sourceFiles.map((source) => `  - ${source}`) : ["  - unknown"]),
    "---",
    "",
    `# Proposal: ${item.title}`,
    "",
    `- Summary: ${item.summary}`,
    `- Reason: ${trimInline(item.reason, 240)}`,
    `- Confidence: ${(item.confidence ?? 0).toFixed(2)}`,
    "- Evidence:",
    ...(item.evidence.length > 0 ? item.evidence.map((entry) => `  - ${entry}`) : ["  - No evidence captured."]),
    "",
  ].join("\n");
  return writeTextFileIfChanged(filePath, `${limitLines(body, settings.promoter.budgets.proposalMaxLines)}\n`);
}

export async function assessPendingPromotion({ settings, paths }) {
  const notes = await scanIndexedNotes(paths.indexedDir);
  const cursor = await loadCanonicalCursor(paths);
  const { pending, stats } = collectPendingNotes(notes, cursor);
  const decision = shouldPromote(settings, stats);
  return { notes, cursor, pending, stats, ...decision };
}

export async function maybePromoteCanonical({ settings, paths, sourceEvent = "manual", force = false }) {
  if (!settings.promoter.enabled) {
    return {
      ran: false,
      changed: false,
      qmdRelevantChange: false,
      reason: "promoter-disabled",
      stats: { pendingBytes: 0, pendingCount: 0, oldestAgeMs: 0 },
    };
  }

  const assessment = await assessPendingPromotion({ settings, paths });
  const shouldRun = force || assessment.shouldPromote;
  if (!shouldRun) {
    await appendPromotionLog(paths, {
      sourceEvent,
      action: "check",
      result: "skipped",
      reason: assessment.reason,
      stats: assessment.stats,
    });
    return {
      ran: false,
      changed: false,
      qmdRelevantChange: false,
      reason: assessment.reason,
      stats: assessment.stats,
    };
  }

  const store = await loadCanonicalStore(paths);
  const docs = await loadCurrentDocs(paths);
  const noteMap = new Map(assessment.pending.map((note) => [note.key, note]));
  const rawCandidates = await classifyPromotions(settings, assessment.pending, docs, store);
  const candidates = rawCandidates
    .map((candidate) => sanitizeCandidate(candidate, noteMap, settings))
    .filter(Boolean);

  const proposalWrites = [];
  for (const candidate of candidates) {
    const item = upsertCandidate(store, candidate);
    if (candidate.status !== "active" || candidate.target === "soul" || candidate.target === "identity") {
      proposalWrites.push(writeProposal(paths, item, settings));
    }
  }
  const proposalResults = await Promise.all(proposalWrites);

  const userItems = selectRenderableItems(store, "user", settings.promoter.budgets.userMaxLines);
  const memoryItems = selectRenderableItems(store, "memory", settings.promoter.budgets.memoryMaxLines);
  const userChanged = await writeManagedDoc(paths.userFile, "Managed User Memory", userItems);
  const memoryChanged = await writeManagedDoc(paths.memoryRootFile, "Managed Canonical Memory", memoryItems);

  store.updatedAt = nowIso();
  await writeJsonFile(paths.canonicalStoreFile, store);

  const nextCursor = assessment.cursor ?? { version: 1, processed: {} };
  for (const note of assessment.pending) {
    nextCursor.processed[note.relativePath] = {
      hash: note.hash,
      size: note.size,
      mtimeMs: note.mtimeMs,
      processedAt: nowIso(),
    };
  }
  nextCursor.lastRunAt = nowIso();
  await writeJsonFile(paths.canonicalCursorFile, nextCursor);

  await appendPromotionLog(paths, {
    sourceEvent,
    action: "promote",
    result: "ok",
    reason: force ? "forced" : assessment.reason,
    stats: assessment.stats,
    promoted: candidates.filter((item) => item.status === "active").length,
    proposals: candidates.filter((item) => item.status !== "active").length,
    userChanged,
    memoryChanged,
  });

  return {
    ran: true,
    changed: userChanged || memoryChanged || proposalResults.some(Boolean),
    qmdRelevantChange: memoryChanged,
    reason: force ? "forced" : assessment.reason,
    stats: assessment.stats,
    promoted: candidates.length,
  };
}

async function appendPromotionLog(paths, payload) {
  await fs.mkdir(path.dirname(paths.promotionLogFile), { recursive: true });
  await fs.appendFile(
    paths.promotionLogFile,
    `${JSON.stringify({
      timestamp: nowIso(),
      ...payload,
    })}\n`,
    "utf8",
  );
}

async function acquireLock(paths, settings) {
  await releaseStaleLock(paths.lockFile, settings.lockTtlMs);
  if (await fileExists(paths.lockFile)) {
    return false;
  }
  await writeJsonFile(paths.lockFile, {
    pid: process.pid,
    startedAt: nowIso(),
    hostname: process.env.COMPUTERNAME ?? process.env.HOSTNAME ?? null,
  });
  return true;
}

async function releaseLock(paths) {
  await fs.rm(paths.lockFile, { force: true });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const cfg = (await readJsonFile(args.configFile, {})) ?? {};
  const settings = resolveHookSettings(cfg);
  const workspaceDir = resolveWorkspaceDir({ cfg, override: args.workspace });
  const paths = resolvePipelinePaths(workspaceDir);
  await ensurePipelineLayout(paths);

  if (args.checkOnce && !args.promoteOnce) {
    const assessment = await assessPendingPromotion({ settings, paths });
    process.stdout.write(
      `${JSON.stringify(
        {
          shouldPromote: assessment.shouldPromote,
          reason: assessment.reason,
          ...assessment.stats,
        },
        null,
        2,
      )}\n`,
    );
    return;
  }

  const locked = await acquireLock(paths, settings);
  if (!locked) {
    process.stdout.write(`${JSON.stringify({ status: "busy" }, null, 2)}\n`);
    return;
  }

  try {
    const result = await maybePromoteCanonical({
      settings,
      paths,
      sourceEvent: "manual",
      force: false,
    });
    await logPipeline(paths, "info", "canonical promoter run completed", result);
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  } finally {
    await releaseLock(paths);
  }
}

if (import.meta.url === `file://${process.argv[1]?.replace(/\\/g, "/")}` || process.argv[1]?.endsWith("canonical-promoter.mjs")) {
  main().catch(async (error) => {
    const args = parseArgs(process.argv.slice(2));
    const cfg = (await readJsonFile(args.configFile, {})) ?? {};
    const workspaceDir = resolveWorkspaceDir({ cfg, override: args.workspace });
    const paths = resolvePipelinePaths(workspaceDir);
    await ensurePipelineLayout(paths);
    await logPipeline(paths, "error", "canonical promoter crashed", {
      error: error instanceof Error ? error.message : String(error),
    });
    await releaseLock(paths);
    process.exitCode = 1;
  });
}
