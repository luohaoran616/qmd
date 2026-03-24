import fs from "node:fs/promises";
import path from "node:path";
import { maybePromoteCanonical } from "./canonical-promoter.mjs";
import {
  cleanupVisibleText,
  createSidecarRelativePath,
  dedupeStrings,
  ensurePipelineLayout,
  extractTextContent,
  fileExists,
  loadIndexedNote,
  logPipeline,
  mergeDurableNote,
  normalizeExtractedNote,
  nowIso,
  parseSessionKey,
  pickDefined,
  readJsonFile,
  readJsonl,
  renderIndexedNote,
  renderSidecarNote,
  releaseStaleLock,
  resolveDefaultConfigPath,
  resolveHookSettings,
  resolvePipelinePaths,
  resolveWorkspaceDir,
  shouldIgnoreAssistantEntry,
  shouldIgnoreUserText,
  shortDatePrefix,
  slugify,
  writeJsonFile,
  writeTextFileIfChanged,
} from "./common.mjs";

function parseArgs(argv) {
  const result = {
    drainOnce: false,
    configFile: resolveDefaultConfigPath(process.env),
    workspace: undefined,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--drain-once") {
      result.drainOnce = true;
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

function toErrorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

const SENSITIVE_VALUE_PATTERN =
  /github_pat_[A-Za-z0-9_]+|\b(?:sk|rk|pk)-[A-Za-z0-9._-]{12,}\b|\bghp_[A-Za-z0-9]{20,}\b|\bBearer\s+[A-Za-z0-9._=-]{12,}\b/gi;

export function redactSensitiveValues(text) {
  return String(text ?? "").replace(SENSITIVE_VALUE_PATTERN, "[REDACTED_SECRET]");
}

export function sanitizeExtractedNotePayload(note) {
  if (!note || typeof note !== "object") {
    return note;
  }
  const sanitizeList = (value) =>
    Array.isArray(value)
      ? value
          .map((item) => redactSensitiveValues(item))
          .map((item) => item.trim())
          .filter(Boolean)
      : value;

  return {
    ...note,
    title: redactSensitiveValues(note.title),
    summary: redactSensitiveValues(note.summary),
    whyItMatters: redactSensitiveValues(note.whyItMatters),
    project: typeof note.project === "string" ? redactSensitiveValues(note.project) : note.project,
    evidence: sanitizeList(note.evidence),
    durableDetails: sanitizeList(note.durableDetails),
    configRefs: sanitizeList(note.configRefs),
    keyExcerpts: sanitizeList(note.keyExcerpts),
    sourceAnchors: Array.isArray(note.sourceAnchors)
      ? note.sourceAnchors.map((anchor) => ({
          ...anchor,
          excerpt: redactSensitiveValues(anchor?.excerpt),
        }))
      : note.sourceAnchors,
  };
}

async function repairMalformedJson(settings, raw, error) {
  const system = [
    "You repair malformed JSON produced by another model.",
    "Return JSON only.",
    'Return an object with shape {"notes":[...]} exactly.',
    "Preserve valid content when possible, but drop any field or note you cannot repair safely.",
    'If nothing can be repaired, return {"notes":[]}.',
  ].join(" ");
  const user = [
    "Repair this malformed JSON into valid JSON.",
    `Parse error: ${toErrorMessage(error)}`,
    "",
    "Malformed JSON:",
    "```json",
    String(raw ?? "").trim(),
    "```",
  ].join("\n");
  return callModel(
    settings,
    [
      { role: "system", content: system },
      { role: "user", content: user },
    ],
    { temperature: 0 },
  );
}

async function extractJsonWithRepair(settings, raw) {
  try {
    return extractJson(raw);
  } catch (error) {
    const repairedRaw = await repairMalformedJson(settings, raw, error);
    try {
      return extractJson(repairedRaw);
    } catch (repairError) {
      throw new Error(
        `distiller JSON parse failed after repair: ${toErrorMessage(error)} | repair: ${toErrorMessage(repairError)}`,
      );
    }
  }
}

async function readConfig(configFile) {
  return readJsonFile(configFile, {});
}

async function acquireLock(paths, settings) {
  await releaseStaleLock(paths.lockFile, settings.lockTtlMs);
  try {
    await fs.access(paths.lockFile);
    return false;
  } catch {
    await writeJsonFile(paths.lockFile, {
      pid: process.pid,
      startedAt: nowIso(),
      hostname: process.env.COMPUTERNAME ?? process.env.HOSTNAME ?? null,
    });
    return true;
  }
}

async function releaseLock(paths) {
  await fs.rm(paths.lockFile, { force: true });
}

function collapseJobs(records) {
  const perSession = new Map();
  let canonicalJob = null;
  for (const record of records) {
    if (!record || typeof record !== "object") {
      continue;
    }
    if (record.kind === "check_or_promote_canonical") {
      const existingTime = Date.parse(canonicalJob?.createdAt ?? "") || 0;
      const nextTime = Date.parse(record.createdAt ?? "") || 0;
      if (!canonicalJob || nextTime >= existingTime) {
        canonicalJob = record;
      }
      continue;
    }
    const sessionKey = record.sessionKey ?? "unknown";
    const existing = perSession.get(sessionKey);
    const priority = record.kind === "finalize_session" ? 2 : 1;
    const existingPriority = existing?.kind === "finalize_session" ? 2 : existing ? 1 : 0;
    if (!existing || priority > existingPriority) {
      perSession.set(sessionKey, record);
      continue;
    }
    if (priority === existingPriority) {
      const existingTime = Date.parse(existing.createdAt ?? "") || 0;
      const nextTime = Date.parse(record.createdAt ?? "") || 0;
      if (nextTime >= existingTime) {
        perSession.set(sessionKey, { ...existing, ...record });
      }
    }
  }

  const ordered = [...perSession.values()].sort((left, right) => {
    const leftPriority = left.kind === "finalize_session" ? 0 : 1;
    const rightPriority = right.kind === "finalize_session" ? 0 : 1;
    if (leftPriority !== rightPriority) {
      return leftPriority - rightPriority;
    }
    return (Date.parse(left.createdAt ?? "") || 0) - (Date.parse(right.createdAt ?? "") || 0);
  });
  if (canonicalJob) {
    ordered.push(canonicalJob);
  }
  return ordered;
}

async function resolveSessionFile(job, openclawHome) {
  const sessionId = typeof job.sessionId === "string" ? job.sessionId : "";
  const { agentId } = parseSessionKey(job.sessionKey ?? "");
  const sessionsDir = path.join(openclawHome, "agents", agentId, "sessions");
  const candidate = typeof job.sessionFile === "string" ? job.sessionFile : null;

  const tryExistingPath = async (target) => {
    if (!target) {
      return null;
    }
    if (await fileExists(target)) {
      return target;
    }
    const dir = path.dirname(target);
    const base = path.basename(target);
    try {
      const names = await fs.readdir(dir);
      const resetMatches = names
        .filter((name) => name === base || name.startsWith(`${base}.reset.`))
        .sort()
        .reverse();
      if (resetMatches.length > 0) {
        return path.join(dir, resetMatches[0]);
      }
    } catch {}
    return null;
  };

  const direct = await tryExistingPath(candidate);
  if (direct) {
    return direct;
  }

  try {
    const sessionStore = await readJsonFile(path.join(sessionsDir, "sessions.json"), {});
    const storeEntry = sessionStore?.[job.sessionKey];
    const fromStore = await tryExistingPath(storeEntry?.sessionFile);
    if (fromStore) {
      return fromStore;
    }
  } catch {}

  try {
    const names = await fs.readdir(sessionsDir);
    const exact = sessionId ? names.find((name) => name === `${sessionId}.jsonl`) : null;
    if (exact) {
      return path.join(sessionsDir, exact);
    }
    const reset = sessionId
      ? names
          .filter((name) => name.startsWith(`${sessionId}.jsonl.reset.`))
          .sort()
          .reverse()[0]
      : null;
    if (reset) {
      return path.join(sessionsDir, reset);
    }
    const topicVariant = sessionId
      ? names
          .filter(
            (name) =>
              name.startsWith(`${sessionId}-topic-`) &&
              name.endsWith(".jsonl") &&
              !name.includes(".reset."),
          )
          .sort()
          .reverse()[0]
      : null;
    if (topicVariant) {
      return path.join(sessionsDir, topicVariant);
    }
  } catch {}

  return null;
}

function isVisibleTurnEntry(entry) {
  return entry?.type === "message" && entry?.message && typeof entry.message === "object";
}

export async function readTranscriptTurns(sessionFile) {
  const raw = await fs.readFile(sessionFile, "utf8");
  const lines = raw.split(/\r?\n/).filter(Boolean);
  const turns = [];
  let transcriptSessionId = null;

  for (let lineIndex = 0; lineIndex < lines.length; lineIndex += 1) {
    const line = lines[lineIndex];
    let entry;
    try {
      entry = JSON.parse(line);
    } catch {
      continue;
    }

    if (entry?.type === "session" && typeof entry.id === "string") {
      transcriptSessionId = entry.id;
      continue;
    }

    if (!isVisibleTurnEntry(entry)) {
      continue;
    }

    const role = entry.message.role;
    if (role !== "user" && role !== "assistant") {
      continue;
    }

    const visibleText = cleanupVisibleText(extractTextContent(entry.message.content));
    if (role === "user") {
      if (shouldIgnoreUserText(visibleText)) {
        continue;
      }
    } else if (shouldIgnoreAssistantEntry(entry, visibleText)) {
      continue;
    }

    if (!visibleText) {
      continue;
    }

    turns.push({
      id: entry.id ?? `${role}-${turns.length + 1}`,
      role,
      text: visibleText,
      timestamp: entry.timestamp ?? entry.message.timestamp ?? null,
      turnNumber: turns.length + 1,
      jsonlLine: lineIndex + 1,
    });
  }

  return { sessionId: transcriptSessionId, turns };
}

function getDeltaTurns(turns, cursorEntry) {
  if (!cursorEntry?.lastTurnId) {
    return turns;
  }
  const lastIndex = turns.findIndex((turn) => turn.id === cursorEntry.lastTurnId);
  if (lastIndex === -1) {
    return turns;
  }
  return turns.slice(lastIndex + 1);
}

export function formatTurns(turns) {
  return turns
    .map((turn, index) => {
      const label = `T${String(turn.turnNumber ?? index + 1).padStart(4, "0")}`;
      const line = turn.jsonlLine ? ` L${turn.jsonlLine}` : "";
      const id = turn.id ? ` id=${turn.id}` : "";
      const time = turn.timestamp ? ` ${turn.timestamp}` : "";
      return `${label}${line}${id}${time} ${turn.role}: ${turn.text}`;
    })
    .join("\n\n");
}

export function chunkTurns(turns, maxChars) {
  const chunks = [];
  let current = [];
  let currentChars = 0;
  for (const turn of turns) {
    const serialized = `${turn.role}: ${turn.text}\n\n`;
    if (current.length > 0 && currentChars + serialized.length > maxChars) {
      chunks.push(current);
      current = [];
      currentChars = 0;
    }
    current.push(turn);
    currentChars += serialized.length;
  }
  if (current.length > 0) {
    chunks.push(current);
  }
  return chunks;
}

function formatTurnRange(turns) {
  if (!Array.isArray(turns) || turns.length === 0) {
    return "turns 0-0";
  }
  const first = turns[0];
  const last = turns[turns.length - 1];
  return `turns ${first.turnNumber ?? 1}-${last.turnNumber ?? first.turnNumber ?? 1}`;
}

export function aggregateCandidateNotes(notes, context = {}) {
  const aggregated = new Map();
  for (const note of notes) {
    if (!note?.key) {
      continue;
    }
    const existing = aggregated.get(note.key);
    aggregated.set(
      note.key,
      existing
        ? mergeDurableNote(existing, {
            ...note,
            updatedAt: context.updatedAt ?? note.updatedAt ?? nowIso(),
            lastSeenAt: context.updatedAt ?? note.lastSeenAt ?? nowIso(),
          })
        : {
            ...note,
            updatedAt: context.updatedAt ?? note.updatedAt ?? nowIso(),
            lastSeenAt: context.updatedAt ?? note.lastSeenAt ?? nowIso(),
          },
    );
  }
  return [...aggregated.values()];
}

async function callModel(settings, messages, overrides = {}) {
  const apiKey = process.env[settings.distiller.apiKeyEnv];
  if (!apiKey) {
    throw new Error(`Missing distiller API key env: ${settings.distiller.apiKeyEnv}`);
  }

  const response = await fetch(`${settings.distiller.baseUrl.replace(/\/$/, "")}/chat/completions`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: settings.distiller.model,
      temperature:
        typeof overrides.temperature === "number"
          ? overrides.temperature
          : settings.distiller.temperature,
      max_tokens:
        typeof overrides.maxOutputTokens === "number" && overrides.maxOutputTokens > 0
          ? Math.floor(overrides.maxOutputTokens)
          : settings.distiller.maxOutputTokens,
      messages,
    }),
    signal: AbortSignal.timeout(settings.distiller.timeoutMs),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`distiller model failed (${response.status}): ${body.slice(0, 400)}`);
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
  throw new Error("distiller model returned no content");
}

export async function extractNotes(settings, turns) {
  const transcript = formatTurns(turns);
  const system = [
    "You are a durable-memory distiller for OpenClaw.",
    "Return JSON only.",
    "Extract only long-lived notes worth indexing for future recall.",
    "Allowed note types: preference, decision, procedure, project, fact.",
    "Reject small talk, tentative brainstorming, unresolved guesses, and purely short-term context.",
    "Each note must include: type, key, title, summary, whyItMatters, evidence (1-6 strings), project (string or null), tags (0-5 strings).",
    "Also include durableDetails (0-8 strings), configRefs (0-6 strings), sourceAnchors (1-4 objects), keyExcerpts (0-4 strings).",
    "Use exact source anchor coordinates from the transcript labels when available.",
    "Each source anchor object must use: turnStart, turnEnd, messageIds, jsonlLineStart, jsonlLineEnd, excerpt.",
    "Make key stable, lowercase, and kebab-case.",
  ].join(" ");
  const user = [
    "Transcript chunk:",
    transcript,
    "",
    "Return a JSON object with this shape:",
    '{"notes":[{"type":"decision","key":"stable-kebab-key","title":"Short title","summary":"One or two sentences","whyItMatters":"One sentence","evidence":["..."],"project":null,"tags":["tag"],"durableDetails":["..."],"configRefs":["..."],"sourceAnchors":[{"turnStart":11,"turnEnd":18,"messageIds":["abc"],"jsonlLineStart":84,"jsonlLineEnd":97,"excerpt":"..."}],"keyExcerpts":["..."]}]}',
    'If nothing is durable, return {"notes":[]}.',
  ].join("\n");
  const raw = await callModel(settings, [
    { role: "system", content: system },
    { role: "user", content: user },
  ]);
  const parsed = await extractJsonWithRepair(settings, raw);
  return Array.isArray(parsed.notes) ? parsed.notes : [];
}

async function summarizeState(settings, turns, sessionRef) {
  if (turns.length === 0) {
    return {
      currentObjective: "No active objective recorded.",
      openThreads: [],
      recentDecisions: [],
      recentCorrections: [],
      importantRefs: [],
      lastDistilledFrom: sessionRef,
    };
  }
  const transcript = formatTurns(turns);
  const system = [
    "You summarize the latest working state after a session boundary.",
    "Return JSON only.",
    "Keep it terse and factual.",
    "Open threads must be unresolved items only.",
    "Recent decisions and corrections must be short bullets, not paragraphs.",
  ].join(" ");
  const user = [
    "Recent transcript tail:",
    transcript,
    "",
    "Return JSON with this shape:",
    '{"currentObjective":"...","openThreads":["..."],"recentDecisions":["..."],"recentCorrections":["..."],"importantRefs":["..."]}',
    "Use empty arrays when nothing applies.",
  ].join("\n");
  const raw = await callModel(settings, [
    { role: "system", content: system },
    { role: "user", content: user },
  ]);
  const parsed = extractJson(raw);
  return {
    currentObjective:
      typeof parsed.currentObjective === "string" && parsed.currentObjective.trim()
        ? parsed.currentObjective.trim()
        : "No active objective recorded.",
    openThreads: Array.isArray(parsed.openThreads) ? parsed.openThreads.map(String).filter(Boolean) : [],
    recentDecisions: Array.isArray(parsed.recentDecisions)
      ? parsed.recentDecisions.map(String).filter(Boolean)
      : [],
    recentCorrections: Array.isArray(parsed.recentCorrections)
      ? parsed.recentCorrections.map(String).filter(Boolean)
      : [],
    importantRefs: Array.isArray(parsed.importantRefs) ? parsed.importantRefs.map(String).filter(Boolean) : [],
    lastDistilledFrom: sessionRef,
  };
}

async function loadCursor(paths) {
  return (
    (await readJsonFile(paths.cursorFile, {
      version: 1,
      sessions: {},
    })) ?? { version: 1, sessions: {} }
  );
}

async function scanExistingIndexedKeys(indexedDir) {
  const found = {};

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
      try {
        const raw = await fs.readFile(fullPath, "utf8");
        const match = raw.match(/^---\r?\n([\s\S]*?)\r?\n---/);
        if (!match) {
          continue;
        }
        const keyLine = match[1]
          .split(/\r?\n/)
          .map((line) => line.trim())
          .find((line) => line.startsWith("key:"));
        if (!keyLine) {
          continue;
        }
        const key = keyLine.slice(4).trim();
        if (key) {
          found[key] = fullPath;
        }
      } catch {}
    }
  }

  await visit(indexedDir);
  return found;
}

function noteDirectoryFor(paths, note) {
  switch (note.type) {
    case "preference":
      return path.join(paths.indexedDir, "preferences");
    case "decision":
      return path.join(paths.indexedDir, "decisions");
    case "procedure":
      return path.join(paths.indexedDir, "procedures");
    case "project":
      return path.join(paths.indexedDir, "projects", slugify(note.project || "general"));
    case "fact":
    default:
      return path.join(paths.indexedDir, "facts");
  }
}

async function writeIndexedNotes(paths, notes, context) {
  const written = [];
  const existing = await scanExistingIndexedKeys(paths.indexedDir);
  const updatedAt = context.updatedAt ?? nowIso();
  for (const note of notes) {
    if (!note?.key) {
      continue;
    }
    const existingPath = existing[note.key] ?? null;
    const existingRelativePath = existingPath
      ? path.relative(paths.indexedDir, existingPath).replace(/\\/g, "/")
      : null;
    const existingNote = existingPath
      ? await loadIndexedNote(existingPath, existingRelativePath, { workspaceDir: paths.workspaceDir })
      : null;
    const incomingSourceSessions = dedupeStrings(
      [...(note.sourceSessions ?? []), ...(context.sessionId ? [context.sessionId] : [])],
      { maxCount: 32, maxChars: 120 },
    );
    const incomingNote = {
      ...note,
      sessionId: context.sessionId ?? note.sessionId ?? null,
      sourcePath: context.sourcePath ?? note.sourcePath ?? null,
      sourceRange: context.sourceRange ?? note.sourceRange ?? null,
      sourceSessions: incomingSourceSessions,
      updatedAt,
      lastSeenAt: updatedAt,
      sidecarPath: note.sidecarPath ?? existingNote?.sidecarPath ?? null,
    };
    const merged = existingNote
      ? mergeDurableNote(existingNote, incomingNote)
      : {
          ...incomingNote,
          createdAt: note.createdAt ?? updatedAt,
        };
    const dir = existingPath ? path.dirname(existingPath) : noteDirectoryFor(paths, merged);
    await fs.mkdir(dir, { recursive: true });
    const filePath = existingPath || path.join(
      dir,
      `${shortDatePrefix(merged.createdAt ?? updatedAt)}-${slugify(merged.key).slice(0, 60)}.md`,
    );
    const rendered = renderIndexedNote({
      ...merged,
      sidecarPath: merged.sidecarPath || createSidecarRelativePath(merged.sessionId, merged.key),
    });
    const oldSidecarPath = existingNote?.sidecarPath ?? null;
    const nextSidecarPath = rendered.overflow ? rendered.sidecarPath : null;
    const indexedChanged = await writeTextFileIfChanged(filePath, rendered.content);
    if (rendered.overflow && nextSidecarPath) {
      const sidecarFullPath = path.join(paths.workspaceDir, nextSidecarPath.replace(/\//g, path.sep));
      await writeTextFileIfChanged(sidecarFullPath, renderSidecarNote({
        ...merged,
        sidecarPath: nextSidecarPath,
      }));
    } else if (oldSidecarPath) {
      const oldSidecarFullPath = path.join(paths.workspaceDir, oldSidecarPath.replace(/\//g, path.sep));
      await fs.rm(oldSidecarFullPath, { force: true });
    }
    if (oldSidecarPath && nextSidecarPath && oldSidecarPath !== nextSidecarPath) {
      const oldSidecarFullPath = path.join(paths.workspaceDir, oldSidecarPath.replace(/\//g, path.sep));
      await fs.rm(oldSidecarFullPath, { force: true });
    }
    existing[merged.key] = filePath;
    if (indexedChanged) {
      written.push(filePath);
    }
  }
  return written;
}

async function writeSessionState(paths, state) {
  const body = [
    "# SESSION-STATE.md",
    "",
    "## Current Objective",
    state.currentObjective || "No active objective recorded.",
    "",
    "## Open Threads",
    ...(state.openThreads.length > 0 ? state.openThreads.map((item) => `- ${item}`) : ["- None recorded."]),
    "",
    "## Recent Decisions",
    ...(state.recentDecisions.length > 0
      ? state.recentDecisions.map((item) => `- ${item}`)
      : ["- None recorded."]),
    "",
    "## Recent Corrections",
    ...(state.recentCorrections.length > 0
      ? state.recentCorrections.map((item) => `- ${item}`)
      : ["- None recorded."]),
    "",
    "## Important IDs / Paths / URLs",
    ...(state.importantRefs.length > 0
      ? state.importantRefs.map((item) => `- ${item}`)
      : ["- None recorded."]),
    "",
    "## Last Distilled From",
    state.lastDistilledFrom || "Unknown boundary.",
    "",
  ].join("\n");
  await fs.writeFile(paths.sessionStateFile, `${body}\n`, "utf8");
}

export async function runQmdCommand(cfg, settings, subcommand) {
  if (!settings.qmdUpdateEnabled) {
    return;
  }
  const command = cfg?.memory?.qmd?.command;
  if (typeof command !== "string" || !command.trim()) {
    return;
  }

  const { spawn } = await import("node:child_process");
  await new Promise((resolve, reject) => {
    const isScript = /\.(?:mjs|js|cjs)$/i.test(command);
    const child = spawn(
      isScript ? process.execPath : command,
      isScript ? [command, subcommand] : [subcommand],
      {
        env: process.env,
        windowsHide: true,
        stdio: "ignore",
      },
    );
    child.once("error", reject);
    child.once("exit", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`QMD ${subcommand} exited with code ${code}`));
    });
  });
}

export async function runQmdUpdate(cfg, settings) {
  await runQmdCommand(cfg, settings, "update");
}

export async function runQmdEmbed(cfg, settings) {
  await runQmdCommand(cfg, settings, "embed");
}

async function processJob(job, settings, paths, openclawHome, cursor) {
  const sessionFile = await resolveSessionFile(job, openclawHome);
  if (!sessionFile) {
    throw new Error(`Unable to resolve transcript for ${job.sessionKey}`);
  }

  const transcript = await readTranscriptTurns(sessionFile);
  const sessionId = pickDefined(transcript.sessionId, job.sessionId, job.sessionKey);
  const cursorEntry = cursor.sessions?.[sessionId] ?? null;
  const deltaTurns = getDeltaTurns(transcript.turns, cursorEntry);

  if (deltaTurns.length === 0) {
    cursor.sessions[sessionId] = {
      lastTurnId: transcript.turns.at(-1)?.id ?? cursorEntry?.lastTurnId ?? null,
      turnCount: transcript.turns.length,
      sessionFile,
      updatedAt: nowIso(),
      lastJobKind: job.kind,
    };
    return { written: [], sessionId };
  }

  const chunks = chunkTurns(deltaTurns, settings.distiller.chunkChars);
  const noteCandidates = [];
  const noteTimestamp = nowIso();
  for (const chunk of chunks) {
    const notes = await extractNotes(settings, chunk);
    const chunkRange = formatTurnRange(chunk);
    for (const rawNote of notes) {
      const sanitizedRawNote = sanitizeExtractedNotePayload(rawNote);
      const normalized = normalizeExtractedNote(sanitizedRawNote, {
        sessionId,
        sourcePath: path.basename(sessionFile),
        sourceRange: chunkRange,
        chunkTurns: chunk,
        createdAt: noteTimestamp,
        updatedAt: noteTimestamp,
      });
      if (normalized) {
        noteCandidates.push(normalized);
      }
    }
  }
  const aggregatedNotes = aggregateCandidateNotes(noteCandidates, { updatedAt: noteTimestamp });

  const stateTurns = deltaTurns.slice(-settings.distiller.tailTurnsForState);
  const stateRef = `${job.sourceEvent} · ${path.basename(sessionFile)} · turns ${Math.max(
    transcript.turns.length - deltaTurns.length + 1,
    1,
  )}-${transcript.turns.length}`;
  const state = await summarizeState(settings, stateTurns, stateRef);
  await writeSessionState(paths, state);

  const written = await writeIndexedNotes(paths, aggregatedNotes, {
    sessionId,
    sourcePath: path.basename(sessionFile),
    sourceRange: `turns ${Math.max(transcript.turns.length - deltaTurns.length + 1, 1)}-${transcript.turns.length}`,
    updatedAt: noteTimestamp,
  });

  cursor.sessions[sessionId] = {
    lastTurnId: transcript.turns.at(-1)?.id ?? null,
    turnCount: transcript.turns.length,
    sessionFile,
    updatedAt: nowIso(),
    lastJobKind: job.kind,
  };

  return { written, sessionId };
}

async function processCanonicalJob(job, settings, paths) {
  return maybePromoteCanonical({
    settings,
    paths,
    sourceEvent: job.sourceEvent ?? "unknown",
    force: false,
  });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const cfg = await readConfig(args.configFile);
  const settings = resolveHookSettings(cfg);
  const workspaceDir = resolveWorkspaceDir({ cfg, override: args.workspace });
  const openclawHome = path.dirname(args.configFile);
  const paths = resolvePipelinePaths(workspaceDir);

  await ensurePipelineLayout(paths);

  const locked = await acquireLock(paths, settings);
  if (!locked) {
    return;
  }

  try {
    const queueRecords = await readJsonl(paths.queueFile);
    const jobs = collapseJobs(queueRecords);
    if (jobs.length === 0) {
      await writeJsonFile(paths.stateFile, {
        lastRunAt: nowIso(),
        status: "idle",
        processedJobs: 0,
      });
      await fs.writeFile(paths.queueFile, "", "utf8");
      return;
    }

    const cursor = await loadCursor(paths);
    const remaining = [];
    let qmdRelevantWrites = false;
    let canonicalRan = false;

    for (const job of jobs) {
      try {
        if (job.kind === "check_or_promote_canonical") {
          const result = await processCanonicalJob(job, settings, paths);
          canonicalRan = canonicalRan || result.ran;
          qmdRelevantWrites = qmdRelevantWrites || result.qmdRelevantChange;
          await logPipeline(paths, "info", "processed canonical promotion job", {
            kind: job.kind,
            sourceEvent: job.sourceEvent,
            ran: result.ran,
            changed: result.changed,
            qmdRelevantChange: result.qmdRelevantChange,
            reason: result.reason,
            stats: result.stats,
          });
          continue;
        }

        const result = await processJob(job, settings, paths, openclawHome, cursor);
        qmdRelevantWrites = qmdRelevantWrites || result.written.length > 0;
        await logPipeline(paths, "info", "processed memory boundary job", {
          sessionKey: job.sessionKey,
          sessionId: result.sessionId,
          kind: job.kind,
          sourceEvent: job.sourceEvent,
          wroteNotes: result.written.length,
        });
      } catch (error) {
        remaining.push(job);
        await logPipeline(paths, "error", "failed memory pipeline job", {
          sessionKey: job.sessionKey,
          kind: job.kind,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }

    await writeJsonFile(paths.cursorFile, cursor);
    await fs.writeFile(
      paths.queueFile,
      remaining.map((job) => JSON.stringify(job)).join("\n") + (remaining.length > 0 ? "\n" : ""),
      "utf8",
    );

    if (qmdRelevantWrites) {
      let updateSucceeded = false;
      try {
        await runQmdUpdate(cfg, settings);
        updateSucceeded = true;
        await logPipeline(paths, "info", "qmd update completed", {});
      } catch (error) {
        await logPipeline(paths, "warn", "qmd update failed", {
          error: error instanceof Error ? error.message : String(error),
        });
      }
      if (updateSucceeded) {
        try {
          await runQmdEmbed(cfg, settings);
          await logPipeline(paths, "info", "qmd embed completed", {});
        } catch (error) {
          await logPipeline(paths, "warn", "qmd embed failed", {
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }
    }

    await writeJsonFile(paths.stateFile, {
      lastRunAt: nowIso(),
      status: remaining.length === 0 ? "ok" : "partial",
      processedJobs: jobs.length,
      remainingJobs: remaining.length,
      wroteIndexedNotes: qmdRelevantWrites,
      ranCanonicalPromotion: canonicalRan,
    });
  } finally {
    await releaseLock(paths);
  }
}

if (import.meta.url === `file://${process.argv[1]?.replace(/\\/g, "/")}` || process.argv[1]?.endsWith("distiller-worker.mjs")) {
  main().catch(async (error) => {
    const args = parseArgs(process.argv.slice(2));
    const cfg = await readConfig(args.configFile);
    const workspaceDir = resolveWorkspaceDir({ cfg, override: args.workspace });
    const paths = resolvePipelinePaths(workspaceDir);
    await ensurePipelineLayout(paths);
    await logPipeline(paths, "error", "distiller worker crashed", {
      error: error instanceof Error ? error.message : String(error),
    });
    await releaseLock(paths);
    process.exitCode = 1;
  });
}
