import fs from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  appendJsonl,
  ensurePipelineLayout,
  logPipeline,
  nowIso,
  releaseStaleLock,
  resolveDefaultConfigPath,
  resolveHookSettings,
  resolvePipelinePaths,
  resolveWorkspaceDir,
} from "./common.mjs";

const hookDir = path.dirname(fileURLToPath(import.meta.url));
const workerScript = path.join(hookDir, "distiller-worker.mjs");

function isBoundaryDistillEvent(event) {
  return (
    (event.type === "command" && (event.action === "new" || event.action === "reset")) ||
    (event.type === "session" && event.action === "compact:after")
  );
}

function isCanonicalTriggerEvent(event) {
  return (
    isBoundaryDistillEvent(event) ||
    (event.type === "gateway" && event.action === "startup") ||
    (event.type === "session" && event.action === "start")
  );
}

function buildBoundaryJob(event, workspaceDir) {
  const previousSession = event.context?.previousSessionEntry ?? {};
  const currentSession = event.context?.sessionEntry ?? {};
  const sessionEntry =
    event.type === "command" && (event.action === "new" || event.action === "reset")
      ? previousSession
      : currentSession;

  return {
    id: crypto.randomUUID(),
    kind:
      event.type === "command" ? "finalize_session" : "post_compaction_refresh",
    sourceEvent: `${event.type}:${event.action}`,
    sessionKey: event.sessionKey,
    sessionId:
      sessionEntry?.sessionId ??
      event.context?.sessionId ??
      currentSession?.sessionId ??
      previousSession?.sessionId ??
      null,
    sessionFile:
      sessionEntry?.sessionFile ??
      event.context?.sessionFile ??
      currentSession?.sessionFile ??
      previousSession?.sessionFile ??
      null,
    workspaceDir,
    commandSource: event.context?.commandSource ?? null,
    createdAt: nowIso(event.timestamp),
    compaction: event.type === "session" ? event.context ?? null : null,
  };
}

function buildCanonicalJob(event, workspaceDir) {
  return {
    id: crypto.randomUUID(),
    kind: "check_or_promote_canonical",
    sourceEvent: `${event.type}:${event.action}`,
    sessionKey:
      typeof event.sessionKey === "string" && event.sessionKey.trim()
        ? event.sessionKey
        : `${event.type}:${event.action}`,
    sessionId:
      event.context?.sessionId ??
      event.context?.sessionEntry?.sessionId ??
      event.context?.previousSessionEntry?.sessionId ??
      null,
    workspaceDir,
    createdAt: nowIso(event.timestamp),
  };
}

async function spawnWorker({ configPath, workspaceDir }) {
  const child = spawn(
    process.execPath,
    [workerScript, "--drain-once", "--config-file", configPath, "--workspace", workspaceDir],
    {
      detached: true,
      stdio: "ignore",
      windowsHide: true,
    },
  );
  child.unref();
}

const handler = async (event) => {
  if (!isCanonicalTriggerEvent(event)) {
    return;
  }

  const cfg = event.context?.cfg ?? {};
  const settings = resolveHookSettings(cfg);
  if (!settings.enabled) {
    return;
  }

  const workspaceDir = resolveWorkspaceDir({
    cfg,
    override: typeof event.context?.workspaceDir === "string" ? event.context.workspaceDir : undefined,
  });
  const paths = resolvePipelinePaths(workspaceDir);
  const configPath = resolveDefaultConfigPath(process.env);

  try {
    await ensurePipelineLayout(paths);

    if (isBoundaryDistillEvent(event)) {
      const job = buildBoundaryJob(event, workspaceDir);
      await appendJsonl(paths.queueFile, job);

      if (job.kind === "finalize_session") {
        await appendJsonl(paths.pendingFinalizeFile, {
          timestamp: job.createdAt,
          sessionKey: job.sessionKey,
          sessionId: job.sessionId,
          sessionFile: job.sessionFile,
          sourceEvent: job.sourceEvent,
        });
      }
    }

    await appendJsonl(paths.queueFile, buildCanonicalJob(event, workspaceDir));

    await releaseStaleLock(paths.lockFile, settings.lockTtlMs);

    try {
      await fs.access(paths.lockFile);
      return;
    } catch {
      await spawnWorker({ configPath, workspaceDir });
    }
  } catch (error) {
    await logPipeline(paths, "error", "memory-pipeline hook failed", {
      error: error instanceof Error ? error.message : String(error),
      eventType: event.type,
      eventAction: event.action,
    });
  }
};

export default handler;
