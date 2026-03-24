import { afterEach, describe, expect, test, vi } from "vitest";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

const common = await import("../else/openclaw-hooks/memory-pipeline/common.mjs");
const distiller = await import("../else/openclaw-hooks/memory-pipeline/distiller-worker.mjs");

const tempDirs: string[] = [];

afterEach(async () => {
  while (tempDirs.length > 0) {
    const dir = tempDirs.pop();
    if (dir) {
      await rm(dir, { recursive: true, force: true });
    }
  }
});

async function makeTempDir(prefix: string) {
  const dir = await mkdtemp(join(tmpdir(), prefix));
  tempDirs.push(dir);
  return dir;
}

function makeAnchor(index: number) {
  return {
    turnStart: index,
    turnEnd: index + 1,
    messageIds: [`msg-${index}`],
    jsonlLineStart: 80 + index,
    jsonlLineEnd: 81 + index,
    sourcePath: "session.jsonl.reset",
    excerpt: `excerpt ${index}`,
  };
}

describe("memory pipeline helpers", () => {
  test("readTranscriptTurns keeps absolute turn numbers and jsonl lines in formatted transcript", async () => {
    const dir = await makeTempDir("memory-pipeline-session-");
    const sessionFile = join(dir, "session.jsonl");
    const raw = [
      JSON.stringify({ type: "session", id: "sess-1" }),
      JSON.stringify({
        type: "message",
        id: "msg-user",
        timestamp: "2026-03-23T01:00:00.000Z",
        message: {
          role: "user",
          content: [{ type: "text", text: "please sync qmd config" }],
        },
      }),
      JSON.stringify({
        type: "message",
        id: "msg-assistant",
        timestamp: "2026-03-23T01:00:01.000Z",
        message: {
          role: "assistant",
          content: [{ type: "text", text: "Synced config and verified memory.backend = qmd." }],
        },
      }),
    ].join("\n");
    await writeFile(sessionFile, `${raw}\n`, "utf8");

    const transcript = await distiller.readTranscriptTurns(sessionFile);
    const formatted = distiller.formatTurns(transcript.turns);

    expect(transcript.turns).toHaveLength(2);
    expect(transcript.turns[0]?.turnNumber).toBe(1);
    expect(transcript.turns[0]?.jsonlLine).toBe(2);
    expect(transcript.turns[1]?.turnNumber).toBe(2);
    expect(transcript.turns[1]?.jsonlLine).toBe(3);
    expect(formatted).toContain("T0001 L2 id=msg-user");
    expect(formatted).toContain("T0002 L3 id=msg-assistant");
  });

  test("normalizeExtractedNote synthesizes fallback anchors from chunk metadata", () => {
    const note = common.normalizeExtractedNote(
      {
        type: "decision",
        key: "openclaw-qmd-sync",
        title: "Sync OpenClaw and QMD Configurations",
        summary: "Cloud and local config were aligned.",
        whyItMatters: "Avoids drift.",
        evidence: ["memory.backend = qmd"],
        durableDetails: ["Cloud config now matches local search mode."],
      },
      {
        sessionId: "sess-1",
        sourcePath: "sess-1.jsonl.reset",
        sourceRange: "turns 11-18",
        createdAt: "2026-03-23T01:00:00.000Z",
        updatedAt: "2026-03-23T01:00:00.000Z",
        chunkTurns: [
          {
            id: "msg-11",
            turnNumber: 11,
            jsonlLine: 84,
            text: "memory.backend = qmd",
          },
          {
            id: "msg-18",
            turnNumber: 18,
            jsonlLine: 97,
            text: "memory.qmd.searchMode = query",
          },
        ],
      },
    );

    expect(note).toBeTruthy();
    expect(note?.sourceAnchors).toHaveLength(1);
    expect(note?.sourceAnchors[0]?.turnStart).toBe(11);
    expect(note?.sourceAnchors[0]?.turnEnd).toBe(18);
    expect(note?.sourceAnchors[0]?.jsonlLineStart).toBe(84);
    expect(note?.sourceAnchors[0]?.jsonlLineEnd).toBe(97);
    expect(note?.sourceAnchors[0]?.messageIds).toEqual(["msg-11", "msg-18"]);
    expect(note?.keyExcerpts[0]).toContain("memory.backend");
  });

  test("aggregateCandidateNotes merges same-key candidates from multiple chunks", () => {
    const first = common.normalizeExtractedNote(
      {
        type: "decision",
        key: "openclaw-qmd-sync",
        title: "Sync OpenClaw and QMD Configurations",
        summary: "Cloud and local config were aligned.",
        whyItMatters: "Avoids drift.",
        evidence: ["memory.backend = qmd"],
        durableDetails: ["Cloud config now matches local search mode."],
        sourceAnchors: [makeAnchor(11)],
        keyExcerpts: ["memory.backend = qmd"],
      },
      {
        sessionId: "sess-1",
        sourcePath: "sess-1.jsonl.reset",
        sourceRange: "turns 11-18",
        createdAt: "2026-03-23T01:00:00.000Z",
        updatedAt: "2026-03-23T01:00:00.000Z",
        chunkTurns: [{ id: "msg-11", turnNumber: 11, jsonlLine: 84, text: "memory.backend = qmd" }],
      },
    );
    const second = common.normalizeExtractedNote(
      {
        type: "decision",
        key: "openclaw-qmd-sync",
        title: "Sync OpenClaw and QMD Configurations",
        summary: "Cloud and local config were aligned.",
        whyItMatters: "Avoids configuration drift between local and cloud nodes.",
        evidence: ["memory.qmd.searchMode = query"],
        configRefs: ["memory.qmd.includeDefaultMemory = false"],
        sourceAnchors: [makeAnchor(24)],
        keyExcerpts: ["memory.qmd.includeDefaultMemory = false"],
      },
      {
        sessionId: "sess-1",
        sourcePath: "sess-1.jsonl.reset",
        sourceRange: "turns 24-31",
        createdAt: "2026-03-23T01:00:00.000Z",
        updatedAt: "2026-03-23T01:00:00.000Z",
        chunkTurns: [{ id: "msg-24", turnNumber: 24, jsonlLine: 143, text: "memory.qmd.searchMode = query" }],
      },
    );

    const aggregated = distiller.aggregateCandidateNotes([first, second], {
      updatedAt: "2026-03-23T01:05:00.000Z",
    });

    expect(aggregated).toHaveLength(1);
    expect(aggregated[0]?.evidence).toEqual([
      "memory.backend = qmd",
      "memory.qmd.searchMode = query",
    ]);
    expect(aggregated[0]?.configRefs).toEqual(["memory.qmd.includeDefaultMemory = false"]);
    expect(aggregated[0]?.sourceAnchors).toHaveLength(2);
    expect(aggregated[0]?.whyItMatters).toContain("configuration drift");
  });

  test("renderIndexedNote keeps moderate notes inline even when they have a few anchors and excerpts", () => {
    const note = {
      type: "procedure",
      key: "qmd-installation",
      title: "Install QMD for OpenClaw",
      summary: "Install the QMD fork and wire it as the OpenClaw memory backend.",
      whyItMatters: "Keeps server memory retrieval consistent.",
      evidence: ["Installed QMD fork: /root/apps/qmd"],
      project: "openclaw",
      tags: ["openclaw", "qmd"],
      sessionId: "sess-overflow",
      createdAt: "2026-03-23T01:00:00.000Z",
      updatedAt: "2026-03-23T01:10:00.000Z",
      lastSeenAt: "2026-03-23T01:10:00.000Z",
      sourcePath: "sess-overflow.jsonl.reset",
      sourceRange: "turns 1-40",
      sourceSessions: ["sess-overflow"],
      durableDetails: Array.from({ length: 7 }, (_, index) => `detail ${index + 1}`),
      configRefs: ["memory.backend = qmd", "memory.qmd.searchMode = query"],
      sourceAnchors: Array.from({ length: 5 }, (_, index) => makeAnchor(index + 1)),
      keyExcerpts: ["excerpt 1", "excerpt 2", "excerpt 3"],
    };

    const indexed = common.renderIndexedNote(note);

    expect(indexed.overflow).toBe(false);
    expect(indexed.sidecarPath).toBeNull();
    expect(indexed.content).not.toContain("## Sidecar Excerpt");
    expect(indexed.content).toContain('"excerpt 1"');
    expect(indexed.content).toContain('"excerpt 2"');
    expect(indexed.content).toContain('"excerpt 3"');
    expect(indexed.content).toContain("detail 7");
    expect(indexed.content).toContain("turns 5-6");
  });

  test("renderIndexedNote only overflows when the preview saves meaningful space", () => {
    const note = {
      type: "procedure",
      key: "qmd-installation-heavy",
      title: "Install QMD for OpenClaw",
      summary: "Install the QMD fork and wire it as the OpenClaw memory backend.",
      whyItMatters: "Keeps server memory retrieval consistent.",
      evidence: ["Installed QMD fork: /root/apps/qmd"],
      project: "openclaw",
      tags: ["openclaw", "qmd"],
      sessionId: "sess-overflow",
      createdAt: "2026-03-23T01:00:00.000Z",
      updatedAt: "2026-03-23T01:10:00.000Z",
      lastSeenAt: "2026-03-23T01:10:00.000Z",
      sourcePath: "sess-overflow.jsonl.reset",
      sourceRange: "turns 1-80",
      sourceSessions: ["sess-overflow"],
      durableDetails: Array.from({ length: 12 }, (_, index) => `detail ${index + 1}: ${"alpha ".repeat(10).trim()}`),
      configRefs: Array.from({ length: 7 }, (_, index) => `config.ref.${index + 1} = ${"beta ".repeat(6).trim()}`),
      sourceAnchors: Array.from({ length: 7 }, (_, index) => makeAnchor(index + 1)),
      keyExcerpts: Array.from({ length: 7 }, (_, index) => `excerpt ${index + 1} ${"gamma ".repeat(8).trim()}`),
    };

    const indexed = common.renderIndexedNote(note);
    const sidecar = common.renderSidecarNote({ ...note, sidecarPath: indexed.sidecarPath });

    expect(indexed.overflow).toBe(true);
    expect(indexed.sidecarPath).toBe("memory/archive/extracts/sess-overflow/qmd-installation-heavy.md");
    expect(
      indexed.metrics.lineSavings >= 20 || indexed.metrics.charSavings >= 700,
    ).toBe(true);
    expect(indexed.content).toContain("## Sidecar Excerpt");
    expect(indexed.content).toContain(`See sidecar: ${indexed.sidecarPath}`);
    expect(indexed.content).not.toContain("detail 12:");
    expect(indexed.content).not.toContain('"excerpt 7');
    expect(sidecar).toContain("## Full Source Anchors");
    expect(sidecar).toContain('"excerpt 7');
  });

  test("loadIndexedNote hydrates full fields from sidecar while keeping indexed summary", async () => {
    const workspaceDir = await makeTempDir("memory-pipeline-workspace-");
    const indexedDir = join(workspaceDir, "memory", "indexed", "decisions");
    const archiveDir = join(workspaceDir, "memory", "archive", "extracts", "sess-overflow");
    await mkdir(indexedDir, { recursive: true });
    await mkdir(archiveDir, { recursive: true });

    const note = {
      type: "decision",
      key: "openclaw-qmd-sync",
      title: "Sync OpenClaw and QMD Configurations",
      summary: "Cloud and local config were aligned.",
      whyItMatters: "Avoids drift.",
      evidence: ["memory.backend = qmd"],
      project: null,
      tags: ["openclaw", "qmd"],
      sessionId: "sess-overflow",
      createdAt: "2026-03-23T01:00:00.000Z",
      updatedAt: "2026-03-23T01:10:00.000Z",
      lastSeenAt: "2026-03-23T01:10:00.000Z",
      sourcePath: "sess-overflow.jsonl.reset",
      sourceRange: "turns 1-80",
      sourceSessions: ["sess-overflow"],
      durableDetails: Array.from({ length: 12 }, (_, index) => `detail ${index + 1}: ${"alpha ".repeat(10).trim()}`),
      configRefs: Array.from({ length: 7 }, (_, index) => `config.ref.${index + 1} = ${"beta ".repeat(6).trim()}`),
      sourceAnchors: Array.from({ length: 7 }, (_, index) => makeAnchor(index + 1)),
      keyExcerpts: Array.from({ length: 7 }, (_, index) => `excerpt ${index + 1} ${"gamma ".repeat(8).trim()}`),
    };

    const indexed = common.renderIndexedNote(note);
    const indexedPath = join(indexedDir, "202603230100-openclaw-qmd-sync.md");
    expect(indexed.sidecarPath).toBeTruthy();
    const sidecarPath = join(workspaceDir, indexed.sidecarPath.replace(/\//g, "\\"));
    await writeFile(indexedPath, indexed.content, "utf8");
    await writeFile(sidecarPath, common.renderSidecarNote({ ...note, sidecarPath: indexed.sidecarPath }), "utf8");

    const loaded = await common.loadIndexedNote(
      indexedPath,
      "decisions/202603230100-openclaw-qmd-sync.md",
      { workspaceDir },
    );

    expect(loaded.summary).toBe("Cloud and local config were aligned.");
    expect(loaded.sourceAnchors).toHaveLength(7);
    expect(loaded.durableDetails).toHaveLength(12);
    expect(loaded.sidecarPath).toBe(indexed.sidecarPath);
  });

  test("old-format indexed notes can be parsed and enriched by richer notes", () => {
    const oldRaw = [
      "---",
      "key: openclaw-qmd-sync",
      "type: decision",
      "project: ",
      "sessionId: sess-old",
      "createdAt: 2026-03-21T19:08:36.950Z",
      "sourcePath: sess-old.jsonl.reset",
      "sourceRange: turns 1-31",
      "tags:",
      "  - openclaw",
      "  - qmd",
      "---",
      "",
      "# Sync OpenClaw and QMD Configurations",
      "",
      "- Summary: Cloud and local config were aligned.",
      "- Why it matters: Avoids drift between local and cloud memory behavior.",
      "- Evidence:",
      "  - memory.backend = qmd",
      "",
    ].join("\n");

    const parsedOld = common.parseIndexedNote(oldRaw, "old.md", "decisions/old.md", {
      size: oldRaw.length,
      mtimeMs: Date.parse("2026-03-21T19:08:36.950Z"),
    });
    const richer = common.normalizeExtractedNote(
      {
        type: "decision",
        key: "openclaw-qmd-sync",
        title: "Sync OpenClaw and QMD Configurations",
        summary: "Cloud and local config were aligned.",
        whyItMatters: "Avoids configuration drift between local and cloud memory behavior.",
        evidence: ["memory.qmd.searchMode = query"],
        durableDetails: ["Cloud config now matches local search mode and default-memory policy."],
        sourceAnchors: [makeAnchor(24)],
        keyExcerpts: ["memory.qmd.includeDefaultMemory = false"],
      },
      {
        sessionId: "sess-new",
        sourcePath: "sess-new.jsonl.reset",
        sourceRange: "turns 24-31",
        createdAt: "2026-03-23T01:00:00.000Z",
        updatedAt: "2026-03-23T01:00:00.000Z",
        chunkTurns: [{ id: "msg-24", turnNumber: 24, jsonlLine: 143, text: "memory.qmd.searchMode = query" }],
      },
    );

    const merged = common.mergeDurableNote(parsedOld, richer);
    const rendered = common.renderIndexedNote(merged);

    expect(merged.evidence).toEqual([
      "memory.backend = qmd",
      "memory.qmd.searchMode = query",
    ]);
    expect(rendered.content).toContain("## Durable Details");
    expect(rendered.content).toContain("## Source Anchors");
    expect(rendered.content).toContain('memory.qmd.includeDefaultMemory = false');
  });

  test("extractNotes retries once with JSON repair when the distiller output is malformed", async () => {
    const originalFetch = globalThis.fetch;
    const originalKey = process.env.SILICONFLOW_API_KEY;
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          choices: [
            {
              message: {
                content:
                  '{"notes":[{"type":"decision","key":"openclaw-qmd-sync","title":"Sync OpenClaw and QMD","summary":"Aligned config","whyItMatters":"Avoids drift","evidence":["memory.backend = qmd"],"sourceAnchors":[{"turnStart":1,"turnEnd":2,"messageIds":["msg-1"],"jsonlLineStart":2,"jsonlLineEnd":3,"excerpt":"memory.backend = qmd"}],"keyExcerpts":["memory.backend = qmd"]}',
              },
            },
          ],
        }),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          choices: [
            {
              message: {
                content: JSON.stringify({
                  notes: [
                    {
                      type: "decision",
                      key: "openclaw-qmd-sync",
                      title: "Sync OpenClaw and QMD",
                      summary: "Aligned config",
                      whyItMatters: "Avoids drift",
                      evidence: ["memory.backend = qmd"],
                      sourceAnchors: [
                        {
                          turnStart: 1,
                          turnEnd: 2,
                          messageIds: ["msg-1"],
                          jsonlLineStart: 2,
                          jsonlLineEnd: 3,
                          excerpt: "memory.backend = qmd",
                        },
                      ],
                      keyExcerpts: ["memory.backend = qmd"],
                    },
                  ],
                }),
              },
            },
          ],
        }),
      });
    globalThis.fetch = fetchMock as typeof fetch;
    process.env.SILICONFLOW_API_KEY = "test-key";

    try {
      const notes = await distiller.extractNotes(
        {
          distiller: {
            baseUrl: "https://example.test/v1",
            apiKeyEnv: "SILICONFLOW_API_KEY",
            model: "Qwen/Qwen2.5-7B-Instruct",
            temperature: 0.1,
            maxOutputTokens: 2200,
            timeoutMs: 5000,
          },
        } as any,
        [
          {
            id: "msg-1",
            role: "user",
            text: "Please align memory backend config.",
            turnNumber: 1,
            jsonlLine: 2,
          },
          {
            id: "msg-2",
            role: "assistant",
            text: "Updated memory.backend = qmd and verified search mode.",
            turnNumber: 2,
            jsonlLine: 3,
          },
        ],
      );

      expect(notes).toHaveLength(1);
      expect(notes[0]?.key).toBe("openclaw-qmd-sync");
      expect(fetchMock).toHaveBeenCalledTimes(2);

      const secondRequest = JSON.parse(String(fetchMock.mock.calls[1]?.[1]?.body ?? "{}"));
      expect(secondRequest.messages[0]?.content).toContain("repair malformed JSON");
    } finally {
      globalThis.fetch = originalFetch;
      if (originalKey === undefined) {
        delete process.env.SILICONFLOW_API_KEY;
      } else {
        process.env.SILICONFLOW_API_KEY = originalKey;
      }
    }
  });

  test("sanitizeExtractedNotePayload redacts raw secret values before note normalization", () => {
    const sanitized = distiller.sanitizeExtractedNotePayload({
      title: "Fix API key issue",
      summary: "Configured sk-secretsecretsecretsecret in the environment.",
      whyItMatters: "Avoids auth failures.",
      evidence: ["Bearer supersecrettokenvalue", "memory.backend = qmd"],
      keyExcerpts: ["github_pat_abcdefghijklmnopqrstuvwxyz"],
      sourceAnchors: [{ excerpt: "pk-secretsecretsecretsecret" }],
    });

    expect(sanitized.summary).toContain("[REDACTED_SECRET]");
    expect(sanitized.summary).not.toContain("sk-secretsecretsecretsecret");
    expect(sanitized.evidence).toEqual([
      "[REDACTED_SECRET]",
      "memory.backend = qmd",
    ]);
    expect(sanitized.keyExcerpts).toEqual(["[REDACTED_SECRET]"]);
    expect(sanitized.sourceAnchors[0]?.excerpt).toBe("[REDACTED_SECRET]");
  });
});
