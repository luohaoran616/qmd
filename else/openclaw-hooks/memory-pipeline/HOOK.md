---
name: memory-pipeline
description: "Boundary-only memory queue plus opportunistic canonical promotion"
homepage: https://github.com/tobi/qmd
metadata:
  {
    "openclaw":
      {
        "emoji": "🧠",
        "events": ["command:new", "command:reset", "session:compact:after", "gateway:startup", "session:start"],
        "requires": { "config": ["workspace.dir"], "bins": ["node"] },
      },
  }
---

# Memory Pipeline Hook

This hook implements the Boundary-Only memory plan for OpenClaw without
modifying OpenClaw core.

## What It Does

When one of these events fires:

- `/new`
- `/reset`
- `session:compact:after`
- `gateway:startup`
- `session:start`

the hook:

1. Appends a lightweight job to `memory/.pipeline/queue.jsonl`
2. Appends `/new` and `/reset` finalize markers to `memory/.pipeline/pending-finalize.jsonl`
3. Starts a detached worker if one is not already running

Boundary events (`/new`, `/reset`, `session:compact:after`) enqueue both:

- a boundary distillation job
- a canonical-promotion check job

Startup/session-open events enqueue only:

- a canonical-promotion check job

The hook itself never calls an LLM and never runs QMD updates inline.

## Design Goals

- Boundary-only distillation
- Opportunistic canonical promotion
- No mid-session threshold processing
- No additional token burn on the main reply path
- Compatible with upstream OpenClaw updates

## Files Used

- `memory/.pipeline/queue.jsonl`
- `memory/.pipeline/pending-finalize.jsonl`
- `memory/.pipeline/worker.lock`
- `memory/.pipeline/distill-log.jsonl`
- `memory/.pipeline/canonical-store.json`
- `memory/.pipeline/canonical-cursor.json`
- `memory/.pipeline/promotion-log.jsonl`
- `memory/.pipeline/proposals/`

## Configuration

Example:

```json
{
  "hooks": {
    "internal": {
      "load": {
        "extraDirs": ["D:/path/to/openclaw-hooks"]
      },
      "entries": {
        "memory-pipeline": {
          "enabled": true,
          "lockTtlMs": 600000,
          "distiller": {
            "baseUrl": "https://api.siliconflow.cn/v1",
            "apiKeyEnv": "SILICONFLOW_API_KEY",
            "model": "Qwen/Qwen2.5-7B-Instruct"
          },
          "promoter": {
            "enabled": true,
            "mode": "mixed",
            "trigger": {
              "pendingBytes": 204800,
              "maxAge": "3d"
            },
            "model": {
              "baseUrl": "https://api.siliconflow.cn/v1",
              "apiKeyEnv": "SILICONFLOW_API_KEY",
              "model": "Qwen/Qwen2.5-7B-Instruct"
            },
            "targets": {
              "auto": ["user", "memory"],
              "proposalOnly": ["soul", "identity"]
            },
            "budgets": {
              "userMaxLines": 80,
              "memoryMaxLines": 120
            }
          }
        }
      }
    }
  }
}
```

## Notes

- The detached worker lives beside this hook in `distiller-worker.mjs`.
- The canonical promoter helper lives in `canonical-promoter.mjs`.
- Transcript raw truth remains `sessions/*.jsonl`.
- Indexed durable memory is written to `memory/indexed/**`.
- Canonical promotion rewrites managed sections in `USER.md` / `MEMORY.md` and
  writes proposal files for `SOUL.md` / `IDENTITY.md`.
