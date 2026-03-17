import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  createRemoteEmbeddingProvider,
  RemoteEmbeddingProvider,
} from "../src/remote-embedding.js";
import {
  createRemoteQueryExpansionProvider,
  RemoteQueryExpansionProvider,
} from "../src/remote-query-expansion.js";
import {
  createRemoteRerankProvider,
  RemoteRerankProvider,
} from "../src/remote-rerank.js";
import { createStore } from "../src/index.js";
import type { CollectionConfig } from "../src/collections.js";

const originalFetch = globalThis.fetch;

describe("remote providers", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    delete process.env.OPENAI_API_KEY;
    delete process.env.OPENAI_API_BASE;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  test("embedding provider resolves env template and preserves batch order", async () => {
    process.env.OPENAI_API_KEY = "test-key";
    const calls: Array<{ url: string; body: unknown }> = [];
    globalThis.fetch = vi.fn(async (url: string | URL, init?: RequestInit) => {
      calls.push({ url: String(url), body: JSON.parse(String(init?.body || "{}")) });
      return new Response(JSON.stringify({
        data: [
          { index: 1, embedding: [0, 1] },
          { index: 0, embedding: [1, 0] },
        ],
      }), { status: 200 });
    }) as typeof fetch;

    const provider = new RemoteEmbeddingProvider({
      provider: "openai-compatible",
      model: "text-embedding-3-small",
      api_key: "${OPENAI_API_KEY}",
      base_url: "https://example.invalid/v1",
    });
    const results = await provider.embedBatch(["first", "second"]);

    expect(results[0]?.embedding).toEqual([1, 0]);
    expect(results[1]?.embedding).toEqual([0, 1]);
    expect(calls[0]?.url).toBe("https://example.invalid/v1/embeddings");
  });

  test("query expansion provider parses typed lines", async () => {
    globalThis.fetch = vi.fn(async () => new Response(JSON.stringify({
      choices: [
        {
          message: {
            content: "lex: auth config\nvec: configure authentication service\nhyde: Authentication can be configured with env vars.",
          },
        },
      ],
    }), { status: 200 })) as typeof fetch;

    const provider = new RemoteQueryExpansionProvider({
      provider: "openai-compatible",
      model: "gpt-4o-mini",
      base_url: "https://example.invalid/v1",
    });

    const results = await provider.expandQuery("auth config", { includeLexical: false });
    expect(results.some((item) => item.type === "lex")).toBe(false);
    expect(results.some((item) => item.type === "vec")).toBe(true);
    expect(results.some((item) => item.type === "hyde")).toBe(true);
  });

  test("rerank provider sorts descending by parsed score", async () => {
    globalThis.fetch = vi.fn(async () => new Response(JSON.stringify({
      choices: [
        {
          message: {
            content: JSON.stringify({
              results: [
                { index: 1, score: 0.2 },
                { index: 0, score: 0.9 },
              ],
            }),
          },
        },
      ],
    }), { status: 200 })) as typeof fetch;

    const provider = new RemoteRerankProvider({
      provider: "openai-compatible",
      model: "gpt-4o-mini",
      base_url: "https://example.invalid/v1",
    });

    const result = await provider.rerank("auth", [
      { file: "a.md", text: "authentication docs" },
      { file: "b.md", text: "gardening notes" },
    ]);

    expect(result.results[0]?.file).toBe("a.md");
    expect(result.results[0]?.score).toBeGreaterThan(result.results[1]?.score ?? 0);
  });

  test("rerank provider supports native /rerank endpoint", async () => {
    const calls: Array<{ url: string; body: unknown }> = [];
    globalThis.fetch = vi.fn(async (url: string | URL, init?: RequestInit) => {
      calls.push({ url: String(url), body: JSON.parse(String(init?.body || "{}")) });
      return new Response(JSON.stringify({
        results: [
          { index: 1, relevance_score: 0.2 },
          { index: 0, relevance_score: 0.95 },
        ],
      }), { status: 200 });
    }) as typeof fetch;

    const provider = new RemoteRerankProvider({
      provider: "openai-compatible",
      endpoint: "rerank",
      model: "Qwen/Qwen3-Reranker-0.6B",
      base_url: "https://example.invalid/v1",
    });

    const result = await provider.rerank("auth", [
      { file: "a.md", text: "authentication docs" },
      { file: "b.md", text: "gardening notes" },
    ]);

    expect(calls[0]?.url).toBe("https://example.invalid/v1/rerank");
    expect(result.results[0]?.file).toBe("a.md");
    expect(result.results[0]?.score).toBeGreaterThan(result.results[1]?.score ?? 0);
    expect(provider.endpoint).toBe("rerank");
  });

  test("factory returns undefined for missing config", () => {
    expect(createRemoteEmbeddingProvider(undefined)).toBeUndefined();
    expect(createRemoteRerankProvider(undefined)).toBeUndefined();
    expect(createRemoteQueryExpansionProvider(undefined)).toBeUndefined();
  });
});

describe("createStore remote wiring", () => {
  let tempDir: string;

  beforeEach(async () => {
    tempDir = await mkdtemp(join(tmpdir(), "qmd-remote-store-"));
  });

  afterEach(async () => {
    await rm(tempDir, { recursive: true, force: true });
  });

  test("keeps official default behavior with no remote config", async () => {
    const store = await createStore({
      dbPath: join(tempDir, "default.sqlite"),
      config: { collections: {} },
    });

    expect(store.internal.remoteEmbedding).toBeUndefined();
    expect(store.internal.remoteRerank).toBeUndefined();
    expect(store.internal.remoteQueryExpansion).toBeUndefined();
    await store.close();
  });

  test("attaches remote providers from inline config", async () => {
    const config: CollectionConfig = {
      collections: {},
      embedding: {
        provider: "openai-compatible",
        model: "text-embedding-3-small",
        base_url: "https://embeddings.example/v1",
      },
      rerank: {
        provider: "openai-compatible",
        model: "gpt-4o-mini",
        base_url: "https://rerank.example/v1",
        endpoint: "rerank",
      },
      query_expansion: {
        provider: "openai-compatible",
        model: "gpt-4o-mini",
        base_url: "https://expand.example/v1",
      },
    };

    const store = await createStore({
      dbPath: join(tempDir, "remote.sqlite"),
      config,
    });

    expect(store.internal.remoteEmbedding?.model).toBe("text-embedding-3-small");
    expect(store.internal.remoteRerank?.model).toBe("gpt-4o-mini");
    expect(store.internal.remoteRerank?.endpoint).toBe("rerank");
    expect(store.internal.remoteQueryExpansion?.model).toBe("gpt-4o-mini");
    await store.close();
  });
});
