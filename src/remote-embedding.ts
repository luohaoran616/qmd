import type { EmbeddingResult } from "./llm.js";
import type { RemoteEmbeddingConfig } from "./collections.js";
import {
  buildModelUri,
  postJson,
  resolveOpenAICompatibleConfig,
  type ResolvedOpenAICompatibleConfig,
} from "./remote-common.js";

type EmbeddingResponse = {
  data?: Array<{
    embedding: number[];
    index: number;
  }>;
};

export class RemoteEmbeddingProvider {
  readonly config: ResolvedOpenAICompatibleConfig;
  readonly provider = "openai-compatible";
  readonly model: string;
  readonly dimensions?: number;
  readonly modelUri: string;

  constructor(config: RemoteEmbeddingConfig) {
    this.config = resolveOpenAICompatibleConfig(config);
    this.model = this.config.model;
    this.dimensions = config.dimensions;
    this.modelUri = buildModelUri(this.config);
  }

  async embed(text: string): Promise<EmbeddingResult | null> {
    const results = await this.embedBatch([text]);
    return results[0] ?? null;
  }

  async embedBatch(texts: string[]): Promise<(EmbeddingResult | null)[]> {
    if (texts.length === 0) return [];

    const response = await postJson<EmbeddingResponse>(this.config, "/embeddings", {
      model: this.model,
      input: texts,
      ...(this.dimensions ? { dimensions: this.dimensions } : {}),
    });

    const byIndex = new Map<number, EmbeddingResult>();
    for (const item of response.data ?? []) {
      byIndex.set(item.index, {
        embedding: item.embedding,
        model: this.modelUri,
      });
    }

    return texts.map((_, index) => byIndex.get(index) ?? null);
  }
}

export function createRemoteEmbeddingProvider(
  config: RemoteEmbeddingConfig | undefined,
): RemoteEmbeddingProvider | undefined {
  if (!config) return undefined;
  if (config.provider !== "openai-compatible" || !config.model) return undefined;
  return new RemoteEmbeddingProvider(config);
}
