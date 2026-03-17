import type { RerankDocument, RerankResult } from "./llm.js";
import type { RemoteRerankConfig } from "./collections.js";
import {
  buildModelUri,
  postJson,
  resolveOpenAICompatibleConfig,
  type ResolvedOpenAICompatibleConfig,
} from "./remote-common.js";

type ChatCompletionResponse = {
  choices?: Array<{
    message?: {
      content?: string | Array<{ type?: string; text?: string }>;
    };
  }>;
};

type ParsedRerankPayload = {
  results?: Array<{
    index: number;
    score: number;
  }>;
};

type NativeRerankResponse = {
  results?: Array<{
    index?: number;
    score?: number;
    relevance_score?: number;
    document?: { index?: number } | string;
  }>;
  data?: Array<{
    index?: number;
    score?: number;
    relevance_score?: number;
    document?: { index?: number } | string;
  }>;
};

export type RemoteRerankEndpoint = "chat-completions" | "rerank";

function getMessageText(response: ChatCompletionResponse): string {
  const content = response.choices?.[0]?.message?.content;
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    return content
      .map((item) => (item?.type === "text" ? item.text ?? "" : ""))
      .join("");
  }
  return "";
}

function extractJsonObject(text: string): ParsedRerankPayload | null {
  const trimmed = text.trim();
  const start = trimmed.indexOf("{");
  const end = trimmed.lastIndexOf("}");
  if (start === -1 || end === -1 || end < start) return null;
  try {
    return JSON.parse(trimmed.slice(start, end + 1)) as ParsedRerankPayload;
  } catch {
    return null;
  }
}

export class RemoteRerankProvider {
  readonly config: ResolvedOpenAICompatibleConfig;
  readonly provider = "openai-compatible";
  readonly model: string;
  readonly modelUri: string;
  readonly endpoint: RemoteRerankEndpoint;
  readonly maxDocuments?: number;

  constructor(config: RemoteRerankConfig) {
    this.config = resolveOpenAICompatibleConfig(config);
    this.model = this.config.model;
    this.endpoint = config.endpoint ?? "chat-completions";
    this.modelUri = `${buildModelUri(this.config)}#${this.endpoint}`;
    this.maxDocuments = config.max_documents;
  }

  async rerank(query: string, documents: RerankDocument[]): Promise<RerankResult> {
    if (documents.length === 0) {
      return { results: [], model: this.modelUri };
    }

    const limitedDocs = this.maxDocuments ? documents.slice(0, this.maxDocuments) : documents;
    const serializedDocs = limitedDocs.map((doc, index) => ({
      index,
      file: doc.file,
      text: doc.text.slice(0, 1500).replace(/\s+/g, " ").trim(),
    }));

    const parsed = this.endpoint === "rerank"
      ? await this.nativeRerank(query, serializedDocs)
      : await this.chatCompletionRerank(query, serializedDocs);
    const byIndex = new Map<number, number>();
    for (const item of parsed?.results ?? []) {
      if (!Number.isInteger(item.index)) continue;
      if (typeof item.score !== "number" || !Number.isFinite(item.score)) continue;
      byIndex.set(item.index, Math.max(0, Math.min(1, item.score)));
    }

    const results = limitedDocs
      .map((doc, index) => ({
        file: doc.file,
        score: byIndex.get(index) ?? 0,
        index,
      }))
      .sort((a, b) => b.score - a.score);

    return {
      results,
      model: this.modelUri,
    };
  }

  private async chatCompletionRerank(
    query: string,
    documents: Array<{ index: number; file: string; text: string }>,
  ): Promise<ParsedRerankPayload | null> {
    const response = await postJson<ChatCompletionResponse>(this.config, "/chat/completions", {
      model: this.model,
      temperature: 0,
      max_tokens: 1200,
      messages: [
        {
          role: "system",
          content: [
            "You are a document reranker.",
            "Return JSON only in the form:",
            '{"results":[{"index":0,"score":0.98}]}',
            "Include every document exactly once.",
            "Scores must be between 0 and 1, sorted descending.",
          ].join(" "),
        },
        {
          role: "user",
          content: JSON.stringify({
            query,
            documents,
          }),
        },
      ],
    });

    return extractJsonObject(getMessageText(response));
  }

  private async nativeRerank(
    query: string,
    documents: Array<{ index: number; file: string; text: string }>,
  ): Promise<ParsedRerankPayload> {
    const response = await postJson<NativeRerankResponse>(this.config, "/rerank", {
      model: this.model,
      query,
      documents: documents.map((doc) => doc.text),
      top_n: documents.length,
      return_documents: false,
    });

    const items = response.results ?? response.data ?? [];
    return {
      results: items.flatMap((item, fallbackIndex) => {
        const rawIndex = item.index
          ?? (typeof item.document === "object" && item.document ? item.document.index : undefined)
          ?? fallbackIndex;
        const rawScore = item.score ?? item.relevance_score;
        if (!Number.isInteger(rawIndex)) return [];
        if (typeof rawScore !== "number" || !Number.isFinite(rawScore)) return [];
        return [{ index: rawIndex, score: rawScore }];
      }),
    };
  }
}

export function createRemoteRerankProvider(
  config: RemoteRerankConfig | undefined,
): RemoteRerankProvider | undefined {
  if (!config) return undefined;
  if (config.provider !== "openai-compatible" || !config.model) return undefined;
  return new RemoteRerankProvider(config);
}
