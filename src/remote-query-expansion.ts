import type { Queryable } from "./llm.js";
import type { RemoteQueryExpansionConfig } from "./collections.js";
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

function parseExpandedQueryLines(query: string, content: string, includeLexical: boolean): Queryable[] {
  const queryLower = query.toLowerCase();
  const queryTerms = queryLower.replace(/[^a-z0-9\s]/g, " ").split(/\s+/).filter(Boolean);
  const hasQueryTerm = (text: string): boolean => {
    const lower = text.toLowerCase();
    return queryTerms.length === 0 || queryTerms.some((term) => lower.includes(term));
  };

  const queryables = content
    .trim()
    .split("\n")
    .map((line) => {
      const colonIdx = line.indexOf(":");
      if (colonIdx === -1) return null;
      const type = line.slice(0, colonIdx).trim();
      if (type !== "lex" && type !== "vec" && type !== "hyde") return null;
      const text = line.slice(colonIdx + 1).trim();
      if (!text || !hasQueryTerm(text)) return null;
      return { type, text } as Queryable;
    })
    .filter((value): value is Queryable => value !== null);

  const filtered = includeLexical ? queryables : queryables.filter((item) => item.type !== "lex");
  if (filtered.length > 0) return filtered;

  const fallback: Queryable[] = [
    { type: "hyde", text: `Information about ${query}` },
    { type: "lex", text: query },
    { type: "vec", text: query },
  ];
  return includeLexical ? fallback : fallback.filter((item) => item.type !== "lex");
}

export class RemoteQueryExpansionProvider {
  readonly config: ResolvedOpenAICompatibleConfig;
  readonly provider = "openai-compatible";
  readonly model: string;
  readonly modelUri: string;
  readonly temperature: number;
  readonly maxTokens: number;

  constructor(config: RemoteQueryExpansionConfig) {
    this.config = resolveOpenAICompatibleConfig(config);
    this.model = this.config.model;
    this.modelUri = buildModelUri(this.config);
    this.temperature = config.temperature ?? 0.2;
    this.maxTokens = config.max_tokens ?? 400;
  }

  async expandQuery(
    query: string,
    options: { context?: string; includeLexical?: boolean; intent?: string } = {},
  ): Promise<Queryable[]> {
    const includeLexical = options.includeLexical ?? true;
    const prompt = [
      "Expand this search query into typed lines.",
      "Return one item per line using exactly one of these prefixes: lex:, vec:, hyde:.",
      "lex lines are short keyword phrases for BM25.",
      "vec lines are natural-language semantic search variants.",
      "hyde is one concise hypothetical answer passage.",
      "Do not include prose or markdown fences.",
      "",
      `Query: ${query}`,
      ...(options.intent ? [`Intent: ${options.intent}`] : []),
      ...(options.context ? [`Context: ${options.context}`] : []),
    ].join("\n");

    const response = await postJson<ChatCompletionResponse>(this.config, "/chat/completions", {
      model: this.model,
      temperature: this.temperature,
      max_tokens: this.maxTokens,
      messages: [
        {
          role: "system",
          content: "You are a search query expansion engine. Output only typed lines.",
        },
        {
          role: "user",
          content: prompt,
        },
      ],
    });

    return parseExpandedQueryLines(query, getMessageText(response), includeLexical);
  }
}

export function createRemoteQueryExpansionProvider(
  config: RemoteQueryExpansionConfig | undefined,
): RemoteQueryExpansionProvider | undefined {
  if (!config) return undefined;
  if (config.provider !== "openai-compatible" || !config.model) return undefined;
  return new RemoteQueryExpansionProvider(config);
}
