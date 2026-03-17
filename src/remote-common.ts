import type {
  RemoteEmbeddingConfig,
  RemoteProviderConfig,
  RemoteQueryExpansionConfig,
  RemoteRerankConfig,
} from "./collections.js";

export const DEFAULT_OPENAI_COMPATIBLE_BASE_URL = "https://api.openai.com/v1";
export const DEFAULT_REMOTE_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_RETRIES = 2;

type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export type OpenAICompatibleConfig =
  | RemoteProviderConfig
  | RemoteEmbeddingConfig
  | RemoteRerankConfig
  | RemoteQueryExpansionConfig;

export type ResolvedOpenAICompatibleConfig = {
  provider: "openai-compatible";
  model: string;
  apiKey?: string;
  baseUrl: string;
  timeoutMs: number;
};

export function resolveEnvTemplate(value: string | undefined): string | undefined {
  if (!value) return undefined;
  const match = value.match(/^\$\{([A-Z0-9_]+)\}$/);
  if (!match) return value;
  const envName = match[1];
  if (!envName) return undefined;
  const envValue = process.env[envName];
  return envValue?.trim() || undefined;
}

export function resolveOpenAICompatibleConfig(
  config: OpenAICompatibleConfig,
  fallbackEnvNames: string[] = ["OPENAI_API_KEY"],
): ResolvedOpenAICompatibleConfig {
  const apiKey =
    resolveEnvTemplate(config.api_key)
    || fallbackEnvNames.map((name) => process.env[name]?.trim()).find(Boolean);

  return {
    provider: "openai-compatible",
    model: config.model,
    apiKey,
    baseUrl: (resolveEnvTemplate(config.base_url) || process.env.OPENAI_API_BASE || DEFAULT_OPENAI_COMPATIBLE_BASE_URL).replace(/\/+$/, ""),
    timeoutMs: config.timeout_ms ?? DEFAULT_REMOTE_TIMEOUT_MS,
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function tryParseJson(text: string): JsonValue | undefined {
  try {
    return JSON.parse(text) as JsonValue;
  } catch {
    return undefined;
  }
}

function extractMessageFromJson(value: JsonValue | undefined): string | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const message = value.message;
  if (typeof message === "string") return message;
  const error = value.error;
  if (!error || typeof error !== "object" || Array.isArray(error)) return undefined;
  return typeof error.message === "string" ? error.message : undefined;
}

function createAbortController(timeoutMs: number): AbortController {
  const controller = new AbortController();
  const timer = setTimeout(() => {
    controller.abort(new Error(`Request timed out after ${timeoutMs}ms`));
  }, timeoutMs);
  timer.unref?.();
  controller.signal.addEventListener("abort", () => clearTimeout(timer), { once: true });
  return controller;
}

export async function postJson<TResponse>(
  config: ResolvedOpenAICompatibleConfig,
  path: string,
  body: Record<string, unknown>,
): Promise<TResponse> {
  let attempt = 0;
  let lastError: unknown;

  while (attempt <= DEFAULT_MAX_RETRIES) {
    const controller = createAbortController(config.timeoutMs);
    try {
      const response = await fetch(`${config.baseUrl}${path}`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(config.apiKey ? { Authorization: `Bearer ${config.apiKey}` } : {}),
        },
        body: JSON.stringify(body),
        signal: controller.signal,
      });

      const text = await response.text();
      if (!response.ok) {
        const parsed = tryParseJson(text);
        const message = extractMessageFromJson(parsed) || text || `HTTP ${response.status}`;
        throw new Error(`Remote request failed (${response.status}): ${message}`);
      }

      return (tryParseJson(text) as TResponse | undefined) ?? ({} as TResponse);
    } catch (error) {
      lastError = error;
      if (attempt >= DEFAULT_MAX_RETRIES) break;
      await sleep(250 * (attempt + 1));
      attempt++;
    }
  }

  throw lastError instanceof Error ? lastError : new Error("Remote request failed");
}

export function buildModelUri(config: ResolvedOpenAICompatibleConfig): string {
  return `${config.provider}:${config.model}`;
}
