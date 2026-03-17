import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

type ChatCompletionResponse = {
  choices?: Array<{
    message?: {
      content?: string | Array<{ type?: string; text?: string }>;
    };
  }>;
  usage?: {
    prompt_tokens?: number;
    completion_tokens?: number;
    total_tokens?: number;
  };
};

type RunResult = {
  model: string;
  query: string;
  runIndex: number;
  elapsedMs: number;
  ok: boolean;
  output: string;
  error?: string;
  metrics: ReturnType<typeof analyzeOutput>;
  usage?: ChatCompletionResponse["usage"];
};

const API_KEY = process.env.QE_API_KEY || process.env.SILICONFLOW_API_KEY || "";
const BASE_URL = process.env.QE_BASE_URL || process.env.SILICONFLOW_BASE_URL || "https://api.siliconflow.cn/v1";
const RUNS_PER_QUERY = Number(process.env.QE_RUNS_PER_QUERY || 3);
const REQUEST_TIMEOUT_MS = Number(process.env.QE_TIMEOUT_MS || 45_000);
const MAX_TOKENS = Number(process.env.QE_MAX_TOKENS || 400);
const TEMPERATURE = Number(process.env.QE_TEMPERATURE || 0.2);

const DEFAULT_MODELS = [
  "Qwen/Qwen3.5-9B",
  "Qwen/Qwen3.5-4B",
  "THUDM/GLM-4-9B-0414",
];
const MODELS = (process.env.QE_MODELS || "")
  .split(",")
  .map((model) => model.trim())
  .filter(Boolean);
const EFFECTIVE_MODELS = MODELS.length > 0 ? MODELS : DEFAULT_MODELS;

const TEST_QUERIES = [
  "AUTH_SECRET config",
  "memory.qmd.scope.default",
  "openclaw gateway restart",
  "searchVec cache key",
  "CollectionConfig embedding",
  "expandQuery lex vec hyde",
  "README remote providers",
  "example-index.remote.yml",
  "Qwen/Qwen3-Reranker-0.6B",
  "Qwen/Qwen3-Embedding-0.6B",
  "OPENAI_API_KEY",
  "memory-root",
  "qmd status remote model",
  "query expansion output format",
  "rerank max_documents",
  "base_url openai-compatible",
  "timeout_ms config",
  "embedding dimensions 1536",
  "AUTH_SECRET environment variable",
  "docs ai UPSTREAM.md",
];

const SYSTEM_PROMPT = [
  "You are a search query expansion engine for a hybrid retrieval system.",
  "Expand the user's query into retrieval-oriented lines.",
  "",
  "Output rules:",
  "- Output only plain text lines",
  "- Each line must start with exactly one of: lex:, vec:, hyde:",
  "- lex: short keyword-style expansions for BM25",
  "- vec: natural-language semantic expansions for vector search",
  "- hyde: one concise hypothetical answer passage",
  "- Preserve important identifiers, env vars, API names, file names, symbols, and product names",
  "- Do not output explanations",
  "- Do not output markdown",
  "- Do not output numbering",
  "- Do not output code fences",
  "- Do not output <think>",
  "",
  "Preferred output:",
  "- 1-2 lex lines",
  "- 1-2 vec lines",
  "- 1 hyde line",
].join("\n");

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

function extractEntities(query: string): string[] {
  const raw = query.match(/[A-Za-z0-9_./:-]+/g) ?? [];
  const entities = raw.filter((token) => {
    if (token.length < 3) return false;
    return (
      /[A-Z]/.test(token)
      || /[_./:-]/.test(token)
      || /\d/.test(token)
    );
  });
  return [...new Set(entities)];
}

function analyzeOutput(query: string, output: string) {
  const lines = output
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  const validPrefixes = new Set(["lex:", "vec:", "hyde:"]);
  const invalidLines = lines.filter((line) => ![...validPrefixes].some((prefix) => line.startsWith(prefix)));
  const lexLines = lines.filter((line) => line.startsWith("lex:"));
  const vecLines = lines.filter((line) => line.startsWith("vec:"));
  const hydeLines = lines.filter((line) => line.startsWith("hyde:"));
  const entities = extractEntities(query);
  const lowerOutput = output.toLowerCase();
  const preservedEntities = entities.filter((entity) => lowerOutput.includes(entity.toLowerCase()));
  const hydeText = hydeLines[0]?.slice("hyde:".length).trim() ?? "";

  const noThink = !/<think>|<\/think>/i.test(output);
  const noMarkdownFence = !/```/.test(output);
  const noBulletOrNumbering = !/^\s*(?:[-*]|\d+\.)\s/m.test(output);
  const hasLex = lexLines.length >= 1;
  const hasVec = vecLines.length >= 1;
  const hasHyde = hydeLines.length >= 1;
  const hydeLengthOk = hydeText.length >= 50 && hydeText.length <= 220;
  const lexShortEnough = lexLines.every((line) => line.length <= 80);
  const vecLongerThanLex = vecLines.every((line) => line.length >= 20);
  const onlyStructuredLines = invalidLines.length === 0;
  const entityPreservationRate = entities.length === 0 ? 1 : preservedEntities.length / entities.length;

  const formatPass = onlyStructuredLines && hasVec && noThink && noMarkdownFence && noBulletOrNumbering;
  const entityPass = entityPreservationRate >= 0.75;
  const hydePass = hasHyde && hydeLengthOk;
  const overallPass = formatPass && entityPass && hydePass;

  return {
    lines,
    invalidLines,
    lexCount: lexLines.length,
    vecCount: vecLines.length,
    hydeCount: hydeLines.length,
    hasLex,
    hasVec,
    hasHyde,
    hydeLength: hydeText.length,
    hydeLengthOk,
    noThink,
    noMarkdownFence,
    noBulletOrNumbering,
    lexShortEnough,
    vecLongerThanLex,
    entities,
    preservedEntities,
    entityPreservationRate,
    formatPass,
    entityPass,
    hydePass,
    overallPass,
  };
}

async function callModel(model: string, query: string): Promise<{ output: string; usage?: ChatCompletionResponse["usage"] }> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
  };
  if (API_KEY) {
    headers.Authorization = `Bearer ${API_KEY}`;
  }

  try {
    const response = await fetch(`${BASE_URL.replace(/\/+$/, "")}/chat/completions`, {
      method: "POST",
      headers,
      signal: controller.signal,
      body: JSON.stringify({
        model,
        temperature: TEMPERATURE,
        max_tokens: MAX_TOKENS,
        messages: [
          { role: "system", content: SYSTEM_PROMPT },
          { role: "user", content: `Expand this query for retrieval: ${query}` },
        ],
      }),
    });
    const text = await response.text();
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${text}`);
    }

    const parsed = JSON.parse(text) as ChatCompletionResponse;
    return {
      output: getMessageText(parsed).trim(),
      usage: parsed.usage,
    };
  } finally {
    clearTimeout(timer);
  }
}

function average(values: number[]): number {
  if (values.length === 0) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function percentage(numerator: number, denominator: number): string {
  if (denominator === 0) return "0.0%";
  return `${((numerator / denominator) * 100).toFixed(1)}%`;
}

function summarizeModel(model: string, results: RunResult[]) {
  const okRuns = results.filter((r) => r.ok);
  const formatPasses = okRuns.filter((r) => r.metrics.formatPass).length;
  const entityPasses = okRuns.filter((r) => r.metrics.entityPass).length;
  const hydePasses = okRuns.filter((r) => r.metrics.hydePass).length;
  const overallPasses = okRuns.filter((r) => r.metrics.overallPass).length;
  const avgMs = average(results.map((r) => r.elapsedMs));
  const avgEntityRate = average(okRuns.map((r) => r.metrics.entityPreservationRate));
  const avgTokens = average(okRuns.map((r) => r.usage?.total_tokens ?? 0));

  return {
    model,
    totalRuns: results.length,
    okRuns: okRuns.length,
    formatPasses,
    entityPasses,
    hydePasses,
    overallPasses,
    avgMs,
    avgEntityRate,
    avgTokens,
    sampleFailures: results
      .filter((r) => !r.metrics.overallPass || !r.ok)
      .slice(0, 3)
      .map((r) => ({
        query: r.query,
        runIndex: r.runIndex,
        error: r.error,
        output: r.output,
        metrics: r.metrics,
      })),
  };
}

function buildMarkdownReport(
  summaries: ReturnType<typeof summarizeModel>[],
  allResults: RunResult[],
  outputJsonPath: string,
): string {
  const lines: string[] = [];
  lines.push("# Query Expansion Candidate Report");
  lines.push("");
  lines.push(`- Base URL: \`${BASE_URL}\``);
  lines.push(`- Runs per query: \`${RUNS_PER_QUERY}\``);
  lines.push(`- Query count: \`${TEST_QUERIES.length}\``);
  lines.push(`- Output JSON: \`${outputJsonPath}\``);
  lines.push("");
  lines.push("## Summary");
  lines.push("");
  lines.push("| Model | OK | Format | Entity | HyDE | Overall | Avg ms | Avg tokens |");
  lines.push("|---|---:|---:|---:|---:|---:|---:|---:|");

  for (const summary of summaries) {
    lines.push(
      `| ${summary.model} | ${percentage(summary.okRuns, summary.totalRuns)} | ${percentage(summary.formatPasses, summary.okRuns)} | ${percentage(summary.entityPasses, summary.okRuns)} | ${percentage(summary.hydePasses, summary.okRuns)} | ${percentage(summary.overallPasses, summary.okRuns)} | ${summary.avgMs.toFixed(0)} | ${summary.avgTokens.toFixed(0)} |`,
    );
  }

  lines.push("");
  lines.push("## Failure Samples");
  lines.push("");

  for (const summary of summaries) {
    if (summary.sampleFailures.length === 0) continue;
    lines.push(`### ${summary.model}`);
    lines.push("");
    for (const failure of summary.sampleFailures) {
      lines.push(`- Query: \`${failure.query}\` (run ${failure.runIndex})`);
      if (failure.error) lines.push(`  Error: \`${failure.error}\``);
      lines.push(`  Output: \`${failure.output.replace(/\n/g, " | ")}\``);
      lines.push(`  Flags: format=${failure.metrics.formatPass}, entity=${failure.metrics.entityPass}, hyde=${failure.metrics.hydePass}`);
    }
    lines.push("");
  }

  const bestOverall = [...summaries].sort((a, b) => b.overallPasses - a.overallPasses || a.avgMs - b.avgMs)[0];
  if (bestOverall) {
    lines.push("## Recommendation");
    lines.push("");
    lines.push(`- Best current candidate by this heuristic: \`${bestOverall.model}\``);
    lines.push("- Before using it in production, still do an actual retrieval A/B test against your own corpus.");
    lines.push("");
  }

  const rawFailureCount = allResults.filter((r) => !r.ok).length;
  if (rawFailureCount > 0) {
    lines.push("## Transport Errors");
    lines.push("");
    lines.push(`- ${rawFailureCount} runs failed at the API transport layer. Review the JSON report for details.`);
    lines.push("");
  }

  return lines.join("\n");
}

async function main() {
  if (EFFECTIVE_MODELS.length === 0) {
    throw new Error("No models configured. Set QE_MODELS or keep the default candidate list.");
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outputDir = join(process.cwd(), "tmp", "query-expansion-eval");
  mkdirSync(outputDir, { recursive: true });

  const allResults: RunResult[] = [];

  console.log(`Testing ${EFFECTIVE_MODELS.length} models x ${TEST_QUERIES.length} queries x ${RUNS_PER_QUERY} runs...`);
  console.log(`Base URL: ${BASE_URL}`);
  console.log(`Auth: ${API_KEY ? "Bearer token" : "none"}`);
  console.log("");

  for (const model of EFFECTIVE_MODELS) {
    console.log(`== ${model} ==`);

    for (const query of TEST_QUERIES) {
      for (let runIndex = 1; runIndex <= RUNS_PER_QUERY; runIndex++) {
        const start = Date.now();
        try {
          const { output, usage } = await callModel(model, query);
          const elapsedMs = Date.now() - start;
          const metrics = analyzeOutput(query, output);
          allResults.push({
            model,
            query,
            runIndex,
            elapsedMs,
            ok: true,
            output,
            metrics,
            usage,
          });
          const passMark = metrics.overallPass ? "PASS" : "WARN";
          console.log(`  [${passMark}] ${query} (#${runIndex}) ${elapsedMs}ms`);
        } catch (error) {
          const elapsedMs = Date.now() - start;
          const message = error instanceof Error ? error.message : String(error);
          allResults.push({
            model,
            query,
            runIndex,
            elapsedMs,
            ok: false,
            output: "",
            error: message,
            metrics: analyzeOutput(query, ""),
          });
          console.log(`  [FAIL] ${query} (#${runIndex}) ${elapsedMs}ms ${message}`);
        }
      }
    }

    console.log("");
  }

  const summaries = EFFECTIVE_MODELS.map((model) => summarizeModel(model, allResults.filter((r) => r.model === model)));
  const jsonPath = join(outputDir, `${timestamp}.json`);
  const mdPath = join(outputDir, `${timestamp}.md`);

  writeFileSync(jsonPath, JSON.stringify({
    config: {
      baseUrl: BASE_URL,
      runsPerQuery: RUNS_PER_QUERY,
      maxTokens: MAX_TOKENS,
      temperature: TEMPERATURE,
      models: EFFECTIVE_MODELS,
      queries: TEST_QUERIES,
    },
    summaries,
    results: allResults,
  }, null, 2), "utf-8");

  const markdown = buildMarkdownReport(summaries, allResults, jsonPath);
  writeFileSync(mdPath, markdown, "utf-8");

  console.log("Summary");
  console.log("| Model | OK | Format | Entity | HyDE | Overall | Avg ms |");
  console.log("|---|---:|---:|---:|---:|---:|---:|");
  for (const summary of summaries) {
    console.log(
      `| ${summary.model} | ${percentage(summary.okRuns, summary.totalRuns)} | ${percentage(summary.formatPasses, summary.okRuns)} | ${percentage(summary.entityPasses, summary.okRuns)} | ${percentage(summary.hydePasses, summary.okRuns)} | ${percentage(summary.overallPasses, summary.okRuns)} | ${summary.avgMs.toFixed(0)} |`,
    );
  }

  console.log("");
  console.log(`Wrote JSON report: ${jsonPath}`);
  console.log(`Wrote Markdown report: ${mdPath}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
