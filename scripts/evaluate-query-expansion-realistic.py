import json
import os
import re
import statistics
import time
import urllib.error
import urllib.request
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "finetune"))

from reward import clean_model_output, score_expansion_detailed  # type: ignore


API_KEY = os.environ.get("QE_API_KEY", "") or os.environ.get("SILICONFLOW_API_KEY", "")
BASE_URL = os.environ.get("QE_BASE_URL", "") or os.environ.get("SILICONFLOW_BASE_URL", "https://api.siliconflow.cn/v1")
REQUEST_TIMEOUT = int(os.environ.get("QE_TIMEOUT_MS", "45000")) / 1000
MAX_TOKENS = int(os.environ.get("QE_MAX_TOKENS", "400"))
TEMPERATURE = float(os.environ.get("QE_TEMPERATURE", "0.2"))
RUNS_PER_QUERY = int(os.environ.get("QE_RUNS_PER_QUERY", "1"))
REQUEST_GAP_MS = int(os.environ.get("QE_REQUEST_GAP_MS", "0"))
MAX_RETRIES = int(os.environ.get("QE_MAX_RETRIES", "0"))

DEFAULT_MODELS = [
    "THUDM/GLM-4-9B-0414",
    "deepseek-ai/DeepSeek-R1-0528-Qwen3-8B",
    "THUDM/GLM-Z1-9B-0414",
    "deepseek-ai/DeepSeek-R1-Distill-Qwen-7B",
    "Qwen/Qwen2.5-7B-Instruct",
    "internlm/internlm2_5-7b-chat",
    "Pro/Qwen/Qwen2.5-7B-Instruct",
]
MODELS = [
    model.strip()
    for model in os.environ.get("QE_MODELS", ",".join(DEFAULT_MODELS)).split(",")
    if model.strip()
]
MODEL_SPECS_JSON = os.environ.get("QE_MODEL_SPECS_JSON", "").strip()

OFFICIAL_QUERIES = [
    "how to configure authentication",
    "typescript async await",
    "docker compose networking",
    "auth",
    "config",
    "api",
    "who is TDS motorsports",
    "React hooks tutorial",
    "AWS Lambda functions",
    "meeting notes project kickoff",
    "what is dependency injection",
    "connection timeout error",
    "latest AI developments",
    "how to implement caching with redis in nodejs",
    "meeting with Bob about C++",
    "notes from the Project Atlas kickoff",
    "natural language processing transformers",
    "visual studio code extensions",
    "rust ownership and borrowing",
    "python web scraping beautiful soup",
    "auth /only:lex",
    "kubernetes pod deployment /only:vec",
    "AWS Lambda cold start /only:hyde",
]

PROJECT_QUERIES = [
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
    "qmd status remote model",
    "query expansion output format",
    "rerank max_documents",
    "base_url openai-compatible",
    "AUTH_SECRET environment variable",
]

TEST_QUERIES = OFFICIAL_QUERIES + PROJECT_QUERIES

SYSTEM_PROMPT = "You are a search query expansion engine. Output only typed lines."


def build_user_prompt(query: str) -> str:
    return "\n".join(
        [
            "Expand this search query into typed lines.",
            "Return one item per line using exactly one of these prefixes: lex:, vec:, hyde:.",
            "lex lines are short keyword phrases for BM25.",
            "vec lines are natural-language semantic search variants.",
            "hyde is one concise hypothetical answer passage.",
            "Do not include prose or markdown fences.",
            "",
            f"Query: {query}",
        ]
    )


def get_message_text(payload: dict) -> str:
    content = payload["choices"][0]["message"].get("content", "")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        text_parts = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text_parts.append(item.get("text", ""))
        return "".join(text_parts).strip()
    return ""


def default_model_specs() -> list[dict]:
    return [
        {
            "label": model,
            "model": model,
            "base_url": BASE_URL,
            "path": "/chat/completions",
            "api_key": API_KEY,
            "provider": "openai-compatible",
            "extra_body": {},
        }
        for model in MODELS
    ]


def load_model_specs() -> list[dict]:
    if not MODEL_SPECS_JSON:
        return default_model_specs()

    raw_specs = json.loads(MODEL_SPECS_JSON)
    specs = []
    for item in raw_specs:
        specs.append(
            {
                "label": item.get("label") or item["model"],
                "model": item["model"],
                "base_url": item.get("base_url", BASE_URL),
                "path": item.get("path", "/chat/completions"),
                "api_key": item.get("api_key", API_KEY),
                "provider": item.get("provider", "openai-compatible"),
                "extra_body": item.get("extra_body", {}),
            }
        )
    return specs


def call_model(spec: dict, query: str) -> tuple[str, float, dict]:
    body = {
        "model": spec["model"],
        "temperature": TEMPERATURE,
        "max_tokens": MAX_TOKENS,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": build_user_prompt(query)},
        ],
    }
    body.update(spec.get("extra_body", {}))
    headers = {"Content-Type": "application/json"}
    if spec.get("api_key"):
        headers["Authorization"] = f"Bearer {spec['api_key']}"

    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        req = urllib.request.Request(
            f"{spec['base_url'].rstrip('/')}{spec['path']}",
            data=json.dumps(body).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        start = time.time()
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            elapsed_ms = (time.time() - start) * 1000
            content = get_message_text(payload)
            return content, elapsed_ms, payload.get("usage", {})
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code == 429 and attempt < MAX_RETRIES:
                time.sleep(min(10, 2 * (attempt + 1)))
                continue
            raise
        except Exception as exc:
            last_error = exc
            raise
    raise RuntimeError(str(last_error))


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * p)))
    return float(ordered[idx])


def is_strict_prod_pass(result: dict, cleaned_output: str, used_thinking: bool) -> bool:
    parsed = result.get("parsed", {})
    return (
        not used_thinking
        and result.get("rating") in {"Excellent", "Good", "Acceptable"}
        and parsed.get("lex")
        and parsed.get("vec")
        and parsed.get("hyde")
        and cleaned_output.strip() == cleaned_output
    )


def summarize_model(model: str, results: list[dict]) -> dict:
    ok_results = [item for item in results if not item.get("error")]
    scores = [item["score"]["percentage"] for item in ok_results]
    latencies = [item["elapsed_ms"] for item in ok_results]
    strict_prod_passes = sum(
        1 for item in ok_results
        if is_strict_prod_pass(item["score"], item["cleaned_output"], item["used_thinking"])
    )
    acceptable_plus = sum(1 for item in ok_results if item["score"]["rating"] in {"Excellent", "Good", "Acceptable"})
    good_plus = sum(1 for item in ok_results if item["score"]["rating"] in {"Excellent", "Good"})
    excellent = sum(1 for item in ok_results if item["score"]["rating"] == "Excellent")
    failed = sum(1 for item in ok_results if item["score"]["rating"] == "Failed")
    think_leaks = sum(1 for item in ok_results if item["used_thinking"])
    transport_errors = len(results) - len(ok_results)
    format_complete = sum(
        1 for item in ok_results
        if item["score"].get("parsed", {}).get("lex")
        and item["score"].get("parsed", {}).get("vec")
        and item["score"].get("parsed", {}).get("hyde")
    )

    return {
        "model": model,
        "total_runs": len(results),
        "avg_score": round(sum(scores) / len(scores), 1) if scores else 0.0,
        "median_score": round(statistics.median(scores), 1) if scores else 0.0,
        "avg_ms": round(sum(latencies) / len(latencies), 0) if latencies else 0.0,
        "p95_ms": round(percentile(latencies, 0.95), 0) if latencies else 0.0,
        "acceptable_plus": acceptable_plus,
        "good_plus": good_plus,
        "excellent": excellent,
        "failed": failed,
        "strict_prod_passes": strict_prod_passes,
        "format_complete": format_complete,
        "think_leaks": think_leaks,
        "transport_errors": transport_errors,
        "top_failures": [
            {
                "query": item["query"],
                "rating": item["score"]["rating"],
                "score": item["score"]["percentage"],
                "used_thinking": item["used_thinking"],
                "deductions": item["score"]["deductions"][:4],
                "cleaned_output": item["cleaned_output"],
            }
            for item in results
            if item.get("error") or item["score"]["rating"] in {"Failed", "Poor"}
        ][:5],
    }


def build_markdown_report(output_json_path: str, summaries: list[dict], runs: list[dict]) -> str:
    model_specs = load_model_specs()
    lines = [
        "# Query Expansion Realistic Evaluation",
        "",
        f"- Query count: `{len(TEST_QUERIES)}`",
        f"- Runs per query: `{RUNS_PER_QUERY}`",
        f"- Models: `{len(model_specs)}`",
        f"- Prompt path: `src/remote-query-expansion.ts`",
        f"- Scoring path: `finetune/reward.py`",
        f"- Output JSON: `{output_json_path}`",
        "",
        "## Model Specs",
        "",
    ]

    for spec in model_specs:
        lines.append(
            f"- `{spec['label']}` -> provider=`{spec['provider']}` base_url=`{spec['base_url']}` path=`{spec['path']}` model=`{spec['model']}`"
        )

    lines.extend(
        [
        "",
        "## Query Mix",
        "",
        f"- Official-style upstream eval queries: `{len(OFFICIAL_QUERIES)}`",
        f"- Project-specific real-world queries: `{len(PROJECT_QUERIES)}`",
        "",
        "## Summary",
        "",
        "| Model | Avg score | Median | Acceptable+ | Good+ | Excellent | Strict prod pass | Format complete | Think leaks | Avg ms | P95 ms | Errors |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])

    for summary in summaries:
        lines.append(
            f"| {summary['model']} | {summary['avg_score']:.1f} | {summary['median_score']:.1f} | "
            f"{summary['acceptable_plus']}/{summary['total_runs']} | {summary['good_plus']}/{summary['total_runs']} | "
            f"{summary['excellent']}/{summary['total_runs']} | {summary['strict_prod_passes']}/{summary['total_runs']} | "
            f"{summary['format_complete']}/{summary['total_runs']} | {summary['think_leaks']}/{summary['total_runs']} | "
            f"{int(summary['avg_ms'])} | {int(summary['p95_ms'])} | {summary['transport_errors']} |"
        )

    lines.extend(["", "## Failure Samples", ""])

    for summary in summaries:
        if not summary["top_failures"]:
            continue
        lines.append(f"### {summary['model']}")
        lines.append("")
        for item in summary["top_failures"]:
            lines.append(
                f"- `{item['query']}` score={item['score']} rating={item['rating']} think={item['used_thinking']}"
            )
            if item["deductions"]:
                lines.append(f"  deductions=`{' ; '.join(item['deductions'])}`")
            if item["cleaned_output"]:
                lines.append(f"  output=`{item['cleaned_output'].replace(chr(10), ' | ')}`")
        lines.append("")

    best = sorted(
        summaries,
        key=lambda item: (
            -item["strict_prod_passes"],
            -item["acceptable_plus"],
            -item["avg_score"],
            item["avg_ms"],
        ),
    )[0]
    lines.extend(
        [
            "## Recommendation",
            "",
            f"- Best current candidate by realistic QMD-style scoring: `{best['model']}`",
            "- Priority order favors strict production-pass count, then acceptable+ coverage, then average score, then latency.",
            "",
        ]
    )

    return "\n".join(lines)


def main() -> None:
    model_specs = load_model_specs()
    if not model_specs:
        raise RuntimeError("No model specs configured.")
    if not any(spec.get("api_key") for spec in model_specs):
        raise RuntimeError("Missing QE_API_KEY or SILICONFLOW_API_KEY.")

    timestamp = time.strftime("%Y-%m-%dT%H-%M-%S")
    model_tag = re.sub(r"[^A-Za-z0-9._-]+", "_", "-".join(spec["label"] for spec in model_specs))[:80]
    output_dir = ROOT / "tmp" / "query-expansion-eval"
    output_dir.mkdir(parents=True, exist_ok=True)

    runs: list[dict] = []
    for spec in model_specs:
        model_results = []
        label = spec["label"]
        print(f"== {label} ==")
        for query in TEST_QUERIES:
            for run_index in range(1, RUNS_PER_QUERY + 1):
                try:
                    output, elapsed_ms, usage = call_model(spec, query)
                    cleaned_output, used_thinking = clean_model_output(output)
                    score = score_expansion_detailed(query, output)
                    model_results.append(
                        {
                            "query": query,
                            "run_index": run_index,
                            "elapsed_ms": elapsed_ms,
                            "output": output,
                            "cleaned_output": cleaned_output,
                            "used_thinking": used_thinking,
                            "usage": usage,
                            "score": score,
                        }
                    )
                    print(f"  [OK] {query} #{run_index} {elapsed_ms:.0f}ms {score['rating']} {score['percentage']}")
                except Exception as exc:
                    model_results.append(
                        {
                            "query": query,
                            "run_index": run_index,
                            "elapsed_ms": 0.0,
                            "error": str(exc),
                            "output": "",
                            "cleaned_output": "",
                            "used_thinking": False,
                            "usage": {},
                            "score": {
                                "percentage": 0.0,
                                "rating": "Failed",
                                "deductions": [str(exc)],
                                "parsed": {"lex": [], "vec": [], "hyde": [], "invalid": []},
                            },
                        }
                    )
                    print(f"  [FAIL] {query} #{run_index} {exc}")
                if REQUEST_GAP_MS > 0:
                    time.sleep(REQUEST_GAP_MS / 1000)
        runs.append({"model": label, "spec": spec, "results": model_results})
        print("")

    summaries = [summarize_model(run["model"], run["results"]) for run in runs]
    summaries.sort(
        key=lambda item: (
            -item["strict_prod_passes"],
            -item["acceptable_plus"],
            -item["avg_score"],
            item["avg_ms"],
        )
    )

    json_path = output_dir / f"{timestamp}-{model_tag}-realistic.json"
    md_path = output_dir / f"{timestamp}-{model_tag}-realistic.md"

    payload = {
        "config": {
            "model_specs": model_specs,
            "runs_per_query": RUNS_PER_QUERY,
            "max_tokens": MAX_TOKENS,
            "temperature": TEMPERATURE,
            "query_count": len(TEST_QUERIES),
            "official_queries": OFFICIAL_QUERIES,
            "project_queries": PROJECT_QUERIES,
        },
        "summaries": summaries,
        "runs": runs,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(build_markdown_report(str(json_path), summaries, runs), encoding="utf-8")

    print("")
    print("Summary")
    print("| Model | Avg score | Acceptable+ | Strict prod pass | Think leaks | Avg ms |")
    print("|---|---:|---:|---:|---:|---:|")
    for summary in summaries:
        print(
            f"| {summary['model']} | {summary['avg_score']:.1f} | "
            f"{summary['acceptable_plus']}/{summary['total_runs']} | "
            f"{summary['strict_prod_passes']}/{summary['total_runs']} | "
            f"{summary['think_leaks']}/{summary['total_runs']} | {int(summary['avg_ms'])} |"
        )

    print("")
    print(f"Wrote JSON report: {json_path}")
    print(f"Wrote Markdown report: {md_path}")


if __name__ == "__main__":
    main()
