"""
Evaluate the fine-tuned SyndrQL IR model against a held-out test set.

Usage:
    # Start vLLM first (see setup-vllm.sh), then:
    python evaluate-http.py [--data test_data.jsonl] [--max-eval 50] [--url http://127.0.0.1:8000]

Uses the /v1/chat/completions endpoint with the same system prompt and chat
format used during training, so the model sees the same turn structure it
learned from.

Reports three accuracy tiers:
  - Exact:      full IR object matches (strict)
  - Statements: only the statements array matches (ignores explanation/confidence)
  - Cleaned:    statements match after stripping null/empty values
"""

import json
import re
import sys
import argparse
import requests

# ---------------------------------------------------------------------------
# CLI args
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Evaluate SyndrQL IR model")
parser.add_argument("--data", default="test_data.jsonl",
                    help="Path to test JSONL (same format as training data)")
parser.add_argument("--max-eval", type=int, default=50,
                    help="Max examples to evaluate (0 = all)")
parser.add_argument("--url", default="http://127.0.0.1:8000",
                    help="vLLM server base URL")
parser.add_argument("--model", default="syndrql-model",
                    help="Model name as registered in vLLM (--served-model-name)")
parser.add_argument("--temperature", type=float, default=0.1,
                    help="Sampling temperature")
parser.add_argument("--verbose", action="store_true",
                    help="Print every example, not just failures")
parser.add_argument("--dump-raw", type=int, default=3,
                    help="Dump raw model output for the first N examples (0 to disable)")
args = parser.parse_args()

# ---------------------------------------------------------------------------
# System prompt — MUST match training exactly
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = (
    "You are a SyndrQL query generator. Given a natural language request, "
    "respond with a single JSON object matching the AIAssistantResponse schema. "
    "Output ONLY valid JSON — no explanation, no markdown, no extra text."
)

API_URL = f"{args.url}/v1/chat/completions"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def extract_json(text: str):
    """Try to extract a JSON object from model output.

    Handles three cases:
    1. Output is already pure JSON  (ideal — trained correctly)
    2. JSON is wrapped in ```json ... ``` markdown fences
    3. JSON object is embedded in prose text

    Returns the parsed object, or None if extraction fails.
    """
    text = text.strip()

    # Case 1: direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Case 2: markdown fenced code block
    fence_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?\s*```", text, re.DOTALL)
    if fence_match:
        try:
            return json.loads(fence_match.group(1).strip())
        except json.JSONDecodeError:
            pass

    # Case 3: find the first { ... } block (greedy)
    brace_match = re.search(r"\{.*\}", text, re.DOTALL)
    if brace_match:
        try:
            return json.loads(brace_match.group(0))
        except json.JSONDecodeError:
            pass

    return None


def is_empty_value(v):
    """Check if a value is semantically empty (null, empty string, empty dict/list, False, 0)."""
    if v is None:
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    if isinstance(v, dict) and len(v) == 0:
        return True
    if isinstance(v, list) and len(v) == 0:
        return True
    if v is False:
        return True
    if isinstance(v, (int, float)) and v == 0:
        return True
    return False


def normalize(obj):
    """Normalize a JSON object for comparison.

    - Sort dict keys for stable comparison
    - Round floats to 2 decimal places
    - Strip whitespace from strings
    """
    if isinstance(obj, dict):
        return {k: normalize(v) for k, v in sorted(obj.items())}
    elif isinstance(obj, list):
        return [normalize(item) for item in obj]
    elif isinstance(obj, float):
        return round(obj, 2)
    elif isinstance(obj, str):
        return obj.strip()
    return obj


def strip_empty(obj):
    """Recursively remove keys with null/empty/zero/false values.

    This handles the case where the model outputs all possible statement-type
    keys with null values (e.g., "beginTransaction": null, "checkpoint": null).
    The Go generator uses omitempty so the gold data doesn't have these, but
    the model sometimes includes them.  Stripping them reveals whether the
    actual content is correct.
    """
    if isinstance(obj, dict):
        cleaned = {}
        for k, v in obj.items():
            v_cleaned = strip_empty(v)
            if not is_empty_value(v_cleaned):
                cleaned[k] = v_cleaned
        return cleaned
    elif isinstance(obj, list):
        return [strip_empty(item) for item in obj]
    return obj


def extract_statements(ir, clean=False):
    """Extract just the statements array from an IR object.

    This is the part that actually compiles to SyndrQL — explanation and
    confidence are metadata that don't affect the query output.

    If clean=True, strip null/empty values first.
    """
    if isinstance(ir, dict):
        stmts = ir.get("statements", [])
        if clean:
            stmts = strip_empty(stmts)
        return normalize(stmts)
    return None


def diff_objects(expected, got, path=""):
    """Return a list of human-readable differences between two normalized objects."""
    diffs = []
    if type(expected) != type(got):
        diffs.append(f"  {path}: type {type(expected).__name__} vs {type(got).__name__}")
        return diffs
    if isinstance(expected, dict):
        all_keys = sorted(set(list(expected.keys()) + list(got.keys())))
        for k in all_keys:
            child_path = f"{path}.{k}" if path else k
            if k not in expected:
                diffs.append(f"  {child_path}: EXTRA key in model output = {json.dumps(got[k])[:80]}")
            elif k not in got:
                diffs.append(f"  {child_path}: MISSING from model output (expected {json.dumps(expected[k])[:80]})")
            else:
                diffs.extend(diff_objects(expected[k], got[k], child_path))
    elif isinstance(expected, list):
        if len(expected) != len(got):
            diffs.append(f"  {path}: list length {len(expected)} vs {len(got)}")
        for i in range(min(len(expected), len(got))):
            diffs.extend(diff_objects(expected[i], got[i], f"{path}[{i}]"))
    else:
        if expected != got:
            exp_str = json.dumps(expected) if not isinstance(expected, str) else expected
            got_str = json.dumps(got) if not isinstance(got, str) else got
            if len(str(exp_str)) > 80:
                exp_str = str(exp_str)[:80] + "..."
            if len(str(got_str)) > 80:
                got_str = str(got_str)[:80] + "..."
            diffs.append(f"  {path}: {exp_str!r} vs {got_str!r}")
    return diffs


def query_model(nl_prompt: str) -> str:
    """Send a chat completion request to vLLM and return the assistant text."""
    payload = {
        "model": args.model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": nl_prompt},
        ],
        "temperature": args.temperature,
        "max_tokens": 1800,
    }
    resp = requests.post(API_URL, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


# ---------------------------------------------------------------------------
# Main evaluation loop
# ---------------------------------------------------------------------------
def main():
    with open(args.data) as f:
        tests = [json.loads(line) for line in f]

    if args.max_eval > 0:
        tests = tests[:args.max_eval]

    print(f"Evaluating {len(tests)} examples against {API_URL}")
    print(f"Model: {args.model}")
    print("-" * 70)

    # Counters
    exact_correct = 0       # Full IR matches exactly
    stmt_correct = 0        # statements array matches (raw)
    cleaned_correct = 0     # statements match after stripping empty values
    parse_failures = 0
    errors = 0
    failure_details = []

    for i, test in enumerate(tests):
        nl = test["nl"]
        gold_ir = test["ir"]

        try:
            raw_output = query_model(nl)
        except requests.RequestException as e:
            print(f"  [{i+1}] ERROR: request failed: {e}")
            errors += 1
            continue

        # Dump raw output for first N examples
        if i < args.dump_raw:
            print(f"\n  === RAW OUTPUT [{i+1}]: {nl} ===")
            print(f"  {raw_output[:600]}")
            print(f"  === END RAW ===\n")

        predicted = extract_json(raw_output)

        if predicted is None:
            parse_failures += 1
            failure_details.append({
                "index": i + 1,
                "type": "parse_failure",
                "nl": nl,
                "raw_output": raw_output[:500],
            })
            if args.verbose:
                print(f"  [{i+1}] PARSE FAIL: {nl}")
            continue

        # --- Tier 1: Exact IR match ---
        gold_norm = normalize(gold_ir)
        pred_norm = normalize(predicted)
        is_exact = (gold_norm == pred_norm)
        if is_exact:
            exact_correct += 1

        # --- Tier 2: Statements-only match (raw) ---
        gold_stmts = extract_statements(gold_ir, clean=False)
        pred_stmts = extract_statements(predicted, clean=False)
        is_stmt_match = (gold_stmts == pred_stmts)
        if is_stmt_match:
            stmt_correct += 1

        # --- Tier 3: Statements match after stripping empties ---
        gold_stmts_clean = extract_statements(gold_ir, clean=True)
        pred_stmts_clean = extract_statements(predicted, clean=True)
        is_cleaned_match = (gold_stmts_clean == pred_stmts_clean)
        if is_cleaned_match:
            cleaned_correct += 1

        # --- Logging ---
        if is_exact:
            if args.verbose:
                print(f"  [{i+1}] EXACT:   {nl}")
        elif is_cleaned_match:
            if args.verbose:
                print(f"  [{i+1}] CLEAN:   {nl}  (match after stripping null/empty keys)")
        elif is_stmt_match:
            if args.verbose:
                print(f"  [{i+1}] STMT:    {nl}  (raw statements match)")
        else:
            if args.verbose:
                print(f"  [{i+1}] MISS:    {nl}")

            # Diff the CLEANED statements so we focus on real differences
            diffs = diff_objects(gold_stmts_clean, pred_stmts_clean, "statements")
            failure_details.append({
                "index": i + 1,
                "type": "stmt_mismatch",
                "nl": nl,
                "diffs": diffs[:10],
                "gold_stmts": json.dumps(gold_stmts_clean, indent=None)[:400],
                "pred_stmts": json.dumps(pred_stmts_clean, indent=None)[:400],
            })

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    total = len(tests)
    evaluated = total - errors
    exact_pct = exact_correct / evaluated * 100 if evaluated else 0
    stmt_pct = stmt_correct / evaluated * 100 if evaluated else 0
    cleaned_pct = cleaned_correct / evaluated * 100 if evaluated else 0

    print()
    print("=" * 70)
    print("RESULTS")
    print("=" * 70)
    print(f"Total examples:       {total}")
    print(f"Request errors:       {errors}")
    print(f"Parse failures:       {parse_failures}  (model didn't output valid JSON)")
    print(f"Evaluated:            {evaluated}")
    print()
    print(f"Exact IR match:       {exact_correct}/{evaluated} = {exact_pct:.1f}%")
    print(f"  (full IR object including explanation/confidence)")
    print(f"Statements match:     {stmt_correct}/{evaluated} = {stmt_pct:.1f}%")
    print(f"  (raw statements array comparison)")
    print(f"Cleaned match:        {cleaned_correct}/{evaluated} = {cleaned_pct:.1f}%  <-- primary metric")
    print(f"  (statements after stripping null/empty keys)")
    print()

    # The cleaned match is the real accuracy metric
    if cleaned_pct >= 90:
        print("PASS: Cleaned statements accuracy >= 90%")
    elif cleaned_pct >= 70:
        print("MARGINAL: Consider more training data or stepping up to 7B model")
    else:
        print("NEEDS WORK: See failure details below for diagnosis")

    # Show failure details
    if failure_details:
        show = min(15, len(failure_details))
        print(f"\nFirst {show} failures (of {len(failure_details)}):")
        print("-" * 70)
        for ex in failure_details[:show]:
            print(f"  [{ex['index']}] {ex['type']}: {ex['nl']}")
            if ex["type"] == "parse_failure":
                print(f"       Raw output: {ex['raw_output'][:200]}")
            elif ex["type"] == "stmt_mismatch":
                if ex["diffs"]:
                    for d in ex["diffs"]:
                        print(f"       {d}")
                else:
                    print(f"       (no diffs found — check gold_stmts vs pred_stmts)")
                    print(f"       Expected: {ex['gold_stmts'][:200]}")
                    print(f"       Got:      {ex['pred_stmts'][:200]}")
            print()

    # Exit code: success if cleaned accuracy >= 90%
    sys.exit(0 if cleaned_pct >= 90 else 1)


if __name__ == "__main__":
    main()
