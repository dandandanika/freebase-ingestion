#!/usr/bin/env bash
# Quick end-to-end smoke test (~1000 triples, CVT coverage, fast hash embeddings).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
INPUT="${INPUT:-$ROOT/testdata/sample_1k.nt}"
WORK="${WORK:-$ROOT/testdata/work_smoke}"
PARTS="${PARTS:-8}"
TARGET_TRIPLES="${TARGET_TRIPLES:-1000}"
SKIP_ENRICH="${SKIP_ENRICH:-0}"
SAMPLE_COUNT="${SAMPLE_COUNT:-5}"

echo "==> Generating ~${TARGET_TRIPLES} triples -> ${INPUT}"
python3 "$ROOT/scripts/generate_sample_data.py" \
  --output "$INPUT" \
  --target-triples "$TARGET_TRIPLES"

echo "==> Cleaning work dir: ${WORK}"
rm -rf "$WORK"
mkdir -p "$WORK"

run_phase() {
  echo ""
  echo "==> $1"
  shift
  python3 -m fb_ingest.cli "$@"
}

run_phase "Phase 1" phase1 \
  --input "$INPUT" \
  --work-dir "$WORK" \
  --partition-count "$PARTS" \
  --spool-max-records 5000 \
  --log-every 200 \
  --sample-count "$SAMPLE_COUNT"

run_phase "Phase 2" phase2 \
  --input "$INPUT" \
  --work-dir "$WORK" \
  --partition-count "$PARTS" \
  --spool-max-records 5000 \
  --log-every 200 \
  --sample-count "$SAMPLE_COUNT"

run_phase "Refactor CVTs" refactor-cvts \
  --input "$INPUT" \
  --work-dir "$WORK" \
  --partition-count "$PARTS" \
  --sample-count "$SAMPLE_COUNT"

if [[ "$SKIP_ENRICH" == "1" ]]; then
  echo ""
  echo "==> Skipping enrich-and-embed (SKIP_ENRICH=1)"
else
  run_phase "Enrich (descriptions only, no embeddings)" enrich-and-embed \
    --input "$INPUT" \
    --work-dir "$WORK" \
    --partition-count "$PARTS" \
    --input-phase refactor_cvts \
    --skip-embeddings \
    --sample-count "$SAMPLE_COUNT"
fi

echo ""
echo "==> Summary"
python3 - <<PY
import json
from pathlib import Path

work = Path("$WORK")
checks = {
    "phase1_mediator_types": work / "phase1/stats/phase1_stats.json",
    "phase2_stage": work / "phase2/stats/phase2_stage_stats.json",
    "phase2_reduce": work / "phase2/stats/phase2_reduce_stats.json",
    "refactor_cvts": work / "phase3/cvt_refactor/stats/refactor_cvts_stats.json",
}
if "$SKIP_ENRICH" != "1":
    checks["enrich_embed"] = work / "phase3/enriched/stats/enrich_embed_stats.json"

ok = True
for label, path in checks.items():
    data = json.loads(path.read_text())
    print(f"{label}: {json.dumps(data, indent=2)}")

stage = json.loads(checks["phase2_stage"].read_text())
reduce = json.loads(checks["phase2_reduce"].read_text())
refactor = json.loads(checks["refactor_cvts"].read_text())

if stage.get("cvt_incoming_facts", 0) < 1:
    print("FAIL: expected cvt_incoming_facts >= 1")
    ok = False
if stage.get("cvt_entity_out_facts", 0) < 1:
    print("FAIL: expected cvt_entity_out_facts >= 1")
    ok = False
if refactor.get("flattened_cvts", 0) < 1:
    print("FAIL: expected flattened_cvts >= 1")
    ok = False

if "$SKIP_ENRICH" != "1":
    enrich = json.loads(checks["enrich_embed"].read_text())
    if enrich.get("nodes_enriched", 0) < 1:
        print("FAIL: expected nodes_enriched >= 1")
        ok = False

sample_paths = [
    work / "phase1/samples/samples.json",
    work / "phase2/samples/stage_samples.json",
    work / "phase2/samples/reduce_samples.json",
    work / "phase3/cvt_refactor/samples/samples.json",
]
if "$SKIP_ENRICH" != "1":
    sample_paths.append(work / "phase3/enriched/samples/samples.json")

for sample_path in sample_paths:
    if not sample_path.exists():
        print(f"FAIL: missing sample file {sample_path}")
        ok = False
        continue
    data = json.loads(sample_path.read_text())
    cats = data.get("categories", {})
    print(f"samples: {sample_path.name} categories={sorted(cats.keys())}")
    if "cvt_incoming" not in cats and sample_path.name == "stage_samples.json":
        print("FAIL: stage samples missing cvt_incoming category")
        ok = False
    if "flattened_cvt_edge" not in cats and sample_path.name == "reduce_samples.json":
        print("FAIL: reduce samples missing flattened_cvt_edge category")
        ok = False

if ok:
    print("PASS: smoke test succeeded")
else:
    raise SystemExit(1)
PY

echo ""
echo "Done. Input: ${INPUT}"
echo "Work artifacts: ${WORK}"
