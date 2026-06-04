from __future__ import annotations

import csv
import json
from pathlib import Path

from fb_ingest.batch.reducers import iter_jsonl_dir
from fb_ingest.pipeline.phase2_stage import phase2_paths


def _json_value(value):
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return value


def export_nodes_csv(work_dir: Path, out_dir: Path) -> Path:
    paths = phase2_paths(work_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "nodes.csv"

    with open(out_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["mid:ID(Entity-ID)", "name", "description", "fb_types:string[]", ":LABEL"])

        for _, row in iter_jsonl_dir(paths["reduce_nodes"]):
            props = row.get("properties", {})
            labels = row.get("labels", ["Entity"])
            fb_types = row.get("fb_types", [])
            writer.writerow([
                row["mid"],
                props.get("name"),
                props.get("description"),
                ";".join(fb_types),
                ";".join(labels),
            ])

    return out_path


def export_edges_csv(work_dir: Path, out_dir: Path) -> Path:
    paths = phase2_paths(work_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "relationships.csv"

    with open(out_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            ":START_ID(Entity-ID)",
            ":END_ID(Entity-ID)",
            ":TYPE",
            "predicate",
            "properties"
        ])

        for _, row in iter_jsonl_dir(paths["reduce_edges"]):
            writer.writerow([
                row["source_mid"],
                row["target_mid"],
                row["rel_type"],
                row.get("predicate"),
                json.dumps(row.get("properties", {}), ensure_ascii=False),
            ])

    return out_path
