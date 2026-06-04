from __future__ import annotations

from dataclasses import dataclass

from config.settings import Settings
from fb_ingest.pipeline.phase2_reduce import run_phase2_reduce
from fb_ingest.pipeline.phase2_stage import run_phase2_stage


@dataclass
class Phase2Result:
    stage_manifest: dict
    reduce_manifest: dict


def run_phase2(settings: Settings) -> Phase2Result:
    stage_manifest = run_phase2_stage(settings)
    reduce_manifest = run_phase2_reduce(settings)
    return Phase2Result(
        stage_manifest=stage_manifest,
        reduce_manifest=reduce_manifest,
    )
