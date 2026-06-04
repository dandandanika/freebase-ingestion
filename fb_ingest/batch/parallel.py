from __future__ import annotations

import os
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import TypeVar

from fb_ingest.logging_utils import get_logger

T = TypeVar("T")
R = TypeVar("R")


def resolve_workers(workers: int) -> int:
    """
    Resolve worker count.

    workers == 1: sequential (no pool)
    workers <= 0: auto (cpu_count - 1, minimum 1)
    workers > 1: use that many processes
    """
    if workers == 1:
        return 1
    if workers <= 0:
        return max(1, (os.cpu_count() or 4) - 1)
    return workers


def map_parallel(
    items: Iterable[T],
    worker_fn: Callable[[T], R],
    *,
    workers: int,
    label: str = "tasks",
    initializer: Callable[..., None] | None = None,
    initargs: tuple = (),
) -> list[R]:
    """
    Run worker_fn over items using a process pool when workers > 1.

    Preserves result order by indexing tasks.
    """
    task_list = list(items)
    if not task_list:
        return []

    worker_count = resolve_workers(workers)
    if worker_count == 1:
        return [worker_fn(item) for item in task_list]

    logger = get_logger("fb_ingest.parallel")
    logger.info("Running %s %s with %s workers", len(task_list), label, worker_count)

    indexed_results: dict[int, R] = {}
    with ProcessPoolExecutor(
        max_workers=worker_count,
        initializer=initializer,
        initargs=initargs,
    ) as pool:
        future_to_index = {
            pool.submit(worker_fn, item): index
            for index, item in enumerate(task_list)
        }
        completed = 0
        for future in as_completed(future_to_index):
            index = future_to_index[future]
            indexed_results[index] = future.result()
            completed += 1
            if completed % max(1, len(task_list) // 10) == 0 or completed == len(task_list):
                logger.info("Completed %s/%s %s", completed, len(task_list), label)

    return [indexed_results[index] for index in range(len(task_list))]


def merge_sample_dicts(target: dict[str, list], source: dict[str, list]) -> None:
    for category, records in source.items():
        bucket = target.setdefault(category, [])
        bucket.extend(records)
