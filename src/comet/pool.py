"""Process pool with shared progress tracking and tqdm integration.

Encapsulates the shared-counter + tqdm polling pattern into a reusable
run_pool() function. Workers call POOL_PROGRESS.increment() to advance the
main-process progress bar.
"""

import logging
import multiprocessing as mp
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import synchronize
from multiprocessing.sharedctypes import Synchronized
from typing import TypeVar

from tqdm import tqdm

log = logging.getLogger(__name__)

T = TypeVar("T")

POOL_PROGRESS: "PoolProgress | None" = None


class PoolProgress:
    """Shared progress state for process pool workers.

    Created once in the main process. Set as the module-level POOL_PROGRESS
    global in each worker via the initializer that run_pool() installs.

    Workers call increment() to advance the tqdm bar and check is_aborted()
    to detect sibling-worker crashes.
    """

    def __init__(self, extras: list[str] | None = None) -> None:
        ctx = mp.get_context("spawn")
        self.counter: Synchronized = ctx.Value("i", 0)
        self.lock: synchronize.Lock = ctx.Lock()
        self.abort: synchronize.Event = ctx.Event()
        self.extras: dict[str, Synchronized] = {
            name: ctx.Value("i", 0) for name in (extras or [])
        }

    def increment(self, n: int = 1) -> None:
        """Record *n* items of progress (updates tqdm in the main process)."""
        with self.lock:
            self.counter.value += n

    def increment_extra(self, name: str, n: int = 1) -> None:
        """Record *n* items of progress on a named extra counter."""
        with self.lock:
            self.extras[name].value += n

    def is_aborted(self) -> bool:
        """True if any worker has crashed and signalled abort."""
        return self.abort.is_set()


def init_pool_progress(
    progress: PoolProgress,
    user_init: Callable | None,
    user_initargs: tuple,
) -> None:
    """Combined worker initializer: sets POOL_PROGRESS, then runs user init."""
    global POOL_PROGRESS
    POOL_PROGRESS = progress
    if user_init is not None:
        user_init(*user_initargs)


def to_batches(items: list[T], batch_size: int) -> Iterator[list[T]]:
    """Yield successive batches of *batch_size* from *items*."""
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


def run_pool(
    tasks: list[T],
    worker_fn: Callable[[T], None],
    *,
    total: int,
    desc: str,
    unit: str = "item",
    max_workers: int | None = None,
    initializer: Callable | None = None,
    initargs: tuple = (),
    extras: dict[str, int] | None = None,
) -> None:
    """Submit tasks to a spawn-context process pool with live tqdm progress.

    Each task is submitted via executor.submit(worker_fn, task). Workers call
    POOL_PROGRESS.increment() to advance the progress bar from within their
    process. On worker exception the abort event is set so siblings can exit
    early.

    Args:
        tasks: Items to distribute across workers (one submit per item).
        worker_fn: Callable invoked once per task in a worker process.
        total: Total item count for the progress bar (may differ from
            len(tasks) when each task processes multiple items).
        desc: Progress bar description.
        unit: Progress bar unit label.
        max_workers: Pool size (None = cpu_count).
        initializer: Optional per-worker init callable (called after
            POOL_PROGRESS is installed).
        initargs: Arguments forwarded to *initializer*.
        extras: Optional named counters shown in the tqdm postfix.
            Maps counter name to its total, e.g. ``{"tars": 20}``.
            Workers advance these via POOL_PROGRESS.increment_extra(name).
    """
    if not tasks:
        return

    extra_names = list(extras.keys()) if extras else None
    progress = PoolProgress(extras=extra_names)
    last_seen = 0

    with (
        tqdm(total=total, desc=desc, unit=unit, smoothing=0) as pbar,
        ProcessPoolExecutor(
            mp_context=mp.get_context("spawn"),
            max_workers=max_workers,
            initializer=init_pool_progress,
            initargs=(progress, initializer, initargs),
        ) as executor,
    ):
        futures = [executor.submit(worker_fn, task) for task in tasks]

        while futures:
            with progress.lock:
                current = progress.counter.value
            delta = current - last_seen
            if delta > 0:
                pbar.update(delta)
                last_seen = current

            if extras:
                with progress.lock:
                    postfix = {
                        name: f"{progress.extras[name].value}/{extras[name]}"
                        for name in extras
                    }
                pbar.set_postfix(postfix, refresh=False)

            finished = []
            for future in futures:
                if future.done():
                    try:
                        future.result()
                    except Exception:
                        log.exception("Worker crashed, signaling abort")
                        progress.abort.set()
                    finally:
                        finished.append(future)

            for f in finished:
                futures.remove(f)

            if futures:
                time.sleep(1)
