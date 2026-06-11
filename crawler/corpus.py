import asyncio
import contextlib
import time

from .storage import Backend


class BufferedCorpusWriter:
    """Keeps DB writes off the crawl hot path.

    The request handler only does `add(record)` (a near-instant queue put). A
    background task drains the queue and writes in batches via the backend
    (COPY on postgres, executemany on sqlite), triggered by size OR time. This
    is the answer to "don't write to the DB on every crawl".
    """

    def __init__(self, backend: Backend, max_records: int, max_seconds: float):
        self.backend = backend
        self.max_records = max_records
        self.max_seconds = max_seconds
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=max_records * 20)
        self._task: asyncio.Task | None = None
        self._err_buf: list[tuple] = []
        self.written = 0

    def start(self):
        self._task = asyncio.create_task(self._run(), name="corpus-flusher")

    async def add(self, record: dict):
        # awaits only if the buffer is way backed up (backpressure), which is
        # the one case where we *want* to slow the crawl down
        await self.queue.put(record)

    def add_error(self, url, node_id, kind, message):
        self._err_buf.append((url, node_id, time.time(), kind, str(message)[:2000]))

    async def _run(self):
        batch: list[dict] = []
        deadline = time.monotonic() + self.max_seconds
        while True:
            timeout = max(0.0, deadline - time.monotonic())
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=timeout)
                if item is None:           # shutdown sentinel
                    await self._flush(batch)
                    return
                batch.append(item)
            except asyncio.TimeoutError:
                pass

            if len(batch) >= self.max_records or time.monotonic() >= deadline:
                await self._flush(batch)
                batch = []
                deadline = time.monotonic() + self.max_seconds

    async def _flush(self, batch):
        if batch:
            try:
                await self.backend.write_corpus(batch)
                self.written += len(batch)
            except Exception as e:
                # never let a write error kill the crawl. log and drop.
                self.add_error("<corpus-flush>", "", type(e).__name__, e)
        if self._err_buf:
            errs, self._err_buf = self._err_buf, []
            with contextlib.suppress(Exception):
                await self.backend.write_errors(errs)

    async def stop(self):
        # graceful: flush everything still queued before we exit
        if self._task:
            await self.queue.put(None)
            await self._task
