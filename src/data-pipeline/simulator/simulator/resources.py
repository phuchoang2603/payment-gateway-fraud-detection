import io
import threading
import time

import psutil


def directory_bytes(root):
    total = 0
    for path in root.rglob("*"):
        try:
            if path.is_file():
                total += path.stat().st_size
        except FileNotFoundError:
            pass
    return total


class CapacityExceeded(OSError):
    pass


class LimitedWriter(io.BufferedWriter):
    def __init__(self, path, accounting, capacity):
        super().__init__(path.open("wb"))
        self.accounting = accounting
        self.capacity = capacity

    def write(self, data):
        if self.accounting[0] + len(data) > self.capacity:
            raise CapacityExceeded(
                "task scratch reservation exhausted; reduce customer block size or increase task_scratch_bytes"
            )
        result = super().write(data)
        self.accounting[0] += result
        return result


class ResourceMonitor:
    def __init__(self, root, execution):
        self.root = root
        self.execution = execution
        self.stop = threading.Event()
        self.error = None
        self.connection = None
        self.peaks = {"process_tree_rss_bytes": 0, "scratch_bytes": 0, "spill_bytes": 0}
        self.started = time.monotonic()
        self.thread = threading.Thread(target=self._run, daemon=True)

    def sample(self):
        process = psutil.Process()
        rss = 0
        for member in [process, *process.children(recursive=True)]:
            try:
                rss += member.memory_info().rss
            except psutil.Error:
                pass
        scratch = directory_bytes(self.root)
        spill = directory_bytes(self.root / "spill")
        for key, value in zip(self.peaks, (rss, scratch, spill)):
            self.peaks[key] = max(self.peaks[key], value)
        if scratch > self.execution.scratch_capacity_bytes:
            self.error = CapacityExceeded("combined scratch capacity exhausted; prepared output remains resumable")
        if spill > self.execution.duckdb_spill_bytes:
            self.error = CapacityExceeded("DuckDB spill capacity exhausted; prepared output remains resumable")
        if self.error and self.connection:
            self.connection.interrupt()

    def _run(self):
        while not self.stop.wait(0.1):
            self.sample()

    def __enter__(self):
        self.sample()
        self.thread.start()
        return self

    def check(self):
        self.sample()
        if self.error:
            raise self.error

    def report(self):
        return {
            "sample_interval_seconds": 0.1,
            "sampled_peaks": self.peaks.copy(),
            "elapsed_seconds": time.monotonic() - self.started,
        }

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join()
        self.sample()
