import time
from contextlib import contextmanager


@contextmanager
def phase(spark, phases: dict, name: str):
    """Record one phase's wall time and the jobs, stages and tasks Spark ran under it."""
    tracker = spark.sparkContext.statusTracker()
    group = f"{name}-{time.time_ns()}"
    spark.sparkContext.setJobGroup(group, name)
    started = time.perf_counter()
    try:
        yield
    finally:
        seconds = time.perf_counter() - started
        jobs = tracker.getJobIdsForGroup(group)
        stage_ids = [stage for job in jobs for stage in (tracker.getJobInfo(job).stageIds or [])]
        tasks = sum(tracker.getStageInfo(stage).numTasks for stage in stage_ids)
        phases[name] = {
            "seconds": round(seconds, 2),
            "job_ids": list(jobs),
            "stage_ids": stage_ids,
            "tasks": tasks,
        }
