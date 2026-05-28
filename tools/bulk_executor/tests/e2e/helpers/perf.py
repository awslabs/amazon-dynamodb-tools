"""Resolve a Glue job run's DPU-seconds for the connector smoke report.

Wall-time comes from the wrapper's own log line (cheap, in stdout already).
DPU-seconds requires an extra ``glue.get_job_run`` call and is the
authoritative cost number.
"""
from __future__ import annotations

from dataclasses import dataclass

import boto3

GLUE_JOB_NAME = "bulk_dynamodb"  # the job name created by `bulk bootstrap`


@dataclass
class JobRunPerf:
    job_run_id: str
    execution_time_s: int          # seconds the job actually billed for
    max_capacity_dpu: float        # DPUs allocated
    dpu_seconds: float             # execution_time_s * max_capacity_dpu

    @classmethod
    def from_response(cls, run_id: str, response: dict) -> "JobRunPerf":
        run = response["JobRun"]
        exec_time = int(run.get("ExecutionTime", 0))
        capacity = float(run.get("MaxCapacity", 0))
        return cls(
            job_run_id=run_id,
            execution_time_s=exec_time,
            max_capacity_dpu=capacity,
            dpu_seconds=exec_time * capacity,
        )


def fetch_perf(job_run_id: str, region: str) -> JobRunPerf | None:
    """Fetch DPU-seconds for a finished Glue job run.

    Returns ``None`` if the job-run ID is missing (suite shouldn't crash
    the report rendering just because a single run's ID couldn't be
    scraped from stdout).
    """
    if not job_run_id:
        return None
    glue = boto3.client("glue", region_name=region)
    response = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=job_run_id)
    return JobRunPerf.from_response(job_run_id, response)
