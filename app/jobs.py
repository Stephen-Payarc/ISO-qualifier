"""
In-memory job store for pipeline runs.

Each upload creates a Job with a unique ID. The pipeline runner updates
the job's progress as contacts are processed. The frontend polls for
updates via SSE.
"""

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any


class JobStatus(str, Enum):
    QUEUED      = "queued"
    RUNNING     = "running"
    DONE        = "done"
    ERROR       = "error"


@dataclass
class Job:
    id: str
    original_filename: str
    input_path: Path
    output_dir: Path
    status: JobStatus = JobStatus.QUEUED
    stage: str = ""                  # "Stage 1" or "Stage 2"
    processed: int = 0
    total: int = 0
    summary: dict = field(default_factory=dict)
    error_message: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None

    # SSE subscribers: each is an asyncio.Queue that receives progress dicts
    _subscribers: list[asyncio.Queue] = field(default_factory=list, repr=False)

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        try:
            self._subscribers.remove(q)
        except ValueError:
            pass

    def _broadcast(self, event: dict) -> None:
        for q in list(self._subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass

    # ------------------------------------------------------------------
    # Progress helpers called by the pipeline runner
    # ------------------------------------------------------------------

    def set_total(self, total: int) -> None:
        self.total = total
        self._broadcast(self._snapshot())

    def update_progress(self, stage: str, processed: int) -> None:
        self.stage = stage
        self.processed = processed
        self.status = JobStatus.RUNNING
        self._broadcast(self._snapshot())

    def mark_done(self, summary: dict) -> None:
        self.status = JobStatus.DONE
        self.summary = summary
        self.processed = self.total
        self.completed_at = datetime.now(timezone.utc)
        self._broadcast(self._snapshot())
        # Signal EOF to all subscribers
        for q in list(self._subscribers):
            try:
                q.put_nowait(None)
            except asyncio.QueueFull:
                pass

    def mark_error(self, message: str) -> None:
        self.status = JobStatus.ERROR
        self.error_message = message
        self.completed_at = datetime.now(timezone.utc)
        self._broadcast(self._snapshot())
        for q in list(self._subscribers):
            try:
                q.put_nowait(None)
            except asyncio.QueueFull:
                pass

    def _snapshot(self) -> dict:
        pct = round(self.processed / self.total * 100, 1) if self.total else 0
        return {
            "job_id":    self.id,
            "status":    self.status,
            "stage":     self.stage,
            "processed": self.processed,
            "total":     self.total,
            "pct":       pct,
            "summary":   self.summary,
            "error":     self.error_message,
        }

    def to_dict(self) -> dict:
        return self._snapshot()


# ---------------------------------------------------------------------------
# Global job registry
# ---------------------------------------------------------------------------

_jobs: dict[str, Job] = {}


def create_job(original_filename: str, input_path: Path, output_dir: Path) -> Job:
    job_id = uuid.uuid4().hex
    job = Job(
        id=job_id,
        original_filename=original_filename,
        input_path=input_path,
        output_dir=output_dir,
    )
    _jobs[job_id] = job
    return job


def get_job(job_id: str) -> Job | None:
    return _jobs.get(job_id)


def all_jobs() -> list[Job]:
    return list(_jobs.values())
