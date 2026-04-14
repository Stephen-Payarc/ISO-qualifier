"""
FastAPI web application for the ISO Lead Qualification Pipeline.

Routes:
  GET  /                    — UI
  POST /upload              — accept xlsx or csv, start pipeline job
  GET  /jobs/{id}/progress  — SSE stream of job progress
  GET  /jobs/{id}/status    — JSON snapshot of job state
  GET  /jobs/{id}/download/csv   — download qualified.csv
  GET  /jobs/{id}/download/xlsx  — download qualified.xlsx
"""

import asyncio
import json
import logging
import os
import shutil
import tempfile
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from app.jobs import Job, JobStatus, create_job, get_job
from pipeline import runner

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="ISO Lead Qualifier")


@app.on_event("startup")
async def startup():
    port = os.environ.get("PORT", "8000")
    logger.info("ISO Lead Qualifier starting on port %s", port)
    logger.info("ANTHROPIC_API_KEY set: %s", bool(os.environ.get("ANTHROPIC_API_KEY")))


@app.get("/test-anthropic")
async def test_anthropic():
    """Diagnostic endpoint — tests whether the Anthropic API is reachable."""
    import anthropic as ac
    import httpx
    from config import settings

    # Step 1: raw TCP/TLS connectivity
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get("https://api.anthropic.com")
        reachable = True
        http_status = r.status_code
    except Exception as exc:
        reachable = False
        http_status = str(exc)

    # Step 2: actual SDK call with a minimal prompt
    claude_ok = False
    claude_error = ""
    if reachable:
        try:
            client = ac.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY, timeout=30)
            resp = await client.messages.create(
                model=settings.CLAUDE_MODEL,
                max_tokens=10,
                messages=[{"role": "user", "content": "Say OK"}],
            )
            claude_ok = True
        except Exception as exc:
            claude_error = f"{type(exc).__name__}: {exc}"

    return {
        "api_reachable": reachable,
        "http_status": http_status,
        "claude_ok": claude_ok,
        "claude_error": claude_error,
        "api_key_prefix": settings.ANTHROPIC_API_KEY[:12] + "..." if settings.ANTHROPIC_API_KEY else "NOT SET",
    }

# Serve static assets (css / js)
_STATIC = Path(__file__).parent / "static"
if _STATIC.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")

_TEMPLATES = Path(__file__).parent / "templates"

# Temp directory that survives the lifetime of the process
# Railway/Render provide a writable /tmp; on Render use a persistent disk
# mounted at /data if you need results to survive a restart.
_UPLOAD_ROOT = Path(tempfile.gettempdir()) / "iso_qualifier_jobs"
_UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index():
    html = (_TEMPLATES / "index.html").read_text()
    return HTMLResponse(content=html)


# ---------------------------------------------------------------------------
# Upload + start job
# ---------------------------------------------------------------------------

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls"}


@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{suffix}'. Upload a .csv or .xlsx file.",
        )

    # Save upload to a job-specific directory
    job_dir = _UPLOAD_ROOT / "pending"
    job_dir.mkdir(parents=True, exist_ok=True)

    input_path = job_dir / f"upload{suffix}"
    with input_path.open("wb") as f:
        shutil.copyfileobj(file.file, f)

    output_dir = _UPLOAD_ROOT / "output"

    job = create_job(
        original_filename=file.filename or "upload",
        input_path=input_path,
        output_dir=output_dir,
    )

    # Move to a job-specific subdirectory now that we have the ID
    job_input_dir = _UPLOAD_ROOT / job.id / "input"
    job_input_dir.mkdir(parents=True, exist_ok=True)
    final_input = job_input_dir / f"upload{suffix}"
    shutil.move(str(input_path), str(final_input))

    job.input_path = final_input
    job.output_dir = _UPLOAD_ROOT / job.id / "output"
    job.output_dir.mkdir(parents=True, exist_ok=True)

    # Kick off pipeline in the background
    asyncio.create_task(_run_job(job))

    return {"job_id": job.id}


async def _run_job(job: Job) -> None:
    try:
        async def _progress(stage: str, processed: int) -> None:
            job.update_progress(stage, processed)

        stats = await runner.run(
            input_path=job.input_path,
            output_dir=job.output_dir,
            on_progress=_progress,
            on_total=job.set_total,
        )
        job.mark_done(stats)
    except Exception as exc:
        job.mark_error(str(exc))


# ---------------------------------------------------------------------------
# SSE progress stream
# ---------------------------------------------------------------------------

@app.get("/jobs/{job_id}/progress")
async def job_progress(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    async def event_stream():
        # Send current snapshot immediately so the client has initial state
        yield _sse(job.to_dict())

        if job.status in (JobStatus.DONE, JobStatus.ERROR):
            yield _sse(job.to_dict(), event="done")
            return

        q = job.subscribe()
        try:
            while True:
                try:
                    event = await asyncio.wait_for(q.get(), timeout=30)
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
                    continue

                if event is None:
                    # Pipeline finished or errored — send final state and close
                    yield _sse(job.to_dict(), event="done")
                    break

                yield _sse(event)
        finally:
            job.unsubscribe(q)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # disable nginx buffering
        },
    )


def _sse(data: dict, event: str = "progress") -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


# ---------------------------------------------------------------------------
# Status (polling fallback)
# ---------------------------------------------------------------------------

@app.get("/jobs/{job_id}/status")
async def job_status(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job.to_dict()


# ---------------------------------------------------------------------------
# File downloads
# ---------------------------------------------------------------------------

@app.get("/jobs/{job_id}/download/csv")
async def download_csv(job_id: str):
    return _download(job_id, "qualified.csv", "text/csv")


@app.get("/jobs/{job_id}/download/xlsx")
async def download_xlsx(job_id: str):
    return _download(
        job_id,
        "qualified.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def _download(job_id: str, filename: str, media_type: str) -> FileResponse:
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.status != JobStatus.DONE:
        raise HTTPException(status_code=409, detail="Job not complete yet")

    path = job.output_dir / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"{filename} not found")

    stem = Path(job.original_filename).stem
    return FileResponse(
        path=str(path),
        media_type=media_type,
        filename=f"{stem}_qualified{Path(filename).suffix}",
    )
