"""
Pipeline runner — orchestrates Stage 1 (website) and Stage 2 (LinkedIn)
across all contacts, with concurrency control, progress tracking, and
checkpoint saves every N rows.

CLI usage:
    python -m pipeline.runner --input data/raw/contacts.csv \\
                               --output data/output/qualified.csv

Programmatic usage (from web app):
    await run(input_path, output_dir, on_progress=job.update_progress)
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Awaitable

import pandas as pd
from tqdm.asyncio import tqdm

from agents import linkedin_agent, website_agent
from config import settings
from pipeline import scorer
from utils import cache, csv_utils

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            settings.LOG_DIR / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        ),
    ],
)
logger = logging.getLogger(__name__)

CHECKPOINT_EVERY = 500

# Type alias for the optional progress callback
ProgressCallback = Callable[[str, int], Awaitable[None]] | None


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------

async def run(
    input_path: str | Path,
    output_dir: str | Path,
    on_progress: ProgressCallback = None,
    on_total: Callable[[int], None] | None = None,
) -> dict:
    """
    Run the full qualification pipeline.

    Args:
        input_path:  Path to the input CSV or xlsx file.
        output_dir:  Directory where output files are written.
                     Produces qualified.csv and qualified.xlsx.
        on_progress: Optional async callback(stage_name, n_processed).
                     Called after each completed contact.
        on_total:    Optional sync callback(total_contacts).
                     Called once after the input file is loaded.

    Returns:
        Summary stats dict from scorer.summary().
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=== ISO Lead Qualification Pipeline ===")
    logger.info("Input:  %s", input_path)
    logger.info("Output: %s", output_dir)
    logger.info("Model:  %s", settings.CLAUDE_MODEL)

    contacts = csv_utils.load_contacts(input_path)
    total = len(contacts)
    logger.info("Processing %d contacts", total)

    if on_total:
        on_total(total)

    semaphore = asyncio.Semaphore(settings.MAX_CONCURRENCY)
    timestamp = datetime.now(timezone.utc).isoformat()

    s1_results: list[dict] = []
    s2_results: list[dict] = []

    # -----------------------------------------------------------------------
    # Stage 1 — Website Agent
    # -----------------------------------------------------------------------
    logger.info("--- Stage 1: Website Agent ---")

    async def run_s1(idx: int, row: dict) -> dict:
        async with semaphore:
            return await website_agent.qualify(
                row_idx=idx,
                company=row.get("company", ""),
                url=row.get("website", ""),
            )

    tasks_s1 = [
        run_s1(idx, row)
        for idx, row in contacts.iterrows()
        if row.get("website", "").strip()
    ]

    async for result in tqdm(
        asyncio.as_completed(tasks_s1),
        total=len(tasks_s1),
        desc="Stage 1",
        unit="contact",
    ):
        s1_results.append(await result)

        if on_progress:
            await on_progress("Stage 1 — Website", len(s1_results))

        if len(s1_results) % CHECKPOINT_EVERY == 0:
            _checkpoint(contacts, s1_results, [], timestamp, output_dir)

    website_idxs = {r["_row_idx"] for r in s1_results}
    for idx, row in contacts.iterrows():
        if idx not in website_idxs:
            empty = website_agent.WebsiteResult(error="no_website")
            s1_results.append({"_row_idx": idx, **empty.to_stage_dict()})

    logger.info("Stage 1 complete: %d results", len(s1_results))

    # -----------------------------------------------------------------------
    # Stage 2 — LinkedIn Agent
    # -----------------------------------------------------------------------
    logger.info("--- Stage 2: LinkedIn Agent ---")

    async def run_s2(idx: int, row: dict) -> dict:
        async with semaphore:
            return await linkedin_agent.qualify(
                row_idx=idx,
                name=row.get("name", ""),
                linkedin_url=row.get("linkedin_url", ""),
            )

    tasks_s2 = [
        run_s2(idx, row)
        for idx, row in contacts.iterrows()
        if row.get("linkedin_url", "").strip()
    ]

    async for result in tqdm(
        asyncio.as_completed(tasks_s2),
        total=len(tasks_s2),
        desc="Stage 2",
        unit="contact",
    ):
        s2_results.append(await result)

        if on_progress:
            await on_progress("Stage 2 — LinkedIn", len(s1_results) + len(s2_results))

        if len(s2_results) % CHECKPOINT_EVERY == 0:
            _checkpoint(contacts, s1_results, s2_results, timestamp, output_dir)

    linkedin_idxs = {r["_row_idx"] for r in s2_results}
    for idx, row in contacts.iterrows():
        if idx not in linkedin_idxs:
            empty = linkedin_agent.LinkedInResult(error="no_linkedin_url")
            s2_results.append({"_row_idx": idx, **empty.to_stage_dict()})

    logger.info("Stage 2 complete: %d results", len(s2_results))

    # -----------------------------------------------------------------------
    # Merge, score, and save both formats
    # -----------------------------------------------------------------------
    df = csv_utils.merge_stage_results(contacts, s1_results)
    df = csv_utils.merge_stage_results(df, s2_results)
    df = scorer.apply_scores(df)
    df["qualified_at"] = timestamp

    csv_path  = output_dir / "qualified.csv"
    xlsx_path = output_dir / "qualified.xlsx"

    csv_utils.save_results(df, csv_path)
    _save_xlsx(df, xlsx_path)

    stats = scorer.summary(df)
    logger.info(
        "Done. %d contacts | %d hot leads (%.1f%%) | avg score %.2f | "
        "s1 errors: %d | s2 errors: %d",
        stats["total"],
        stats["hot_leads"],
        stats["hot_lead_pct"],
        stats["avg_final_score"],
        stats["s1_errors"],
        stats["s2_errors"],
    )
    logger.info("Cache stats: %s", cache.stats())
    return stats


def _save_xlsx(df: pd.DataFrame, path: Path) -> None:
    """Write results to Excel with basic formatting."""
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Qualified Leads")
        ws = writer.sheets["Qualified Leads"]

        # Freeze header row
        ws.freeze_panes = "A2"

        # Auto-width columns (capped at 50)
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 50)

    logger.info("Wrote xlsx → %s", path)


def _checkpoint(
    contacts: pd.DataFrame,
    s1_results: list[dict],
    s2_results: list[dict],
    timestamp: str,
    output_dir: Path,
) -> None:
    df = csv_utils.merge_stage_results(contacts, s1_results)
    if s2_results:
        df = csv_utils.merge_stage_results(df, s2_results)
    df = scorer.apply_scores(df)
    df["qualified_at"] = timestamp
    n = len(s1_results) + len(s2_results)
    chk_path = settings.DATA_PROCESSED_DIR / f"checkpoint_{n}.csv"
    csv_utils.save_results(df, chk_path)
    logger.info("Checkpoint saved → %s", chk_path)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="ISO Lead Qualification Pipeline")
    parser.add_argument("--input", required=True)
    parser.add_argument(
        "--output-dir",
        default=str(settings.DATA_OUTPUT_DIR),
        help="Directory for output files (default: data/output/)",
    )
    parser.add_argument("--clear-cache", action="store_true")
    args = parser.parse_args()

    if args.clear_cache:
        cache.clear()
        logger.info("Cache cleared.")

    asyncio.run(run(args.input, args.output_dir))


if __name__ == "__main__":
    main()
