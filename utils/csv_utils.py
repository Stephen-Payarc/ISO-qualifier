"""
CSV helpers: loading, validation, batching, and writing results.

Expected input columns (case-insensitive): name, company, website, linkedin_url
All other columns are passed through to the output unchanged.
"""

import csv
import logging
from pathlib import Path
from typing import Iterator

import pandas as pd

from config import settings

logger = logging.getLogger(__name__)

# Canonical column names after normalisation
REQUIRED_COLS = {"name", "company"}
OPTIONAL_COLS = {"website", "linkedin_url"}
ALL_EXPECTED = REQUIRED_COLS | OPTIONAL_COLS

# Output columns appended by the pipeline
OUTPUT_COLS = [
    # Stage 1 — company (website)
    "s1_sells_payments",
    "s1_is_independent_iso",
    "s1_zero_cost_pricing",
    "s1_value_added_products",
    "s1_has_agent_program",
    "s1_score",
    "s1_error",
    # Stage 2 — person (LinkedIn)
    "s2_person_sells_payments",
    "s2_person_at_independent_iso",
    "s2_person_is_decision_maker",
    "s2_person_has_payments_experience",
    "s2_person_manages_team",
    "s2_title",
    "s2_score",
    "s2_error",
    # Final
    "final_score",
    "lead_tier",
    "hot_lead",
    "qualified_at",
]


def load_contacts(path: str | Path) -> pd.DataFrame:
    """
    Load a CSV of contacts, normalise column names, add missing optional
    columns as empty strings, and return a DataFrame.

    Raises ValueError if required columns are missing.
    """
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"Input CSV is missing required columns: {missing}")

    for col in OPTIONAL_COLS:
        if col not in df.columns:
            df[col] = ""

    # Drop contacts that have neither a website nor a LinkedIn URL
    skippable = df["website"].str.strip().eq("") & df["linkedin_url"].str.strip().eq("")
    skipped = df[skippable].copy()
    df = df[~skippable].copy().reset_index(drop=True)

    if len(skipped):
        skip_path = settings.DATA_PROCESSED_DIR / "skipped.csv"
        skipped.to_csv(skip_path, index=False)
        logger.info("Skipped %d contacts with no website or LinkedIn URL → %s", len(skipped), skip_path)

    logger.info("Loaded %d contacts from %s", len(df), path)
    return df


def iter_batches(df: pd.DataFrame, batch_size: int) -> Iterator[pd.DataFrame]:
    """Yield successive slices of a DataFrame."""
    for start in range(0, len(df), batch_size):
        yield df.iloc[start : start + batch_size]


def save_results(df: pd.DataFrame, path: str | Path) -> None:
    """Write the results DataFrame to a CSV, creating parent dirs as needed."""
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False, quoting=csv.QUOTE_NONNUMERIC)
    logger.info("Wrote %d rows → %s", len(df), out)


def merge_stage_results(
    contacts: pd.DataFrame,
    stage_results: list[dict],
) -> pd.DataFrame:
    """
    Merge a list of per-contact result dicts back onto the contacts DataFrame.

    Each dict must contain a '_row_idx' key matching the contacts index.
    """
    results_df = pd.DataFrame(stage_results)
    if results_df.empty:
        return contacts

    results_df = results_df.set_index("_row_idx")
    return contacts.join(results_df, how="left")
