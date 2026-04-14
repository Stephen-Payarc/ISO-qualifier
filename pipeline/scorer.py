"""
Scorer — applies the point-based gating model to produce a final score.

Gate logic (disqualifies the contact entirely if either gate fails):
  s1_sells_payments = false        → disqualified (company not in payments)
  s1_is_independent_iso = false    → disqualified (not an independent ISO)
  s2_person_sells_payments = false → disqualified (person not in payments sales)
  s2_person_at_independent_iso = false → disqualified (not at independent ISO)

  Gates only apply when that stage has data. A missing stage (no website or no
  LinkedIn URL) does not disqualify — it just limits the maximum achievable score.

Scoring (each stage max 10 pts):
  Stage 1 — Company (website):
    Q1 sells_payments         4 pts  (gate)
    Q2 is_independent_iso     2 pts  (gate)
    Q5 has_agent_program      2 pts
    Q4 value_added_products   1 pt
    Q3 zero_cost_pricing      1 pt

  Stage 2 — Person (LinkedIn):
    Q1 person_sells_payments            4 pts  (gate)
    Q2 person_at_independent_iso        2 pts  (gate)
    Q3 person_is_decision_maker         2 pts
    Q4 person_has_payments_experience   1 pt
    Q5 person_manages_team              1 pt

Final score and lead tiers:
  0          Disqualified (failed a gate)
  1–4        Cold
  5–6        Warm
  7–8        Hot
  9–10       Ideal customer profile
"""

import pandas as pd

from config import settings


# ---------------------------------------------------------------------------
# Lead tier labels (stored in output CSV alongside numeric score)
# ---------------------------------------------------------------------------
def _tier(score: float, disqualified: bool) -> str:
    if disqualified:
        return "disqualified"
    if score <= 4:
        return "cold"
    if score <= 6:
        return "warm"
    if score <= 8:
        return "hot"
    return "ideal"


def apply_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'final_score', 'lead_tier', and 'hot_lead' columns to the DataFrame.
    """
    df = df.copy()

    # Helpers — boolean columns default to False when missing
    def _bool_col(name: str) -> pd.Series:
        col = df.get(name, pd.Series(False, index=df.index))
        return col.astype(str).str.lower().eq("true")

    def _has_stage(error_col: str) -> pd.Series:
        """True when the stage ran and did NOT return an error."""
        return df.get(error_col, pd.Series("", index=df.index)).astype(str).str.strip().eq("")

    has_s1 = _has_stage("s1_error")

    # --- Gate check (Stage 1 only) ---
    s1_gate_fail = has_s1 & (
        ~_bool_col("s1_sells_payments") | ~_bool_col("s1_is_independent_iso")
    )
    disqualified = s1_gate_fail

    # --- Score from Stage 1 ---
    s1 = pd.to_numeric(df.get("s1_score", 0), errors="coerce").fillna(0)

    final = pd.Series(0.0, index=df.index)
    final[has_s1 & ~disqualified] = s1[has_s1 & ~disqualified]
    # disqualified rows stay 0

    df["final_score"] = final.round(2)
    df["lead_tier"]   = [
        _tier(score, dis)
        for score, dis in zip(df["final_score"], disqualified)
    ]
    df["hot_lead"] = df["lead_tier"].isin(["hot", "ideal"])

    return df


def summary(df: pd.DataFrame) -> dict:
    """Return a stats dict for logging at the end of a run."""
    total        = len(df)
    hot          = int(df["hot_lead"].sum()) if "hot_lead" in df.columns else 0
    avg          = float(df["final_score"].mean()) if "final_score" in df.columns else 0.0
    s1_errors    = int(df["s1_error"].astype(str).str.strip().ne("").sum()) if "s1_error" in df.columns else 0
    disqualified = int((df.get("lead_tier", "") == "disqualified").sum())

    tier_counts = (
        df["lead_tier"].value_counts().to_dict()
        if "lead_tier" in df.columns
        else {}
    )

    return {
        "total": total,
        "hot_leads": hot,
        "hot_lead_pct": round(hot / total * 100, 1) if total else 0,
        "avg_final_score": round(avg, 2),
        "disqualified": disqualified,
        "tiers": tier_counts,
        "s1_errors": s1_errors,
    }
