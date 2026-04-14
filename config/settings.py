"""
Central configuration. All values can be overridden via environment variables
or a .env file in the project root.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root (two levels up from this file)
_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_ROOT / ".env")


# ---------------------------------------------------------------------------
# API keys
# ---------------------------------------------------------------------------

ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
PDL_API_KEY: str = os.getenv("PDL_API_KEY", "")

# ---------------------------------------------------------------------------
# Claude model
# ---------------------------------------------------------------------------

CLAUDE_MODEL: str = os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6")

# ---------------------------------------------------------------------------
# Concurrency / rate limiting
# ---------------------------------------------------------------------------

# Number of contacts processed in parallel
MAX_CONCURRENCY: int = int(os.getenv("MAX_CONCURRENCY", "10"))

# Seconds to wait between PDL requests (free tier: 100 req/min)
PDL_RATE_LIMIT_DELAY: float = float(os.getenv("PDL_RATE_LIMIT_DELAY", "0.6"))

# Max seconds to wait for a website to load
WEBSITE_TIMEOUT_SECONDS: int = int(os.getenv("WEBSITE_TIMEOUT_SECONDS", "20"))

# Number of retries for failed HTTP requests
HTTP_MAX_RETRIES: int = int(os.getenv("HTTP_MAX_RETRIES", "3"))

# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

# Weights must sum to 1.0
STAGE1_WEIGHT: float = float(os.getenv("STAGE1_WEIGHT", "0.4"))
STAGE2_WEIGHT: float = float(os.getenv("STAGE2_WEIGHT", "0.6"))

# final_score >= this threshold → hot_lead = True
HOT_LEAD_THRESHOLD: float = float(os.getenv("HOT_LEAD_THRESHOLD", "7.0"))

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

CACHE_DIR: Path = _ROOT / os.getenv("CACHE_DIR", ".cache")
LOG_DIR: Path = _ROOT / "logs"
DATA_RAW_DIR: Path = _ROOT / "data" / "raw"
DATA_PROCESSED_DIR: Path = _ROOT / "data" / "processed"
DATA_OUTPUT_DIR: Path = _ROOT / "data" / "output"

# Ensure runtime directories exist
for _d in (CACHE_DIR, LOG_DIR, DATA_PROCESSED_DIR, DATA_OUTPUT_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

# ---------------------------------------------------------------------------
# Website agent
# ---------------------------------------------------------------------------

# Max characters of page text passed to Claude (keeps tokens / cost bounded)
WEBSITE_MAX_CHARS: int = int(os.getenv("WEBSITE_MAX_CHARS", "8000"))

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

if not (0.0 < STAGE1_WEIGHT < 1.0 and 0.0 < STAGE2_WEIGHT < 1.0):
    raise ValueError("STAGE1_WEIGHT and STAGE2_WEIGHT must each be between 0 and 1.")
if abs(STAGE1_WEIGHT + STAGE2_WEIGHT - 1.0) > 1e-6:
    raise ValueError(f"STAGE1_WEIGHT + STAGE2_WEIGHT must equal 1.0, got {STAGE1_WEIGHT + STAGE2_WEIGHT}.")
