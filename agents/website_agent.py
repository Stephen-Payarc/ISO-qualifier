"""
Stage 1 — Website Agent

Fetches a company website and asks Claude five yes/no ICP qualification
questions derived from reverse-engineering existing clients. Each question
carries a specific point value; the agent also computes the raw stage score.

Scoring (max 10 pts):
  Q1  Actively sells payment processing to other businesses?  4 pts (gate)
  Q2  Independent ISO / MSP / agent (not a major processor)?  2 pts (gate)
  Q5  Has a sales agent / partner / referral program?         2 pts
  Q4  Offers value-added products (POS, funding, ATM, etc.)?  1 pt
  Q3  Offers zero-cost / cash discount / surcharging?         1 pt

If Q1 or Q2 is false the contact is flagged as disqualified by the scorer,
regardless of the numeric score.
"""

import asyncio
import json
import logging
from dataclasses import asdict, dataclass

import anthropic
import httpx

from config import settings
from utils import cache, http_utils

logger = logging.getLogger(__name__)

_client = anthropic.AsyncAnthropic(
    api_key=settings.ANTHROPIC_API_KEY,
    timeout=60.0,
    max_retries=3,
    http_client=httpx.AsyncClient(http2=False),
)

# Point values for each question
Q1_PTS = 4
Q2_PTS = 2
Q3_PTS = 1
Q4_PTS = 1
Q5_PTS = 2

_SYSTEM_PROMPT = """\
You are an expert business development analyst specialising in the independent \
payment processing and merchant services industry. You will be given text scraped \
from a company website. Answer five yes/no qualification questions about the company.

Answer ONLY with a valid JSON object — no markdown, no explanation, no extra text.
"""

_USER_TEMPLATE = """\
Company name: {company}
Website URL: {url}

--- BEGIN WEBSITE TEXT ---
{text}
--- END WEBSITE TEXT ---

Answer ONLY based on the website text above. Return a JSON object with exactly these keys:

{{
  "q1_sells_payments": true or false,
  "q2_is_independent_iso": true or false,
  "q3_zero_cost_pricing": true or false,
  "q4_value_added_products": true or false,
  "q5_has_agent_program": true or false,
  "reasoning": "one sentence (max 200 chars) explaining your answers"
}}

Definitions — answer true only when there is clear evidence:

q1_sells_payments:
  TRUE  if the company actively sells or provides payment processing / merchant
        services TO other businesses (i.e. they are on the selling side).
  FALSE if the company merely accepts payments as a regular merchant, or is
        unrelated to payments.

q2_is_independent_iso:
  TRUE  if the company appears to be an independent ISO, MSP, or sales agent —
        a smaller, independently operated payments business.
  FALSE if they appear to be a major processor (Stripe, Square, Worldpay, Chase,
        First Data / Fiserv, etc.) or a non-payments company.

q3_zero_cost_pricing:
  TRUE  if the site mentions zero-cost processing, cash discounting, surcharging,
        dual pricing, or any program that passes processing fees to the cardholder.
  FALSE otherwise.

q4_value_added_products:
  TRUE  if they offer products BEYOND basic card processing — such as POS systems,
        business funding / merchant cash advance, ATM services, payroll, or loyalty.
  FALSE if they appear to offer card processing only.

q5_has_agent_program:
  TRUE  if the site explicitly mentions a sales agent program, ISO partner program,
        referral program, sub-ISO recruiting, residual income for agents, or
        white-label / reseller opportunities.
  FALSE otherwise.
"""


@dataclass
class WebsiteResult:
    q1_sells_payments: bool = False       # gate — 4 pts
    q2_is_independent_iso: bool = False   # gate — 2 pts
    q3_zero_cost_pricing: bool = False    # 1 pt
    q4_value_added_products: bool = False # 1 pt
    q5_has_agent_program: bool = False    # 2 pts
    score: int = 0
    reasoning: str = ""
    error: str = ""

    def compute_score(self) -> int:
        """Derive the numeric score from the boolean answers."""
        if not self.q1_sells_payments or not self.q2_is_independent_iso:
            return 0
        return (
            Q1_PTS
            + Q2_PTS
            + (Q3_PTS if self.q3_zero_cost_pricing else 0)
            + (Q4_PTS if self.q4_value_added_products else 0)
            + (Q5_PTS if self.q5_has_agent_program else 0)
        )

    def to_stage_dict(self) -> dict:
        """Return dict with s1_ prefixed keys for CSV output."""
        return {
            "s1_sells_payments": self.q1_sells_payments,
            "s1_is_independent_iso": self.q2_is_independent_iso,
            "s1_zero_cost_pricing": self.q3_zero_cost_pricing,
            "s1_value_added_products": self.q4_value_added_products,
            "s1_has_agent_program": self.q5_has_agent_program,
            "s1_score": self.score,
            "s1_error": self.error,
        }


async def qualify(row_idx: int, company: str, url: str) -> dict:
    """
    Main entry point called by the pipeline runner.

    Args:
        row_idx: Index of the contact row (used for merge-back).
        company: Company name.
        url:     Company website URL.

    Returns:
        Dict with '_row_idx' plus all s1_ prefixed result keys.
    """
    cache_key = cache.make_key("s1", url)

    if cache.exists(cache_key):
        logger.debug("Cache hit for website: %s", url)
        result = WebsiteResult(**cache.get(cache_key))
        return {"_row_idx": row_idx, **result.to_stage_dict()}

    result = await _run_agent(company, url)
    cache.set(cache_key, asdict(result))
    return {"_row_idx": row_idx, **result.to_stage_dict()}


async def _run_agent(company: str, url: str) -> WebsiteResult:
    if url and not url.startswith(("http://", "https://")):
        url = "https://" + url

    # --- Fetch page text ---
    try:
        page_text = await http_utils.fetch_text(url)
    except Exception as exc:
        logger.warning("Failed to fetch %s: %s", url, exc)
        return WebsiteResult(error=f"fetch_error: {type(exc).__name__}: {exc}")

    if not page_text.strip():
        return WebsiteResult(error="empty_page")

    # --- Ask Claude ---
    prompt = _USER_TEMPLATE.format(company=company, url=url, text=page_text)

    # Respect Anthropic rate limit — free tier is 5 req/min = 12s between calls.
    # Override with CLAUDE_RATE_LIMIT_DELAY env var once on a paid tier.
    await asyncio.sleep(settings.CLAUDE_RATE_LIMIT_DELAY)

    try:
        response = await _client.messages.create(
            model=settings.CLAUDE_MODEL,
            max_tokens=400,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning("Claude returned invalid JSON for %s: %s", url, exc)
        return WebsiteResult(error=f"json_parse_error: {exc}")
    except Exception as exc:
        logger.warning("Claude API error for %s: %s", url, exc)
        return WebsiteResult(error=f"claude_error: {type(exc).__name__}: {exc}")

    result = WebsiteResult(
        q1_sells_payments=bool(data.get("q1_sells_payments", False)),
        q2_is_independent_iso=bool(data.get("q2_is_independent_iso", False)),
        q3_zero_cost_pricing=bool(data.get("q3_zero_cost_pricing", False)),
        q4_value_added_products=bool(data.get("q4_value_added_products", False)),
        q5_has_agent_program=bool(data.get("q5_has_agent_program", False)),
        reasoning=str(data.get("reasoning", ""))[:300],
    )
    result.score = result.compute_score()
    return result
