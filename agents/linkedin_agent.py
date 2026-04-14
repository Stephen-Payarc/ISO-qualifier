"""
Stage 2 — LinkedIn Agent

Fetches a LinkedIn profile via the Proxycurl API and asks Claude five yes/no
questions that mirror the website agent's ICP scoring — but applied to the
*person* rather than the company.

Scoring (max 10 pts):
  Q1  Person actively sells payment processing / merchant services?  4 pts (gate)
  Q2  Person works at an independent ISO / MSP (not a major processor)? 2 pts (gate)
  Q3  Person is a decision-maker / owner / founder?                  2 pts
  Q4  Person has 2+ years of direct payments sales experience?       1 pt
  Q5  Person mentions managing or building a sales / agent team?     1 pt

If Q1 or Q2 is false the contact is flagged as disqualified by the scorer,
regardless of the numeric score.
"""

import json
import logging
from dataclasses import asdict, dataclass

import anthropic

from config import settings
from utils import cache, http_utils

logger = logging.getLogger(__name__)

_client = anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY)

# Point values for each question
Q1_PTS = 4
Q2_PTS = 2
Q3_PTS = 2
Q4_PTS = 1
Q5_PTS = 1

_SYSTEM_PROMPT = """\
You are an expert business development analyst specialising in the independent \
payment processing and merchant services industry. You will be given a LinkedIn \
profile in JSON format. Answer five yes/no qualification questions about whether \
this person is a high-value ISO / merchant services contact.

Answer ONLY with a valid JSON object — no markdown, no explanation, no extra text.
"""

_USER_TEMPLATE = """\
Contact name: {name}

--- BEGIN LINKEDIN PROFILE (JSON) ---
{profile_json}
--- END LINKEDIN PROFILE ---

Answer ONLY based on the LinkedIn data above. Return a JSON object with exactly these keys:

{{
  "q1_person_sells_payments": true or false,
  "q2_person_at_independent_iso": true or false,
  "q3_person_is_decision_maker": true or false,
  "q4_person_has_payments_experience": true or false,
  "q5_person_manages_team": true or false,
  "title": "current job title (max 80 chars, empty string if not found)",
  "reasoning": "one sentence (max 200 chars) explaining your answers"
}}

Definitions — answer true only when there is clear evidence:

q1_person_sells_payments:
  TRUE  if the person's current role involves actively selling or providing
        payment processing or merchant services to other businesses — e.g. they
        are an ISO agent, merchant services rep, payments sales executive, or
        owner of a merchant services company.
  FALSE if they are a regular business owner who only accepts payments, a
        software engineer, marketer, or otherwise not in payments sales.

q2_person_at_independent_iso:
  TRUE  if they appear to work at or own an independent ISO, MSP, or small
        payments agency — not at a major processor (Stripe, Square, Worldpay,
        Chase, Fiserv, First Data, PayPal, Adyen, etc.).
  FALSE otherwise.

q3_person_is_decision_maker:
  TRUE  if their current title includes Owner, Founder, President, CEO, COO,
        VP, Director, Partner, or Head of — at a payments-related company.
  FALSE otherwise.

q4_person_has_payments_experience:
  TRUE  if their work history shows 2 or more years in roles where payment
        processing or merchant services was a primary responsibility.
  FALSE otherwise.

q5_person_manages_team:
  TRUE  if their profile mentions managing, building, or leading a sales team,
        agent network, or ISO channel (e.g. "manage a team of agents",
        "recruited 20+ agents", "built ISO channel").
  FALSE otherwise.
"""

# PDL field names (slightly different from the old Proxycurl schema)
_PROFILE_FIELDS = [
    "full_name",
    "headline",
    "summary",
    "job_title",           # current title
    "job_company_name",    # current employer
    "experience",          # PDL uses "experience" (not "experiences")
    "skills",
    "industry",
    "location_country",
]


@dataclass
class LinkedInResult:
    q1_person_sells_payments: bool = False       # gate — 4 pts
    q2_person_at_independent_iso: bool = False   # gate — 2 pts
    q3_person_is_decision_maker: bool = False    # 2 pts
    q4_person_has_payments_experience: bool = False  # 1 pt
    q5_person_manages_team: bool = False         # 1 pt
    score: int = 0
    title: str = ""
    reasoning: str = ""
    error: str = ""

    def compute_score(self) -> int:
        """Derive the numeric score from the boolean answers."""
        if not self.q1_person_sells_payments or not self.q2_person_at_independent_iso:
            return 0
        return (
            Q1_PTS
            + Q2_PTS
            + (Q3_PTS if self.q3_person_is_decision_maker else 0)
            + (Q4_PTS if self.q4_person_has_payments_experience else 0)
            + (Q5_PTS if self.q5_person_manages_team else 0)
        )

    def to_stage_dict(self) -> dict:
        """Return dict with s2_ prefixed keys for CSV output."""
        return {
            "s2_person_sells_payments": self.q1_person_sells_payments,
            "s2_person_at_independent_iso": self.q2_person_at_independent_iso,
            "s2_person_is_decision_maker": self.q3_person_is_decision_maker,
            "s2_person_has_payments_experience": self.q4_person_has_payments_experience,
            "s2_person_manages_team": self.q5_person_manages_team,
            "s2_title": self.title,
            "s2_score": self.score,
            "s2_error": self.error,
        }


async def qualify(row_idx: int, name: str, linkedin_url: str) -> dict:
    """
    Main entry point called by the pipeline runner.

    Args:
        row_idx:      Index of the contact row (used for merge-back).
        name:         Contact full name.
        linkedin_url: LinkedIn profile URL.

    Returns:
        Dict with '_row_idx' plus all s2_ prefixed result keys.
    """
    if not linkedin_url or not linkedin_url.strip():
        result = LinkedInResult(error="no_linkedin_url")
        return {"_row_idx": row_idx, **result.to_stage_dict()}

    cache_key = cache.make_key("s2", linkedin_url)

    if cache.exists(cache_key):
        logger.debug("Cache hit for LinkedIn: %s", linkedin_url)
        result = LinkedInResult(**cache.get(cache_key))
        return {"_row_idx": row_idx, **result.to_stage_dict()}

    result = await _run_agent(name, linkedin_url)
    cache.set(cache_key, asdict(result))
    return {"_row_idx": row_idx, **result.to_stage_dict()}


async def _run_agent(name: str, linkedin_url: str) -> LinkedInResult:
    try:
        profile = await http_utils.pdl_get(linkedin_url)
    except Exception as exc:
        logger.warning("PDL error for %s: %s", linkedin_url, exc)
        return LinkedInResult(error=f"pdl_error: {type(exc).__name__}: {exc}")

    if not profile:
        return LinkedInResult(error="profile_not_found")

    trimmed = {k: profile[k] for k in _PROFILE_FIELDS if k in profile}
    if "experience" in trimmed and isinstance(trimmed["experience"], list):
        trimmed["experience"] = trimmed["experience"][:10]

    profile_json = json.dumps(trimmed, indent=2)
    prompt = _USER_TEMPLATE.format(name=name, profile_json=profile_json)

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
        logger.warning("Claude returned invalid JSON for %s: %s", linkedin_url, exc)
        return LinkedInResult(error=f"json_parse_error: {exc}")
    except Exception as exc:
        logger.warning("Claude API error for %s: %s", linkedin_url, exc)
        return LinkedInResult(error=f"claude_error: {type(exc).__name__}: {exc}")

    result = LinkedInResult(
        q1_person_sells_payments=bool(data.get("q1_person_sells_payments", False)),
        q2_person_at_independent_iso=bool(data.get("q2_person_at_independent_iso", False)),
        q3_person_is_decision_maker=bool(data.get("q3_person_is_decision_maker", False)),
        q4_person_has_payments_experience=bool(data.get("q4_person_has_payments_experience", False)),
        q5_person_manages_team=bool(data.get("q5_person_manages_team", False)),
        title=str(data.get("title", ""))[:200],
        reasoning=str(data.get("reasoning", ""))[:300],
    )
    result.score = result.compute_score()
    return result
