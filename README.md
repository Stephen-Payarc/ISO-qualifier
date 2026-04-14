# ISO Lead Qualification Pipeline

A two-stage AI-powered pipeline for qualifying payment processing / merchant services leads at scale.

## Overview

Given a CSV of ~40,000 contacts (name, company, website, LinkedIn URL), the pipeline runs each contact through two sequential qualification agents and produces a scored output CSV.

```
Stage 1: Website Agent    → visits company website, assesses payments relevance
Stage 2: LinkedIn Agent   → pulls LinkedIn profile via Proxycurl, confirms role fit
```

Each contact receives a qualification score and structured answers to key questions.

---

## Project Structure

```
iso-qualifer/
├── data/
│   ├── raw/          # Drop your input CSV(s) here
│   ├── processed/    # Intermediate per-stage outputs
│   └── output/       # Final scored CSV
├── agents/
│   ├── website_agent.py    # Stage 1: scrapes & evaluates company websites
│   └── linkedin_agent.py   # Stage 2: Proxycurl lookup + role qualification
├── pipeline/
│   ├── runner.py           # Orchestrates the full pipeline
│   └── scorer.py           # Combines stage outputs into final score
├── utils/
│   ├── csv_utils.py        # CSV read/write, batching, dedup
│   ├── http_utils.py       # Rate-limited HTTP client, retry logic
│   └── cache.py            # Disk-based result cache (avoid re-fetching)
├── config/
│   └── settings.py         # API keys, thresholds, concurrency limits
├── logs/                   # Run logs (gitignored)
├── .env.example            # Environment variable template
├── requirements.txt
└── README.md
```

---

## Qualification Logic

### Stage 1 — Website Agent

For each contact's company website the agent answers:

| Question | Type |
|---|---|
| Does the company operate in payment processing or merchant services? | bool |
| What products/services do they sell? | text |
| What verticals or industries do they serve? | text |
| Is there evidence they work with ISOs or payment agents? | bool |
| Stage 1 confidence score | 0–10 |

### Stage 2 — LinkedIn Agent (Proxycurl)

For each contact's LinkedIn profile the agent answers:

| Question | Type |
|---|---|
| Does the person's current role involve payments or merchant services? | bool |
| What is their current title and company? | text |
| How many years of payments-relevant experience? | int |
| Are they likely a decision-maker or influencer? | bool |
| Stage 2 confidence score | 0–10 |

### Final Score

```
final_score = (stage1_score * 0.4) + (stage2_score * 0.6)
```

Contacts with `final_score >= 7` are flagged as **hot leads**.

---

## Setup

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment variables
cp .env.example .env
# Edit .env with your API keys

# 4. Drop your input CSV into data/raw/
# Required columns: name, company, website, linkedin_url

# 5. Run the pipeline
python -m pipeline.runner --input data/raw/contacts.csv --output data/output/qualified.csv
```

---

## Environment Variables

| Variable | Description |
|---|---|
| `ANTHROPIC_API_KEY` | Claude API key (for website + LinkedIn agents) |
| `PROXYCURL_API_KEY` | Proxycurl API key (LinkedIn enrichment) |
| `MAX_CONCURRENCY` | Parallel workers (default: 10) |
| `CACHE_DIR` | Path for disk cache (default: `.cache/`) |
| `LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING` (default: `INFO`) |

---

## Input CSV Format

| Column | Required | Notes |
|---|---|---|
| `name` | Yes | Contact full name |
| `company` | Yes | Company name |
| `website` | No | Company website URL |
| `linkedin_url` | No | Contact LinkedIn profile URL |

Contacts missing both `website` and `linkedin_url` are skipped and written to `data/processed/skipped.csv`.

---

## Output CSV Format

All input columns are preserved. The following columns are appended:

| Column | Description |
|---|---|
| `s1_is_payments` | Stage 1: company in payments? (true/false) |
| `s1_products` | Stage 1: products/services description |
| `s1_verticals` | Stage 1: industries served |
| `s1_iso_friendly` | Stage 1: ISO/agent channel present? |
| `s1_score` | Stage 1 confidence score (0–10) |
| `s2_role_in_payments` | Stage 2: person's role in payments? (true/false) |
| `s2_title` | Stage 2: current title |
| `s2_payments_experience_years` | Stage 2: years of relevant experience |
| `s2_is_decision_maker` | Stage 2: decision maker / influencer? |
| `s2_score` | Stage 2 confidence score (0–10) |
| `final_score` | Weighted final score (0–10) |
| `hot_lead` | true if final_score >= 7 |
| `qualified_at` | ISO 8601 timestamp of qualification run |

---

## Cost Estimates (40k contacts)

| Resource | Estimate |
|---|---|
| Proxycurl lookups | ~$40 (at $0.001/lookup) |
| Claude API (website agent) | ~$8–15 depending on page size |
| Claude API (LinkedIn agent) | ~$4–8 |
| **Total** | **~$52–63** |

Cache all results — re-runs against already-processed contacts are free.

---

## Notes

- The pipeline is resumable: already-processed contacts are skipped via the disk cache.
- Websites that time out or return errors are marked `s1_error` and scored 0.
- LinkedIn profiles that are not found via Proxycurl are marked `s2_not_found` and scored 0.
- Run logs are written to `logs/` and rotated daily.
