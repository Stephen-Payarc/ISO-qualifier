"""
Rate-limited, retry-aware HTTP client utilities.

- fetch_text() — scrape a URL and return cleaned page text (uses Playwright
                 for JS-rendered sites, falls back to httpx for plain HTML).
- pdl_get()    — thin wrapper around the People Data Labs person enrichment API.
"""

import asyncio
import logging
import re
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Playwright-based website fetcher
# ---------------------------------------------------------------------------

async def fetch_text(url: str) -> str:
    """
    Load a URL with Playwright (headless Chromium), extract visible text,
    and return up to WEBSITE_MAX_CHARS characters.

    Falls back to a plain httpx GET if Playwright fails.
    """
    try:
        from playwright.async_api import async_playwright, TimeoutError as PWTimeout

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            try:
                page = await browser.new_page()
                await page.goto(
                    url,
                    timeout=settings.WEBSITE_TIMEOUT_SECONDS * 1000,
                    wait_until="domcontentloaded",
                )
                html = await page.content()
            except PWTimeout:
                logger.warning("Playwright timeout for %s", url)
                raise
            finally:
                await browser.close()

        return _extract_text(html)

    except Exception as exc:
        logger.warning("Playwright failed for %s (%s), falling back to httpx", url, exc)
        return await _fetch_text_httpx(url)


async def _fetch_text_httpx(url: str) -> str:
    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=settings.WEBSITE_TIMEOUT_SECONDS,
        headers={"User-Agent": "Mozilla/5.0 (compatible; LeadQualifier/1.0)"},
    ) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return _extract_text(resp.text)


def _extract_text(html: str) -> str:
    """Strip HTML tags, collapse whitespace, and cap length."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")

    # Remove non-content elements
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav"]):
        tag.decompose()

    text = soup.get_text(separator=" ")
    # Collapse runs of whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text[: settings.WEBSITE_MAX_CHARS]


# ---------------------------------------------------------------------------
# People Data Labs (PDL) client
# ---------------------------------------------------------------------------

_PDL_BASE = "https://api.peopledatalabs.com/v5/person/enrich"


@retry(
    retry=retry_if_exception_type(httpx.HTTPStatusError),
    stop=stop_after_attempt(settings.HTTP_MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
async def pdl_get(linkedin_url: str) -> dict[str, Any]:
    """
    Fetch a person profile via the People Data Labs enrichment API.

    Looks up by LinkedIn profile URL. Returns the PDL person dict,
    or an empty dict if the profile is not found.
    Raises httpx.HTTPStatusError on non-2xx responses after retries.
    """
    await asyncio.sleep(settings.PDL_RATE_LIMIT_DELAY)

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            _PDL_BASE,
            params={"profile": linkedin_url, "pretty": "false"},
            headers={"X-Api-Key": settings.PDL_API_KEY},
        )

        if resp.status_code == 404:
            # Profile not in PDL database — not a transient error, don't retry
            return {}

        resp.raise_for_status()
        data = resp.json()
        # PDL wraps the person record under a "data" key
        return data.get("data") or {}
