"""
Rate-limited, retry-aware HTTP client utilities.

- fetch_text()   — scrape a URL and return cleaned page text (uses Playwright
                   for JS-rendered sites, falls back to httpx for plain HTML).
- proxycurl_get() — thin wrapper around the Proxycurl REST API with retry logic.
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
# Proxycurl client
# ---------------------------------------------------------------------------

_PROXYCURL_BASE = "https://nubela.co/proxycurl/api/v2/linkedin"


@retry(
    retry=retry_if_exception_type(httpx.HTTPStatusError),
    stop=stop_after_attempt(settings.HTTP_MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
async def proxycurl_get(linkedin_url: str) -> dict[str, Any]:
    """
    Fetch a LinkedIn profile via the Proxycurl API.

    Returns the raw Proxycurl profile dict.
    Raises httpx.HTTPStatusError on non-2xx responses after retries.
    """
    await asyncio.sleep(settings.PROXYCURL_RATE_LIMIT_DELAY)

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            _PROXYCURL_BASE,
            params={"url": linkedin_url, "use_cache": "if-present"},
            headers={"Authorization": f"Bearer {settings.PROXYCURL_API_KEY}"},
        )

        if resp.status_code == 404:
            # Profile not found — not a transient error, don't retry
            return {}

        resp.raise_for_status()
        return resp.json()
