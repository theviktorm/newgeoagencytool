"""
Momentus AI — Web Scraper & Content Extractor
Real fetching with error handling, content cleaning,
and structured extraction of title/headings/body/lists/FAQs.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup, Comment, NavigableString, Tag
from tenacity import (
    retry, stop_after_attempt, wait_exponential,
    retry_if_exception_type, before_sleep_log,
)

from .config import settings
from .database import execute, fetch_all, fetch_one, gen_id, to_json

logger = logging.getLogger("geo.scraper")

# Elements that are never content
NOISE_TAGS = {
    "nav", "header", "footer", "aside", "script", "style", "noscript",
    "iframe", "object", "embed", "form", "button", "input", "select",
    "textarea", "svg", "canvas", "video", "audio", "map", "figure",
}

NOISE_CLASSES = {
    "nav", "navbar", "navigation", "sidebar", "footer", "header",
    "menu", "breadcrumb", "pagination", "ad", "advertisement",
    "social", "share", "comment", "comments", "cookie", "popup",
    "modal", "overlay", "banner", "promo", "related", "recommended",
    "widget", "toolbar", "search", "newsletter", "subscribe",
}

NOISE_IDS = {
    "nav", "navbar", "navigation", "sidebar", "footer", "header",
    "menu", "breadcrumbs", "comments", "cookie-banner", "popup",
    "advertisement", "ad-container", "newsletter",
}


# ═══════════════════════════════════════════════════════════════
# FETCHER — Downloads pages with proper error handling
# ═══════════════════════════════════════════════════════════════

class PageFetcher:
    """Async HTTP fetcher with retry logic, rate limiting, and error handling."""

    def __init__(self):
        self.semaphore = asyncio.Semaphore(settings.scraper_max_concurrent)
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=settings.scraper_timeout,
                follow_redirects=True,
                max_redirects=5,
                headers={
                    "User-Agent": settings.scraper_user_agent,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive",
                    "Cache-Control": "no-cache",
                },
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    @retry(
        stop=stop_after_attempt(settings.scraper_retry_attempts),
        wait=wait_exponential(multiplier=1, min=2, max=15),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    async def fetch(self, url: str) -> Dict[str, Any]:
        """
        Fetch a URL and return {html, status_code, url, error}.
        Handles 403s, rate limits, timeouts, and redirects.
        """
        async with self.semaphore:
            try:
                client = await self._get_client()
                response = await client.get(url)

                if response.status_code == 200:
                    content_type = response.headers.get("content-type", "")
                    if "text/html" not in content_type and "application/xhtml" not in content_type:
                        return {
                            "html": None,
                            "status_code": response.status_code,
                            "url": str(response.url),
                            "error": f"Non-HTML content type: {content_type}",
                        }

                    return {
                        "html": response.text,
                        "status_code": 200,
                        "url": str(response.url),  # final URL after redirects
                        "error": None,
                    }

                elif response.status_code == 403:
                    return {
                        "html": None,
                        "status_code": 403,
                        "url": url,
                        "error": "Access forbidden (403). Site may block scraping.",
                    }

                elif response.status_code == 429:
                    retry_after = response.headers.get("Retry-After", "60")
                    return {
                        "html": None,
                        "status_code": 429,
                        "url": url,
                        "error": f"Rate limited (429). Retry after {retry_after}s.",
                    }

                elif response.status_code == 404:
                    return {
                        "html": None,
                        "status_code": 404,
                        "url": url,
                        "error": "Page not found (404).",
                    }

                else:
                    return {
                        "html": None,
                        "status_code": response.status_code,
                        "url": url,
                        "error": f"HTTP {response.status_code}",
                    }

            except httpx.TimeoutException:
                return {
                    "html": None,
                    "status_code": 0,
                    "url": url,
                    "error": f"Timeout after {settings.scraper_timeout}s",
                }
            except httpx.ConnectError as e:
                return {
                    "html": None,
                    "status_code": 0,
                    "url": url,
                    "error": f"Connection error: {str(e)}",
                }
            except Exception as e:
                return {
                    "html": None,
                    "status_code": 0,
                    "url": url,
                    "error": f"Unexpected error: {str(e)}",
                }


# ═══════════════════════════════════════════════════════════════
# CLEANER — Strips noise from HTML
# ═══════════════════════════════════════════════════════════════

class ContentCleaner:
    """Removes navigation, footer, sidebar, ads, and other noise from HTML."""

    def clean(self, html: str) -> BeautifulSoup:
        """Parse HTML and strip noise elements. Returns cleaned BeautifulSoup."""
        soup = BeautifulSoup(html, "lxml")

        # Remove comments
        for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
            comment.extract()

        # Remove noise tags entirely
        for tag_name in NOISE_TAGS:
            for el in soup.find_all(tag_name):
                el.decompose()

        # Remove elements with noise class names
        for el in soup.find_all(True):
            classes = el.get("class", [])
            if isinstance(classes, list):
                class_str = " ".join(classes).lower()
            else:
                class_str = str(classes).lower()

            el_id = (el.get("id") or "").lower()

            # Check class matches
            if any(noise in class_str for noise in NOISE_CLASSES):
                el.decompose()
                continue

            # Check id matches
            if any(noise in el_id for noise in NOISE_IDS):
                el.decompose()
                continue

            # Remove hidden elements
            style = el.get("style", "").lower()
            if "display:none" in style.replace(" ", "") or "visibility:hidden" in style.replace(" ", ""):
                el.decompose()
                continue

            # Remove aria-hidden elements
            if el.get("aria-hidden") == "true":
                el.decompose()
                continue

        return soup

    def find_main_content(self, soup: BeautifulSoup) -> Tag:
        """
        Find the main content container.
        Tries <main>, <article>, role="main", then falls back to <body>.
        """
        # Priority order
        for selector in [
            soup.find("main"),
            soup.find("article"),
            soup.find(attrs={"role": "main"}),
            soup.find(attrs={"id": re.compile(r"content|article|post|entry", re.I)}),
            soup.find(attrs={"class": re.compile(r"content|article|post|entry", re.I)}),
        ]:
            if selector:
                return selector

        return soup.find("body") or soup


# ═══════════════════════════════════════════════════════════════
# EXTRACTOR — Structured content extraction
# ═══════════════════════════════════════════════════════════════

class ContentExtractor:
    """
    Extracts structured content from cleaned HTML:
    title, headings hierarchy, body text, lists, FAQs, meta, schema markup.
    """

    def extract(self, html: str, url: str = "") -> Dict[str, Any]:
        """Full extraction pipeline. Returns structured content dict."""
        cleaner = ContentCleaner()
        soup = cleaner.clean(html)
        main = cleaner.find_main_content(soup)

        result = {
            "title": self._extract_title(soup),
            "headings": self._extract_headings(main),
            "body_text": self._extract_body_text(main),
            "lists": self._extract_lists(main),
            "faqs": self._extract_faqs(soup, main),
            "meta_description": self._extract_meta(soup),
            "schema_markup": self._extract_schema(soup),
            "word_count": 0,
            "raw_html_hash": hashlib.md5(html.encode()).hexdigest(),
        }

        result["word_count"] = len(result["body_text"].split())

        return result

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract page title from <title>, <h1>, or og:title."""
        # Try og:title first (usually cleaner)
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            return og["content"].strip()

        # Try <h1>
        h1 = soup.find("h1")
        if h1:
            return h1.get_text(strip=True)

        # Fall back to <title>
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)
            # Remove common suffixes like " | Site Name" or " - Brand"
            for sep in [" | ", " - ", " — ", " – ", " :: "]:
                if sep in title:
                    title = title.split(sep)[0].strip()
            return title

        return ""

    def _extract_headings(self, container: Tag) -> List[Dict[str, Any]]:
        """Extract heading hierarchy with level and text."""
        headings = []
        for level in range(1, 7):
            for h in container.find_all(f"h{level}"):
                text = h.get_text(strip=True)
                if text and len(text) > 1:
                    headings.append({"level": level, "text": text})

        # Sort by document order (approximate via position in source)
        # BeautifulSoup preserves document order within find_all per tag,
        # but we need cross-tag ordering. Use source position.
        all_headings = container.find_all(re.compile(r"^h[1-6]$"))
        ordered = []
        for h in all_headings:
            text = h.get_text(strip=True)
            level = int(h.name[1])
            if text and len(text) > 1:
                ordered.append({"level": level, "text": text})

        return ordered

    def _extract_body_text(self, container: Tag) -> str:
        """
        Extract clean body text from paragraphs.
        Preserves structure but strips HTML.
        """
        paragraphs = []

        for el in container.find_all(["p", "div", "section"]):
            # Skip if element contains block-level children (avoid duplication)
            if el.name in ("div", "section"):
                has_block = el.find(["p", "div", "section", "article", "h1", "h2", "h3", "h4", "h5", "h6"])
                if has_block:
                    continue

            text = el.get_text(separator=" ", strip=True)
            # Filter out noise
            if (
                text
                and len(text) > 20  # skip very short fragments
                and not self._is_boilerplate(text)
            ):
                paragraphs.append(text)

        return "\n\n".join(paragraphs)

    def _extract_lists(self, container: Tag) -> List[Dict[str, Any]]:
        """Extract ordered and unordered lists."""
        lists = []

        for list_el in container.find_all(["ul", "ol"]):
            # Skip navigation lists
            parent = list_el.parent
            if parent and parent.name in ("nav", "header", "footer"):
                continue
            parent_class = " ".join(parent.get("class", [])).lower() if parent else ""
            if any(n in parent_class for n in NOISE_CLASSES):
                continue

            items = []
            for li in list_el.find_all("li", recursive=False):
                text = li.get_text(separator=" ", strip=True)
                if text and len(text) > 3:
                    items.append(text)

            if items and len(items) >= 2:
                lists.append({
                    "type": "ol" if list_el.name == "ol" else "ul",
                    "items": items,
                })

        return lists

    def _extract_faqs(self, full_soup: BeautifulSoup, container: Tag) -> List[Dict[str, Any]]:
        """
        Extract FAQ pairs using multiple strategies:
        1. FAQ schema markup (most reliable)
        2. <details>/<summary> elements
        3. FAQ-like heading patterns (Q: / A: or bold question + paragraph answer)
        4. Sections with "FAQ" or "Questions" in heading
        """
        faqs = []

        # Strategy 1: JSON-LD FAQ schema
        for script in full_soup.find_all("script", type="application/ld+json"):
            try:
                import json
                data = json.loads(script.string or "")
                if isinstance(data, dict):
                    items = data if data.get("@type") == "FAQPage" else {}
                    main_entity = items.get("mainEntity", data.get("mainEntity", []))
                    if isinstance(main_entity, list):
                        for item in main_entity:
                            q = item.get("name", "")
                            a_obj = item.get("acceptedAnswer", {})
                            a = a_obj.get("text", "") if isinstance(a_obj, dict) else ""
                            if q and a:
                                faqs.append({"question": q, "answer": a})
            except (json.JSONDecodeError, Exception):
                pass

        if faqs:
            return faqs

        # Strategy 2: <details>/<summary> (accordion FAQs)
        for details in container.find_all("details"):
            summary = details.find("summary")
            if summary:
                question = summary.get_text(strip=True)
                # Answer is everything after summary
                answer_parts = []
                for sibling in summary.next_siblings:
                    if isinstance(sibling, Tag):
                        answer_parts.append(sibling.get_text(separator=" ", strip=True))
                    elif isinstance(sibling, NavigableString):
                        text = sibling.strip()
                        if text:
                            answer_parts.append(text)
                answer = " ".join(answer_parts).strip()
                if question and answer:
                    faqs.append({"question": question, "answer": answer})

        if faqs:
            return faqs

        # Strategy 3: Look for FAQ sections by heading
        faq_section = None
        for heading in container.find_all(re.compile(r"^h[1-6]$")):
            heading_text = heading.get_text(strip=True).lower()
            if any(kw in heading_text for kw in ["faq", "frequently asked", "common questions", "questions and answers"]):
                faq_section = heading
                break

        if faq_section:
            # Collect Q&A pairs: subheadings are questions, following <p> are answers
            current_q = None
            current_a = []

            for sibling in faq_section.find_all_next():
                # Stop at next same-level or higher heading
                if sibling.name and re.match(r"^h[1-6]$", sibling.name):
                    sib_level = int(sibling.name[1])
                    faq_level = int(faq_section.name[1])
                    if sib_level <= faq_level:
                        break
                    # This is a sub-heading = question
                    if current_q:
                        faqs.append({"question": current_q, "answer": " ".join(current_a).strip()})
                    current_q = sibling.get_text(strip=True)
                    current_a = []
                elif sibling.name in ("p", "div") and current_q:
                    text = sibling.get_text(separator=" ", strip=True)
                    if text:
                        current_a.append(text)

            if current_q and current_a:
                faqs.append({"question": current_q, "answer": " ".join(current_a).strip()})

        # Strategy 4: Bold text followed by paragraph (common FAQ pattern)
        if not faqs:
            for p in container.find_all("p"):
                strong = p.find(["strong", "b"])
                if strong:
                    q_text = strong.get_text(strip=True)
                    if q_text.endswith("?") and len(q_text) > 10:
                        # Answer is the rest of this paragraph + next paragraph
                        remaining = p.get_text(strip=True).replace(q_text, "").strip()
                        next_p = p.find_next_sibling("p")
                        if next_p:
                            remaining += " " + next_p.get_text(strip=True)
                        if remaining:
                            faqs.append({"question": q_text, "answer": remaining.strip()})

        return faqs

    def _extract_meta(self, soup: BeautifulSoup) -> str:
        """Extract meta description."""
        meta = soup.find("meta", attrs={"name": "description"})
        if meta and meta.get("content"):
            return meta["content"].strip()
        og_desc = soup.find("meta", property="og:description")
        if og_desc and og_desc.get("content"):
            return og_desc["content"].strip()
        return ""

    def _extract_schema(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract structured data / schema.org markup."""
        schemas = {}
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                import json
                data = json.loads(script.string or "")
                if isinstance(data, dict):
                    schema_type = data.get("@type", "unknown")
                    schemas[schema_type] = data
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            schema_type = item.get("@type", "unknown")
                            schemas[schema_type] = item
            except (json.JSONDecodeError, Exception):
                pass
        return schemas

    @staticmethod
    def _is_boilerplate(text: str) -> bool:
        """Detect boilerplate text that shouldn't be in body."""
        lower = text.lower()
        boilerplate_signals = [
            "cookie", "privacy policy", "terms of service", "all rights reserved",
            "subscribe to", "sign up for", "follow us on", "share this",
            "advertisement", "sponsored", "click here to", "accept cookies",
            "we use cookies", "copyright ©", "powered by",
        ]
        return any(signal in lower for signal in boilerplate_signals)


# ═══════════════════════════════════════════════════════════════
# SCRAPE ORCHESTRATOR — coordinates fetch + extract + store
# ═══════════════════════════════════════════════════════════════

async def scrape_urls(
    urls: List[str],
    project_id: str = "default",
) -> Dict[str, Any]:
    """
    Scrape multiple URLs: fetch, clean, extract, store.
    Returns summary with successes and failures.
    """
    fetcher = PageFetcher()
    extractor = ContentExtractor()

    results = {
        "total": len(urls),
        "scraped": 0,
        "failed": 0,
        "errors": [],
        "details": [],
    }

    try:
        # Process URLs concurrently with semaphore
        tasks = [_scrape_single(fetcher, extractor, url, project_id) for url in urls]
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)

        for outcome in outcomes:
            if isinstance(outcome, Exception):
                results["failed"] += 1
                results["errors"].append(str(outcome))
            elif outcome.get("success"):
                results["scraped"] += 1
                results["details"].append(outcome)
            else:
                results["failed"] += 1
                results["errors"].append(outcome.get("error", "Unknown error"))
                results["details"].append(outcome)

    finally:
        await fetcher.close()

    return results


async def _scrape_single(
    fetcher: PageFetcher,
    extractor: ContentExtractor,
    url: str,
    project_id: str,
) -> Dict[str, Any]:
    """Scrape a single URL and store results."""
    # Fetch
    fetch_result = await fetcher.fetch(url)

    if not fetch_result["html"]:
        # Update source status to failed
        await execute(
            "UPDATE sources SET scrape_status = 'failed' WHERE project_id = ? AND url = ?",
            (project_id, url),
        )
        return {
            "success": False,
            "url": url,
            "status_code": fetch_result["status_code"],
            "error": fetch_result["error"],
        }

    # Extract
    try:
        extracted = extractor.extract(fetch_result["html"], url)
    except Exception as e:
        logger.error("Extraction failed for %s: %s", url, e)
        await execute(
            "UPDATE sources SET scrape_status = 'failed' WHERE project_id = ? AND url = ?",
            (project_id, url),
        )
        return {"success": False, "url": url, "error": f"Extraction error: {str(e)}"}

    # Get or create source record
    source = await fetch_one(
        "SELECT id FROM sources WHERE project_id = ? AND url = ?",
        (project_id, url),
    )

    source_id = source["id"] if source else gen_id("src-")

    if not source:
        await execute(
            "INSERT INTO sources (id, project_id, url, title, scrape_status, scraped_at) "
            "VALUES (?, ?, ?, ?, 'scraped', ?)",
            (source_id, project_id, url, extracted["title"], datetime.utcnow().isoformat()),
        )
    else:
        await execute(
            "UPDATE sources SET scrape_status = 'scraped', scraped_at = ?, title = ? "
            "WHERE id = ?",
            (datetime.utcnow().isoformat(), extracted["title"] or url, source_id),
        )

    # Store scraped content (upsert)
    existing = await fetch_one(
        "SELECT id FROM scraped_content WHERE source_id = ?", (source_id,)
    )

    if existing:
        await execute(
            "UPDATE scraped_content SET title = ?, headings = ?, body_text = ?, "
            "lists = ?, faqs = ?, meta_description = ?, schema_markup = ?, "
            "word_count = ?, raw_html_hash = ?, scraped_at = ? WHERE id = ?",
            (
                extracted["title"], to_json(extracted["headings"]),
                extracted["body_text"], to_json(extracted["lists"]),
                to_json(extracted["faqs"]), extracted["meta_description"],
                to_json(extracted["schema_markup"]), extracted["word_count"],
                extracted["raw_html_hash"], datetime.utcnow().isoformat(),
                existing["id"],
            ),
        )
    else:
        await execute(
            "INSERT INTO scraped_content (id, source_id, url, title, headings, body_text, "
            "lists, faqs, meta_description, schema_markup, word_count, raw_html_hash) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                gen_id("sc-"), source_id, url, extracted["title"],
                to_json(extracted["headings"]), extracted["body_text"],
                to_json(extracted["lists"]), to_json(extracted["faqs"]),
                extracted["meta_description"], to_json(extracted["schema_markup"]),
                extracted["word_count"], extracted["raw_html_hash"],
            ),
        )

    return {
        "success": True,
        "url": url,
        "title": extracted["title"],
        "word_count": extracted["word_count"],
        "headings_count": len(extracted["headings"]),
        "lists_count": len(extracted["lists"]),
        "faqs_count": len(extracted["faqs"]),
        "has_schema": bool(extracted["schema_markup"]),
    }
