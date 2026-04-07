"""
Momentus AI — Peec Connector
Real API access check, CSV import with field mapping,
data normalization, source deduplication, and feedback loop.
"""
from __future__ import annotations

import csv
import io
import json
import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .config import settings
from .database import (
    execute, execute_many, fetch_all, fetch_one, gen_id, insert_returning_id, to_json, from_json,
)
from .models import (
    PEEC_FIELD_MAP, Measurement, PeecConnectionStatus, PeecRecord,
    normalize_model_source, Source, Cluster,
)

logger = logging.getLogger("geo.peec")


# ═══════════════════════════════════════════════════════════════
# PEEC API CLIENT
# ═══════════════════════════════════════════════════════════════

class PeecClient:
    """
    HTTP client for the Peec Customer API.
    Currently in beta — Enterprise only.
    Falls back to CSV import when API is unavailable.
    """

    def __init__(self, api_key: str = "", base_url: str = ""):
        self.api_key = api_key or settings.peec_api_key
        self.base_url = (base_url or settings.peec_api_base_url).rstrip("/")
        self.timeout = settings.peec_api_timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "User-Agent": "MomentusAI/1.0",
                },
                timeout=self.timeout,
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ── Connection Check ──

    async def check_connection(self) -> PeecConnectionStatus:
        """
        Real API access check.
        Attempts to reach Peec API and validates the key.
        Returns detailed connection status.
        """
        status = PeecConnectionStatus(
            has_api_key=bool(self.api_key),
            base_url=self.base_url,
        )

        if not self.api_key:
            status.message = "No API key configured. Use CSV import as fallback."
            return status

        try:
            client = await self._get_client()

            # Try health/status endpoint first
            for endpoint in ["/health", "/status", "/v1/health", "/me", "/projects"]:
                try:
                    resp = await client.get(endpoint)
                    if resp.status_code == 200:
                        status.connected = True
                        status.api_available = True
                        status.available_endpoints.append(endpoint)
                        status.message = "Connected to Peec API successfully."
                    elif resp.status_code == 401:
                        status.api_available = True
                        status.message = "Peec API reachable but API key is invalid or expired."
                    elif resp.status_code == 403:
                        status.api_available = True
                        status.message = (
                            "Peec API reachable but access denied. "
                            "The Customer API is currently in beta for Enterprise customers."
                        )
                    elif resp.status_code == 404:
                        continue  # try next endpoint
                    else:
                        status.api_available = True
                        status.message = f"Peec API returned status {resp.status_code}."
                except httpx.ConnectError:
                    continue

            if not status.api_available:
                status.message = (
                    "Could not reach Peec API at the configured URL. "
                    "Verify the endpoint or use CSV export as fallback."
                )

            # If connected, probe for available data endpoints
            if status.connected:
                for ep in ["/urls", "/prompts", "/topics", "/models", "/projects", "/reports"]:
                    try:
                        resp = await client.get(ep)
                        if resp.status_code in (200, 201):
                            status.available_endpoints.append(ep)
                    except Exception:
                        pass

        except httpx.TimeoutException:
            status.message = f"Connection to Peec API timed out after {self.timeout}s."
        except httpx.ConnectError as e:
            status.message = f"Cannot connect to Peec API: {str(e)}"
        except Exception as e:
            status.message = f"Unexpected error checking Peec API: {str(e)}"

        return status

    # ── Live Data Fetch ──

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
    )
    async def fetch_url_report(self, project_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch URL report from Peec API.
        Returns list of raw records with URL, title, usage_count,
        citation_count, citation_rate, retrievals, etc.
        """
        client = await self._get_client()
        params = {}
        if project_id:
            params["project_id"] = project_id

        resp = await client.get("/urls", params=params)
        resp.raise_for_status()
        data = resp.json()

        # Handle both {data: [...]} and direct [...] response formats
        if isinstance(data, dict) and "data" in data:
            return data["data"]
        elif isinstance(data, list):
            return data
        else:
            logger.warning("Unexpected Peec URL report format: %s", type(data))
            return []

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
    )
    async def fetch_prompts(self, project_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch prompts/queries from Peec."""
        client = await self._get_client()
        params = {}
        if project_id:
            params["project_id"] = project_id

        resp = await client.get("/prompts", params=params)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", data) if isinstance(data, dict) else data

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
    )
    async def fetch_measurements(
        self, urls: Optional[List[str]] = None, project_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Fetch current visibility/citation data from Peec for specific URLs.
        Used in the feedback loop after publishing content.
        """
        client = await self._get_client()
        params = {}
        if project_id:
            params["project_id"] = project_id

        if urls:
            # POST with URL list for bulk measurement
            resp = await client.post("/urls/report", json={"urls": urls}, params=params)
        else:
            resp = await client.get("/urls", params=params)

        resp.raise_for_status()
        data = resp.json()
        return data.get("data", data) if isinstance(data, dict) else data


# ═══════════════════════════════════════════════════════════════
# CSV IMPORT WITH FIELD MAPPING
# ═══════════════════════════════════════════════════════════════

class PeecFieldMapper:
    """
    Maps Peec CSV/API columns to our normalized schema.
    Handles multiple column naming conventions.
    """

    def __init__(self, custom_mapping: Optional[Dict[str, str]] = None):
        """
        custom_mapping: {our_field: csv_column_name} for overrides
        """
        self.custom_mapping = custom_mapping or {}

    def detect_columns(self, headers: List[str]) -> Dict[str, int]:
        """
        Auto-detect which CSV column maps to which normalized field.
        Returns {our_field_name: column_index}.
        """
        header_lower = [h.strip().lower().replace('"', '').replace("'", "") for h in headers]
        mapping = {}

        for our_field, aliases in PEEC_FIELD_MAP.items():
            # Check custom mapping first
            if our_field in self.custom_mapping:
                custom_col = self.custom_mapping[our_field].lower()
                if custom_col in header_lower:
                    mapping[our_field] = header_lower.index(custom_col)
                    continue

            # Try each alias
            for alias in aliases:
                if alias in header_lower:
                    mapping[our_field] = header_lower.index(alias)
                    break

        return mapping

    def validate_mapping(self, mapping: Dict[str, int], headers: List[str]) -> Dict[str, Any]:
        """
        Validate detected mapping. URL is required; everything else optional.
        Returns validation result with warnings.
        """
        result = {
            "valid": "url" in mapping,
            "mapped_fields": list(mapping.keys()),
            "unmapped_fields": [f for f in PEEC_FIELD_MAP.keys() if f not in mapping],
            "extra_columns": [],
            "warnings": [],
        }

        if "url" not in mapping:
            result["warnings"].append("CRITICAL: No URL column detected. Import cannot proceed.")
        if "citation_count" not in mapping:
            result["warnings"].append("No citation_count column found — citation metrics will be zero.")
        if "model_source" not in mapping:
            result["warnings"].append("No model_source column found — all records will be tagged 'Other'.")

        # Identify extra columns not in our schema
        mapped_indices = set(mapping.values())
        for i, h in enumerate(headers):
            if i not in mapped_indices:
                result["extra_columns"].append(h)

        return result

    def parse_row(self, row: List[str], mapping: Dict[str, int]) -> Dict[str, Any]:
        """Parse a single CSV row using the detected mapping."""
        def get(field: str, default: Any = "") -> str:
            idx = mapping.get(field)
            if idx is not None and idx < len(row):
                return row[idx].strip().strip('"').strip("'")
            return str(default)

        tags_raw = get("tags", "")
        tags = []
        if tags_raw:
            # Handle semicolon, comma, or pipe-separated tags
            for sep in [";", "|", ","]:
                if sep in tags_raw:
                    tags = [t.strip() for t in tags_raw.split(sep) if t.strip()]
                    break
            if not tags:
                tags = [tags_raw.strip()]

        url_raw = get("url")
        urls = self._extract_urls(url_raw)

        result = {
            "url": urls[0] if urls else url_raw,
            "title": get("title"),
            "usage_count": self._safe_int(get("usage_count", "0")),
            "citation_count": self._safe_int(get("citation_count", "0")),
            "citation_rate": self._safe_float(get("citation_rate", "0")),
            "retrievals": self._safe_int(get("retrievals", "0")),
            "topic": get("topic", "Uncategorized") or "Uncategorized",
            "tags": tags,
            "model_source": normalize_model_source(get("model_source", "Other")),
            "raw_data": {h: row[i] if i < len(row) else "" for i, h in enumerate(row)},
        }

        # If sources column has multiple URLs, attach them for expansion
        if len(urls) > 1:
            result["_urls"] = urls

        return result

    @staticmethod
    def _extract_urls(text: str) -> list:
        """Extract all URLs from a text field (handles multi-URL sources column)."""
        import re
        if not text:
            return []
        urls = re.findall(r'https?://[^\s,;\]\)\"\']+', text)
        # Clean trailing punctuation
        urls = [u.rstrip('.,;)>]') for u in urls]
        return urls if urls else [text] if text.strip() else []

    @staticmethod
    def _safe_int(val: str) -> int:
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return 0

    @staticmethod
    def _safe_float(val: str) -> float:
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0.0


def parse_csv_content(
    content: str,
    project_id: str = "default",
    custom_mapping: Optional[Dict[str, str]] = None,
) -> Tuple[List[PeecRecord], Dict[str, Any]]:
    """
    Parse CSV content into normalized PeecRecords.
    Returns (records, validation_report).
    """
    mapper = PeecFieldMapper(custom_mapping)

    reader = csv.reader(io.StringIO(content))
    rows = list(reader)

    if len(rows) < 2:
        return [], {"valid": False, "warnings": ["CSV has fewer than 2 rows"], "mapped_fields": [], "unmapped_fields": []}

    headers = rows[0]
    col_mapping = mapper.detect_columns(headers)
    validation = mapper.validate_mapping(col_mapping, headers)

    if not validation["valid"]:
        return [], validation

    batch_id = gen_id("batch-")
    records = []
    parse_errors = []

    for i, row in enumerate(rows[1:], start=2):
        if not any(cell.strip() for cell in row):
            continue  # skip empty rows
        try:
            parsed = mapper.parse_row(row, col_mapping)
            if not parsed["url"]:
                parse_errors.append(f"Row {i}: empty URL, skipped")
                continue

            # Expand multi-URL rows (e.g. sources column with multiple URLs)
            extra_urls = parsed.pop("_urls", None)
            if extra_urls and len(extra_urls) > 1:
                for url in extra_urls:
                    if not url.strip():
                        continue
                    records.append(PeecRecord(
                        id=gen_id("pr-"),
                        project_id=project_id,
                        import_batch_id=batch_id,
                        imported_at=datetime.utcnow().isoformat(),
                        url=url.strip(),
                        **{k: v for k, v in parsed.items() if k not in ("url", "raw_data")},
                        raw_data=parsed.get("raw_data", {}),
                    ))
            else:
                records.append(PeecRecord(
                    id=gen_id("pr-"),
                    project_id=project_id,
                    import_batch_id=batch_id,
                    imported_at=datetime.utcnow().isoformat(),
                    **{k: v for k, v in parsed.items() if k != "raw_data"},
                    raw_data=parsed.get("raw_data", {}),
                ))
        except Exception as e:
            parse_errors.append(f"Row {i}: {str(e)}")

    validation["total_rows"] = len(rows) - 1
    validation["parsed_records"] = len(records)
    validation["parse_errors"] = parse_errors
    validation["batch_id"] = batch_id

    return records, validation


# ═══════════════════════════════════════════════════════════════
# DATA INGESTION — Save to database
# ═══════════════════════════════════════════════════════════════

async def ingest_records(records: List[PeecRecord], project_id: str = "default") -> Dict[str, Any]:
    """
    Save normalized Peec records to database.
    Also generates deduplicated sources and topic clusters.
    Returns summary of what was created.
    """
    if not records:
        return {"records": 0, "sources": 0, "clusters": 0}

    # 0. Ensure project exists (auto-create if missing)
    existing_project = await fetch_one(
        "SELECT id FROM projects WHERE id = ?", (project_id,)
    )
    if not existing_project:
        await execute(
            "INSERT INTO projects (id, name) VALUES (?, ?)",
            (project_id, project_id),
        )

    # 1. Insert Peec records
    await execute_many(
        "INSERT INTO peec_records (id, project_id, url, title, usage_count, citation_count, "
        "citation_rate, retrievals, topic, tags, model_source, raw_data, import_batch_id, imported_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (r.id, project_id, r.url, r.title, r.usage_count, r.citation_count,
             r.citation_rate, r.retrievals, r.topic, to_json(r.tags), r.model_source,
             to_json(r.raw_data), r.import_batch_id, r.imported_at)
            for r in records
        ],
    )

    # 2. Generate deduplicated sources
    source_count = await _generate_sources(records, project_id)

    # 3. Generate topic clusters
    cluster_count = await _generate_clusters(records, project_id)

    return {
        "records": len(records),
        "sources": source_count,
        "clusters": cluster_count,
        "batch_id": records[0].import_batch_id if records else "",
    }


async def _generate_sources(records: List[PeecRecord], project_id: str) -> int:
    """Deduplicate URLs and create/update source records."""
    url_map: Dict[str, Dict] = {}

    for r in records:
        if r.url not in url_map:
            url_map[r.url] = {
                "url": r.url,
                "title": r.title,
                "total_citation_count": 0,
                "max_citation_rate": 0.0,
                "topics": set(),
                "model_sources": set(),
            }
        entry = url_map[r.url]
        entry["total_citation_count"] += r.citation_count
        entry["max_citation_rate"] = max(entry["max_citation_rate"], r.citation_rate)
        entry["topics"].add(r.topic)
        entry["model_sources"].add(r.model_source)
        if not entry["title"] and r.title:
            entry["title"] = r.title

    for url, entry in url_map.items():
        existing = await fetch_one(
            "SELECT id FROM sources WHERE project_id = ? AND url = ?",
            (project_id, url),
        )
        if existing:
            await execute(
                "UPDATE sources SET total_citation_count = ?, max_citation_rate = ?, "
                "topics = ?, model_sources = ?, title = ? WHERE id = ?",
                (
                    entry["total_citation_count"],
                    entry["max_citation_rate"],
                    to_json(sorted(entry["topics"])),
                    to_json(sorted(entry["model_sources"])),
                    entry["title"],
                    existing["id"],
                ),
            )
        else:
            await execute(
                "INSERT INTO sources (id, project_id, url, title, total_citation_count, "
                "max_citation_rate, topics, model_sources) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    gen_id("src-"), project_id, url, entry["title"],
                    entry["total_citation_count"], entry["max_citation_rate"],
                    to_json(sorted(entry["topics"])), to_json(sorted(entry["model_sources"])),
                ),
            )

    return len(url_map)


async def _generate_clusters(records: List[PeecRecord], project_id: str) -> int:
    """Generate topic clusters from Peec records."""
    topic_map: Dict[str, Dict] = {}

    for r in records:
        topic = r.topic or "Uncategorized"
        if topic not in topic_map:
            topic_map[topic] = {
                "name": topic,
                "items": [],
                "urls": set(),
                "tags": set(),
            }
        topic_map[topic]["items"].append(r)
        topic_map[topic]["urls"].add(r.url)
        for t in r.tags:
            topic_map[topic]["tags"].add(t)

    count = 0
    for topic, data in topic_map.items():
        cluster_id = gen_id("cl-")
        items = data["items"]
        avg_rate = sum(i.citation_rate for i in items) / len(items) if items else 0
        total_cit = sum(i.citation_count for i in items)

        # Check if cluster already exists for this topic
        existing = await fetch_one(
            "SELECT id FROM clusters WHERE project_id = ? AND name = ?",
            (project_id, topic),
        )

        if existing:
            cluster_id = existing["id"]
            await execute(
                "UPDATE clusters SET prompt_count = ?, url_count = ?, "
                "avg_citation_rate = ?, total_citations = ?, tags = ? WHERE id = ?",
                (len(items), len(data["urls"]), avg_rate, total_cit,
                 to_json(sorted(data["tags"])), cluster_id),
            )
        else:
            await execute(
                "INSERT INTO clusters (id, project_id, name, topics, tags, prompt_count, "
                "url_count, avg_citation_rate, total_citations) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    cluster_id, project_id, topic,
                    to_json([topic]), to_json(sorted(data["tags"])),
                    len(items), len(data["urls"]), avg_rate, total_cit,
                ),
            )
            count += 1

        # Link records to cluster
        for item in items:
            await execute(
                "INSERT OR IGNORE INTO cluster_records (cluster_id, peec_record_id) VALUES (?, ?)",
                (cluster_id, item.id),
            )

        # Link sources to cluster
        for url in data["urls"]:
            source = await fetch_one(
                "SELECT id FROM sources WHERE project_id = ? AND url = ?",
                (project_id, url),
            )
            if source:
                await execute(
                    "INSERT OR IGNORE INTO cluster_sources (cluster_id, source_id) VALUES (?, ?)",
                    (cluster_id, source["id"]),
                )

    return count


# ═══════════════════════════════════════════════════════════════
# LIVE INGESTION FROM PEEC API
# ═══════════════════════════════════════════════════════════════

async def ingest_from_api(
    api_key: str = "",
    base_url: str = "",
    project_id: str = "default",
) -> Dict[str, Any]:
    """
    Fetch data from Peec API and ingest into database.
    Full pipeline: connect → fetch → normalize → store.
    """
    client = PeecClient(api_key=api_key, base_url=base_url)

    try:
        # Step 1: Check connection
        status = await client.check_connection()
        if not status.connected:
            return {"success": False, "error": status.message, "connection": status.dict()}

        # Step 2: Fetch URL report
        raw_data = await client.fetch_url_report(project_id)
        if not raw_data:
            return {"success": False, "error": "No data returned from Peec API"}

        # Step 3: Normalize records
        batch_id = gen_id("api-batch-")
        mapper = PeecFieldMapper()
        records = []

        for raw in raw_data:
            # API returns dict, not CSV row — map directly
            record = PeecRecord(
                id=gen_id("pr-"),
                project_id=project_id,
                url=raw.get("url", raw.get("source_url", "")),
                title=raw.get("title", raw.get("page_title", "")),
                usage_count=mapper._safe_int(str(raw.get("usage_count", 0))),
                citation_count=mapper._safe_int(str(raw.get("citation_count", 0))),
                citation_rate=mapper._safe_float(str(raw.get("citation_rate", 0))),
                retrievals=mapper._safe_int(str(raw.get("retrievals", 0))),
                topic=raw.get("topic", raw.get("category", "Uncategorized")),
                tags=raw.get("tags", []) if isinstance(raw.get("tags"), list) else [],
                model_source=normalize_model_source(str(raw.get("model_source", raw.get("model", "Other")))),
                import_batch_id=batch_id,
                imported_at=datetime.utcnow().isoformat(),
                raw_data=raw,
            )
            if record.url:
                records.append(record)

        # Step 4: Ingest into database
        result = await ingest_records(records, project_id)
        result["success"] = True
        result["source"] = "api"
        return result

    except httpx.HTTPStatusError as e:
        return {"success": False, "error": f"Peec API error: HTTP {e.response.status_code}"}
    except Exception as e:
        return {"success": False, "error": f"Ingestion failed: {str(e)}"}
    finally:
        await client.close()


# ═══════════════════════════════════════════════════════════════
# FEEDBACK LOOP — Peec Re-measurement
# ═══════════════════════════════════════════════════════════════

async def measure_performance(
    project_id: str = "default",
    urls: Optional[List[str]] = None,
    draft_id: Optional[str] = None,
    api_key: str = "",
    base_url: str = "",
) -> Dict[str, Any]:
    """
    Re-measure performance of published URLs via Peec.
    Stores measurements and computes deltas against baselines.
    """
    client = PeecClient(api_key=api_key, base_url=base_url)

    try:
        # If no URLs specified, get all published URLs
        if not urls:
            published = await fetch_all(
                "SELECT DISTINCT e.published_url, d.id as draft_id "
                "FROM exports e JOIN drafts d ON e.draft_id = d.id "
                "WHERE d.status = 'approved' AND e.published_url != ''",
            )
            if not published:
                return {"success": False, "error": "No published URLs to measure"}
            urls = [p["published_url"] for p in published]

        # Fetch current measurements from Peec
        raw_measurements = await client.fetch_measurements(urls, project_id)

        results = []
        for raw in raw_measurements:
            url = raw.get("url", "")
            if not url:
                continue

            m_id = gen_id("ms-")
            await execute(
                "INSERT INTO measurements (id, project_id, url, citation_count, citation_rate, "
                "visibility, position, sentiment, model_source, brand_mentions, draft_id, measured_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    m_id, project_id, url,
                    raw.get("citation_count", 0),
                    raw.get("citation_rate", 0.0),
                    raw.get("visibility", 0.0),
                    raw.get("position", 0.0),
                    raw.get("sentiment", ""),
                    raw.get("model_source", ""),
                    raw.get("brand_mentions", 0),
                    draft_id or "",
                    datetime.utcnow().isoformat(),
                ),
            )

            # Compute delta against baseline (first measurement for this URL)
            baseline = await fetch_one(
                "SELECT citation_count, citation_rate, visibility, position "
                "FROM measurements WHERE project_id = ? AND url = ? "
                "ORDER BY measured_at ASC LIMIT 1",
                (project_id, url),
            )

            delta = {}
            if baseline:
                for metric in ["citation_count", "citation_rate", "visibility", "position"]:
                    before = baseline.get(metric, 0) or 0
                    after = raw.get(metric, 0)
                    diff = after - before
                    pct = (diff / before * 100) if before else 0
                    delta[metric] = {
                        "before": before,
                        "after": after,
                        "delta": diff,
                        "delta_pct": round(pct, 2),
                        "direction": "improved" if diff > 0 else ("declined" if diff < 0 else "stable"),
                    }

            results.append({"url": url, "measurements": raw, "deltas": delta})

        return {"success": True, "measured_urls": len(results), "results": results}

    except Exception as e:
        return {"success": False, "error": f"Measurement failed: {str(e)}"}
    finally:
        await client.close()


# ═══════════════════════════════════════════════════════════════
# DATA ACCESS HELPERS
# ═══════════════════════════════════════════════════════════════

async def get_project_records(project_id: str) -> List[Dict]:
    """Get all Peec records for a project."""
    rows = await fetch_all(
        "SELECT * FROM peec_records WHERE project_id = ? ORDER BY citation_count DESC",
        (project_id,),
    )
    for r in rows:
        r["tags"] = from_json(r.get("tags", "[]"), [])
        r["raw_data"] = from_json(r.get("raw_data", "{}"), {})
    return rows


async def get_project_sources(project_id: str) -> List[Dict]:
    """Get all deduplicated sources for a project, ranked by citation count."""
    rows = await fetch_all(
        "SELECT * FROM sources WHERE project_id = ? ORDER BY total_citation_count DESC",
        (project_id,),
    )
    for r in rows:
        r["topics"] = from_json(r.get("topics", "[]"), [])
        r["model_sources"] = from_json(r.get("model_sources", "[]"), [])
    return rows


async def get_project_clusters(project_id: str) -> List[Dict]:
    """Get all clusters for a project."""
    rows = await fetch_all(
        "SELECT * FROM clusters WHERE project_id = ? ORDER BY total_citations DESC",
        (project_id,),
    )
    for r in rows:
        r["topics"] = from_json(r.get("topics", "[]"), [])
        r["tags"] = from_json(r.get("tags", "[]"), [])
    return rows


async def get_cluster_sources(cluster_id: str) -> List[Dict]:
    """Get all sources linked to a cluster with their scraped content."""
    rows = await fetch_all(
        "SELECT s.*, sc.body_text, sc.headings, sc.faqs, sc.word_count as scraped_word_count "
        "FROM sources s "
        "JOIN cluster_sources cs ON s.id = cs.source_id "
        "LEFT JOIN scraped_content sc ON s.id = sc.source_id "
        "WHERE cs.cluster_id = ? "
        "ORDER BY s.total_citation_count DESC",
        (cluster_id,),
    )
    for r in rows:
        r["topics"] = from_json(r.get("topics", "[]"), [])
        r["model_sources"] = from_json(r.get("model_sources", "[]"), [])
        r["headings"] = from_json(r.get("headings", "[]"), [])
        r["faqs"] = from_json(r.get("faqs", "[]"), [])
    return rows
