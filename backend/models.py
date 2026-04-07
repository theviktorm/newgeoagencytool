"""
Momentus AI — Pydantic Models
Every API request/response contract lives here.
Normalized data model for the entire pipeline.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ═══════════════════════════════════════════════════════════════
# NORMALIZED DATA MODEL — the canonical internal format
# ═══════════════════════════════════════════════════════════════

class PeecRecord(BaseModel):
    """Normalized record from Peec data. Every field mapped and validated."""
    model_config = {"protected_namespaces": ()}
    id: str = ""
    url: str
    title: str = ""
    usage_count: int = 0
    citation_count: int = 0
    citation_rate: float = 0.0
    retrievals: int = 0
    topic: str = "Uncategorized"
    tags: List[str] = Field(default_factory=list)
    model_source: str = "Other"
    project_id: str = ""
    import_batch_id: str = ""
    imported_at: str = ""
    raw_data: Dict[str, Any] = Field(default_factory=dict)


class Source(BaseModel):
    """Deduplicated source URL with aggregated metrics."""
    model_config = {"protected_namespaces": ()}
    id: str = ""
    url: str
    title: str = ""
    total_citation_count: int = 0
    max_citation_rate: float = 0.0
    topics: List[str] = Field(default_factory=list)
    model_sources: List[str] = Field(default_factory=list)
    scrape_status: str = "pending"
    scraped_at: Optional[str] = None
    created_at: str = ""


class ScrapedContent(BaseModel):
    """Structured extraction from a scraped page."""
    id: str = ""
    source_id: str = ""
    url: str = ""
    title: str = ""
    headings: List[Dict[str, Any]] = Field(default_factory=list)   # [{level: int, text: str}]
    body_text: str = ""
    lists: List[Dict[str, Any]] = Field(default_factory=list)      # [{type: "ul"|"ol", items: [str]}]
    faqs: List[Dict[str, Any]] = Field(default_factory=list)       # [{question: str, answer: str}]
    meta_description: str = ""
    schema_markup: Dict[str, Any] = Field(default_factory=dict)
    word_count: int = 0
    scraped_at: str = ""
    error: Optional[str] = None


class Cluster(BaseModel):
    """Topic/intent cluster grouping related Peec records."""
    id: str = ""
    project_id: str = ""
    name: str
    topics: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)
    prompt_count: int = 0
    url_count: int = 0
    avg_citation_rate: float = 0.0
    total_citations: int = 0
    created_at: str = ""


class Analysis(BaseModel):
    """Claude source analysis result."""
    id: str = ""
    cluster_id: str = ""
    prompt_template: str = ""
    prompt_type: str = "blog"
    model: str = ""
    temperature: float = 0.7
    result: str = ""
    tokens_input: int = 0
    tokens_output: int = 0
    cost: float = 0.0
    cached_tokens: int = 0
    created_at: str = ""


class Brief(BaseModel):
    """Generated content brief."""
    id: str = ""
    cluster_id: str = ""
    analysis_id: str = ""
    content: str = ""
    model: str = ""
    tokens_input: int = 0
    tokens_output: int = 0
    cost: float = 0.0
    created_at: str = ""


class Draft(BaseModel):
    """Generated content draft with version tracking."""
    id: str = ""
    cluster_id: str = ""
    brief_id: str = ""
    version: int = 1
    template_type: str = "blog"
    content: str = ""
    word_count: int = 0
    model: str = ""
    temperature: float = 0.7
    tokens_input: int = 0
    tokens_output: int = 0
    cost: float = 0.0
    status: str = "pending"
    reviewer_notes: str = ""
    reviewed_at: Optional[str] = None
    parent_draft_id: Optional[str] = None
    source_snapshot: Dict[str, Any] = Field(default_factory=dict)
    prompt_snapshot: str = ""
    created_at: str = ""


class Measurement(BaseModel):
    """Peec feedback measurement for tracking content performance."""
    model_config = {"protected_namespaces": ()}
    id: str = ""
    project_id: str = ""
    url: str
    citation_count: int = 0
    citation_rate: float = 0.0
    visibility: float = 0.0
    position: float = 0.0
    sentiment: str = ""
    model_source: str = ""
    brand_mentions: int = 0
    draft_id: Optional[str] = None
    measured_at: str = ""


class SuccessMetric(BaseModel):
    """Defined success metric with target."""
    id: str = ""
    project_id: str = ""
    metric_name: str
    target_value: float = 0.0
    baseline_value: float = 0.0
    current_value: float = 0.0
    direction: str = "increase"  # increase | decrease


# ═══════════════════════════════════════════════════════════════
# API REQUEST MODELS
# ═══════════════════════════════════════════════════════════════

class PeecConnectRequest(BaseModel):
    """Request to connect to Peec API."""
    api_key: str
    base_url: str = "https://api.peec.ai/v1"
    project_id: Optional[str] = None


class PeecImportCSVRequest(BaseModel):
    """Metadata for CSV import. File sent as multipart."""
    project_id: str = "default"
    field_mapping: Optional[Dict[str, str]] = None  # custom column mapping


class ScrapeRequest(BaseModel):
    """Request to scrape one or more URLs."""
    urls: List[str]
    project_id: str = "default"


class AnalyzeRequest(BaseModel):
    """Request to run Claude source analysis on a cluster."""
    cluster_id: str
    prompt_type: str = "blog"  # blog | product | faq
    model: Optional[str] = None
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    use_cache: bool = True


class GenerateBriefRequest(BaseModel):
    """Request to generate a content brief from analysis."""
    cluster_id: str
    analysis_id: Optional[str] = None  # if not provided, uses latest
    model: Optional[str] = None
    temperature: Optional[float] = None


class GenerateDraftRequest(BaseModel):
    """Request to generate a content draft from a brief."""
    cluster_id: str
    brief_id: Optional[str] = None
    template_type: str = "blog"  # blog | faq | landing | kb
    model: Optional[str] = None
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None


class ReviewDraftRequest(BaseModel):
    """Request to update a draft's review status."""
    status: str  # approved | rejected | revision
    notes: str = ""


class ExportRequest(BaseModel):
    """Request to export a draft."""
    draft_id: str
    format: str = "markdown"  # markdown | html | plaintext


class PublishRequest(BaseModel):
    """Request to publish to a CMS."""
    draft_id: str
    cms: str  # wordpress | webflow
    title: Optional[str] = None
    slug: Optional[str] = None
    status: str = "draft"  # draft | publish


class BatchRequest(BaseModel):
    """Request for batch processing."""
    job_type: str  # scrape | analyze | generate
    project_id: str = "default"
    items: List[Dict[str, Any]] = Field(default_factory=list)


class MeasureRequest(BaseModel):
    """Request to take Peec measurements for published URLs."""
    project_id: str = "default"
    urls: Optional[List[str]] = None  # if None, measure all published
    draft_id: Optional[str] = None


class SuccessMetricRequest(BaseModel):
    """Request to define a success metric."""
    project_id: str = "default"
    metric_name: str
    target_value: float
    baseline_value: float = 0.0
    direction: str = "increase"


class PromptTemplateRequest(BaseModel):
    """Request to create/update a prompt template."""
    project_id: str = "default"
    name: str
    category: str  # source_analysis | brief_generation | content_generation
    sub_type: str = "blog"
    template: str


# ═══════════════════════════════════════════════════════════════
# API RESPONSE MODELS
# ═══════════════════════════════════════════════════════════════

class ApiResponse(BaseModel):
    """Standard API response wrapper."""
    success: bool = True
    data: Any = None
    error: Optional[str] = None
    meta: Dict[str, Any] = Field(default_factory=dict)


class PeecConnectionStatus(BaseModel):
    """Peec API connection check result."""
    connected: bool = False
    api_available: bool = False
    has_api_key: bool = False
    base_url: str = ""
    message: str = ""
    available_endpoints: List[str] = Field(default_factory=list)


class TokenUsageSummary(BaseModel):
    """Token usage and cost summary."""
    daily_cost: float = 0.0
    monthly_cost: float = 0.0
    daily_limit: float = 0.0
    monthly_limit: float = 0.0
    daily_ok: bool = True
    monthly_ok: bool = True
    allowed: bool = True
    total_tokens_input: int = 0
    total_tokens_output: int = 0
    total_cached: int = 0


class BatchJobStatus(BaseModel):
    """Status of a batch processing job."""
    id: str
    job_type: str
    status: str
    total_items: int
    completed_items: int
    failed_items: int
    progress_pct: float = 0.0
    error: str = ""
    started_at: Optional[str] = None
    completed_at: Optional[str] = None


class PerformanceDelta(BaseModel):
    """Before/after performance comparison for a URL."""
    url: str
    draft_id: str = ""
    metric: str
    before_value: float
    after_value: float
    delta: float
    delta_pct: float
    direction: str  # improved | declined | stable
    measured_at: str = ""


# ═══════════════════════════════════════════════════════════════
# PEEC FIELD MAPPING SPEC
# ═══════════════════════════════════════════════════════════════

# Maps every known Peec CSV/API column name to our normalized field.
# Multiple aliases per field to handle format variations.
PEEC_FIELD_MAP = {
    # Our field → list of Peec column aliases (case-insensitive)
    # Includes Peec Chat Export format: sources, user, mentions, model, etc.
    "url": ["url", "source_url", "page_url", "cited_url", "link", "sources"],
    "title": ["title", "page_title", "source_title", "name"],
    "usage_count": ["usage_count", "usage", "usagecount", "uses", "total_usage"],
    "citation_count": ["citation_count", "citations", "citationcount", "cited", "total_citations", "mentions"],
    "citation_rate": ["citation_rate", "citationrate", "rate", "cite_rate"],
    "retrievals": ["retrievals", "retrieval_count", "retrievalcount", "retrieved"],
    "topic": ["topic", "category", "subject", "theme", "user", "query", "prompt", "queries"],
    "tags": ["tags", "labels", "keywords", "tag_list"],
    "model_source": ["model_source", "modelsource", "model", "source_model", "ai_model", "engine"],
    "country": ["country", "region", "geo", "location"],
    "position": ["position", "rank", "pos"],
    "sentiment": ["sentiment", "sentiment_score"],
    "created": ["created", "created_at", "date", "timestamp", "measured_at"],
    "assistant": ["assistant", "response", "answer", "ai_response"],
}

# Validated model source names
VALID_MODEL_SOURCES = {"ChatGPT", "Perplexity", "Gemini", "Claude", "Copilot", "Other"}

def normalize_model_source(raw: str) -> str:
    """Normalize model source string to canonical form."""
    if not raw:
        return "Other"
    low = raw.lower().strip()
    mapping = {
        "chatgpt": "ChatGPT", "openai": "ChatGPT", "gpt": "ChatGPT", "gpt-4": "ChatGPT", "gpt-3.5": "ChatGPT",
        "perplexity": "Perplexity", "pplx": "Perplexity",
        "gemini": "Gemini", "google": "Gemini", "bard": "Gemini",
        "claude": "Claude", "anthropic": "Claude",
        "copilot": "Copilot", "bing": "Copilot",
    }
    for key, val in mapping.items():
        if key in low:
            return val
    return "Other"
