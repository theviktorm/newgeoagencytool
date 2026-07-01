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
# BASE REQUEST CLASS
# ═══════════════════════════════════════════════════════════════

class BaseRequest(BaseModel):
    """Base request model for all API endpoints."""
    pass


# ═══════════════════════════════════════════════════════════════
# NORMALIZED DATA MODEL — the canonical internal format
# ═══════════════════════════════════════════════════════════════

class PeecRecord(BaseModel):
    """Normalized record from Peec data. Every field mapped and validated."""
    model_config = {"protected_namespaces": ()}
    id: str = ""
    # Core Peec.ai fields
    prompt: str = Field(..., description="The AI prompt text")
    query: Optional[str] = Field(None, description="The search query if different from prompt")
    question: Optional[str] = Field(None, description="The question if different from prompt")
    ai_engine: str = Field(..., description="The AI engine/platform (e.g., ChatGPT, Gemini)")
    platform: Optional[str] = Field(None, description="Specific platform if different from engine")
    model: Optional[str] = Field(None, description="Specific model used (e.g., GPT-4, Claude 3)")
    date: datetime = Field(..., description="Date/timestamp of the Peec.ai run")
    run_id: Optional[str] = Field(None, description="Peec.ai run ID")

    # Brand-specific visibility
    brand_mentioned: bool = Field(False, description="True if the brand was mentioned")
    brand_rank: Optional[int] = Field(None, description="Brand's rank/position in the answer")
    brand_position: Optional[str] = Field(None, description="Brand's position description")
    visibility_score: Optional[float] = Field(None, description="Overall visibility score")
    sentiment: Optional[str] = Field(None, description="Sentiment towards the brand")
    share_of_voice: Optional[float] = Field(None, description="Brand's share of voice")

    # Answer content
    answer_text: Optional[str] = Field(None, description="Full AI answer text")
    answer_summary: Optional[str] = Field(None, description="Summary of the AI answer")

    # Citations
    cited_urls: List[str] = Field(default_factory=list, description="URLs cited in the AI answer")
    citation_urls: List[str] = Field(default_factory=list, description="URLs identified as citations")
    sources: List[str] = Field(default_factory=list, description="Source names/domains cited")
    source_type: Optional[str] = Field(None, description="Type of source (e.g., own website, review site)")
    url_cited: Optional[str] = Field(None, description="Specific URL cited")
    domain_cited: Optional[str] = Field(None, description="Domain of the cited URL")
    reddit_citation: Optional[str] = Field(None, description="Reddit citation/source if applicable")

    # Competitors
    competitor_names: List[str] = Field(default_factory=list, description="Names of competitors mentioned")
    competitor_domains: List[str] = Field(default_factory=list, description="Domains of competitors mentioned")
    competitor_ranks: List[int] = Field(default_factory=list, description="Ranks of competitors")
    competitor_positions: List[str] = Field(default_factory=list, description="Positions of competitors")
    competitor_visibility: Optional[float] = Field(None, description="Competitor visibility score")

    # Categorization
    topic: Optional[str] = Field(None, description="Topic of the prompt/answer")
    cluster: Optional[str] = Field(None, description="Cluster grouping for the prompt")
    tag: List[str] = Field(default_factory=list, description="Tags associated with the prompt")
    buyer_intent: Optional[str] = Field(None, description="Buyer intent of the prompt")

    # Geo-specifics
    location: Optional[str] = Field(None, description="Geographic location of the query")
    country: Optional[str] = Field(None, description="Country of the query")
    language: Optional[str] = Field(None, description="Language of the query")
    device: Optional[str] = Field(None, description="Device used for the query")

    # AI Overview specific
    aio_presence: Optional[bool] = Field(None, alias="AI_Overview_presence", description="True if AI Overview was present")

    # Internal tracking
    prompt_score: Optional[float] = Field(None, description="Internal score for the prompt")
    model_response_id: Optional[str] = Field(None, description="ID from the AI model response")
    project_id: str = Field(..., description="ID of the associated project")
    import_batch_id: str = Field(..., description="ID of the import batch")
    imported_at: datetime = Field(..., description="Timestamp of import")
    raw_data: Dict[str, Any] = Field(default_factory=dict, description="Raw data from the Peec.ai export")
    confidence: Optional[str] = Field(None, description="Confidence level of the data")
    data_source: Optional[str] = Field(None, description="Source of the data (e.g., Peec.ai import)")


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


class ImportBatch(BaseModel):
    """Record of a Peec.ai data import batch."""
    id: str = ""
    workspace_id: str
    file_name: str
    source: str = "Peec.ai"
    imported_by: str
    imported_at: datetime
    rows_total: int = 0
    rows_success: int = 0
    rows_failed: int = 0
    warnings: Optional[str] = None
    mapping_config: Dict[str, str] = Field(default_factory=dict)
    status: str = "completed"  # pending | processing | completed | failed


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
    "prompt": ["prompt", "query", "question", "prompt_text"],
    "ai_engine": ["ai_engine", "platform", "model", "engine", "ai_model"],
    "date": ["date", "timestamp", "run_date", "run_timestamp", "imported_at"],
    "run_id": ["run_id", "peec_run_id"],
    "brand_mentioned": ["brand_mentioned", "mentioned_brand", "brand_in_answer"],
    "brand_rank": ["brand_rank", "brand_position_rank"],
    "brand_position": ["brand_position", "brand_pos_desc"],
    "visibility_score": ["visibility_score", "overall_visibility"],
    "sentiment": ["sentiment", "sentiment_score"],
    "share_of_voice": ["share_of_voice", "sov"],
    "answer_text": ["answer_text", "ai_answer", "response_text"],
    "answer_summary": ["answer_summary", "ai_answer_summary"],
    "cited_urls": ["cited_urls", "citation_urls", "sources_urls", "cited_links"],
    "sources": ["sources", "source_names", "citation_sources"],
    "source_type": ["source_type", "citation_type"],
    "url_cited": ["url_cited", "single_cited_url"],
    "domain_cited": ["domain_cited", "cited_domain"],
    "reddit_citation": ["reddit_citation", "reddit_source"],
    "competitor_names": ["competitor_names", "competitors_mentioned"],
    "competitor_domains": ["competitor_domains", "competitor_urls"],
    "competitor_ranks": ["competitor_ranks", "competitor_positions_rank"],
    "competitor_positions": ["competitor_positions", "competitor_pos_desc"],
    "competitor_visibility": ["competitor_visibility", "comp_visibility_score"],
    "topic": ["topic", "category", "subject", "theme", "user", "query", "prompt", "queries"],
    "cluster": ["cluster", "topic_cluster"],
    "tag": ["tags", "labels", "keywords", "tag_list"],
    "buyer_intent": ["buyer_intent", "intent_type"],
    "location": ["location", "geo_location"],
    "country": ["country", "region", "geo"],
    "language": ["language", "lang"],
    "device": ["device", "user_device"],
    "aio_presence": ["aio_presence", "ai_overview_presence", "google_aio"],
    "prompt_score": ["prompt_score", "internal_prompt_score"],
    "model_response_id": ["model_response_id", "ai_response_id"],
    "confidence": ["confidence", "data_confidence"],
    "data_source": ["data_source", "source_of_data"],
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
