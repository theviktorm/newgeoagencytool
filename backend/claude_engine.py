"""
Momentus AI — Claude API Engine
Real Anthropic API integration with:
- Token counting (pre-flight and actual)
- Prompt caching for repeated context
- Message Batches API for bulk processing
- Source pattern recognition
- Brief generation
- Content generation
- Cost tracking
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import anthropic

from .config import settings
from .database import (
    check_cost_limit, execute, fetch_all, fetch_one,
    gen_id, record_token_usage, to_json, from_json,
)

logger = logging.getLogger("geo.claude")

# ═══════════════════════════════════════════════════════════════
# PRICING TABLE (per 1K tokens, as of 2026)
# ═══════════════════════════════════════════════════════════════
PRICING = {
    "claude-sonnet-4-20250514": {"input": 0.003, "output": 0.015, "cache_read": 0.0003, "cache_write": 0.00375},
    "claude-opus-4-20250514": {"input": 0.015, "output": 0.075, "cache_read": 0.0015, "cache_write": 0.01875},
    "claude-haiku-3-5-20241022": {"input": 0.0008, "output": 0.004, "cache_read": 0.00008, "cache_write": 0.001},
}

def estimate_cost(model: str, input_tokens: int, output_tokens: int, cached_tokens: int = 0) -> float:
    """Estimate cost in USD for a Claude API call."""
    prices = PRICING.get(model, PRICING.get("claude-sonnet-4-20250514", {}))
    input_cost = ((input_tokens - cached_tokens) / 1000) * prices.get("input", 0.003)
    cache_cost = (cached_tokens / 1000) * prices.get("cache_read", 0.0003)
    output_cost = (output_tokens / 1000) * prices.get("output", 0.015)
    return round(input_cost + cache_cost + output_cost, 6)


# ═══════════════════════════════════════════════════════════════
# PROMPT TEMPLATES (versioned, stored in DB too)
# ═══════════════════════════════════════════════════════════════

DEFAULT_PROMPTS = {
    "source_analysis": {
        "blog": """You are a GEO (Generative Engine Optimization) analyst. Your job is to analyze source pages that AI models frequently cite and identify the structural patterns that make them get cited.

Analyze the following {source_count} source pages for the topic "{topic}". These pages are the most-cited by AI models ({models}) for this topic cluster.

For each source, systematically identify:

1. **Content Structure**: Heading hierarchy (H1/H2/H3 usage), number of sections, section organization pattern
2. **Content Depth**: Approximate word count, level of detail, use of statistics and data points
3. **FAQ Patterns**: Presence of FAQ sections, number of questions, question format (natural language vs. keyword), answer depth
4. **Authority Signals**: Author credentials, medical/expert review badges, publication dates, source citations within content
5. **List & Table Usage**: Bullet lists, numbered lists, comparison tables, data tables — frequency and placement
6. **Unique Elements**: Key takeaway boxes, summary sections, infographics descriptions, step-by-step guides

After analyzing all sources, provide:
- **Cross-Source Pattern Summary**: What structural elements appear in 60%+ of cited sources
- **Citation Correlation Insights**: Which elements seem to correlate with higher citation rates
- **Content Gap Analysis**: What's missing from current sources that could be an opportunity
- **Recommended Blueprint**: The ideal content structure for this topic based on what gets cited

SOURCE DATA:
{sources}""",

        "product": """Analyze these {source_count} product/service pages that AI models cite for "{topic}".

Focus specifically on:
1. Value proposition clarity and placement
2. Feature presentation format (lists vs. paragraphs vs. comparison tables)
3. Social proof patterns (testimonials, reviews, case studies, numbers)
4. CTA structures and placement
5. Trust signals (certifications, guarantees, security badges)
6. Pricing presentation format
7. Comparison/alternative sections

SOURCE DATA:
{sources}""",

        "faq": """Analyze these {source_count} FAQ/knowledge-base pages cited by AI for "{topic}".

Focus on:
1. Question format and phrasing patterns
2. Answer depth and structure (short vs. detailed)
3. Category/section organization
4. Internal linking patterns between answers
5. Schema markup usage (FAQ schema, HowTo schema)
6. Related questions/topics sections
7. Search functionality and navigation

SOURCE DATA:
{sources}""",
    },

    "brief_generation": """Based on the following source analysis for the topic cluster "{cluster}", generate a detailed, actionable content brief.

SOURCE ANALYSIS:
{analysis}

CLUSTER DATA:
- Topic: {topic}
- Total sources analyzed: {source_count}
- Average citation rate: {avg_citation_rate}%
- Models citing this topic: {models}

Generate a brief with these exact sections:

## Target Intent
What specific user question or need this content addresses. Be precise.

## Winning Patterns
List the structural elements that correlate with AI citation, ranked by importance.

## Recommended Structure
Exact heading outline with H1, H2, H3 hierarchy. Include the specific heading text.

## Word Count
Recommended range based on top-performing sources, with justification.

## Must-Include Elements
- FAQ section specifications (number of questions, format)
- Required lists and tables
- Statistics and data points to include (with suggested sources)
- Schema markup recommendations

## Tone & Authority
Voice guidelines, authority signals to include, credential/review requirements.

## Differentiation Strategy
How to add unique value beyond what existing cited sources provide.""",

    "content_generation": {
        "blog": """Write a comprehensive, GEO-optimized blog post based on this content brief. Your goal is to create content that AI models will cite when users ask about this topic.

CONTENT BRIEF:
{brief}

REQUIREMENTS:
1. Follow the heading structure from the brief exactly
2. Include ALL must-have elements specified in the brief
3. Write in the specified tone with appropriate authority signals
4. Include an FAQ section with proper formatting (each question as a subheading, each answer as a substantive paragraph)
5. Use data, statistics, and specific numbers where the brief calls for them
6. Make each section substantive enough to be cited independently by an AI model
7. Target the specified word count range
8. Write in a way that directly answers user questions — this is what makes AI cite content
9. Include comparison tables where relevant
10. Bold key terms and important findings on first mention

Write the complete article in Markdown format.""",

        "faq": """Create a comprehensive FAQ page based on this brief. Each Q&A must be formatted for maximum AI citability.

CONTENT BRIEF:
{brief}

REQUIREMENTS:
1. Each question as an H3 heading in natural question format
2. Each answer as a substantive paragraph (50-150 words minimum)
3. Include specific data/numbers in answers where possible
4. Group related questions under H2 category headings
5. Format for FAQ schema markup compatibility
6. Include a brief intro section before the FAQs

Write in Markdown format.""",

        "landing": """Write a conversion-focused landing page based on this brief, structured for AI citation.

CONTENT BRIEF:
{brief}

Include: hero section, problem/solution, features with benefits, social proof section, comparison table, FAQ section, CTA. Write in Markdown format.""",

        "kb": """Write a knowledge base article based on this brief. This should be authoritative, reference-quality content that AI models would cite as a primary source.

CONTENT BRIEF:
{brief}

Requirements: comprehensive depth, neutral authoritative tone, extensive data, clear structure, proper citations. Write in Markdown format.""",
    },
}


# ═══════════════════════════════════════════════════════════════
# CLAUDE CLIENT WRAPPER
# ═══════════════════════════════════════════════════════════════

class ClaudeEngine:
    """
    Wraps the Anthropic Python SDK with:
    - Automatic token counting
    - Prompt caching for repeated system prompts
    - Cost tracking and limit enforcement
    - Retry logic
    - Batch processing via Message Batches API
    """

    def __init__(self, api_key: str = ""):
        self.api_key = api_key or settings.anthropic_api_key
        self._client: Optional[anthropic.Anthropic] = None
        self._async_client: Optional[anthropic.AsyncAnthropic] = None

    def _get_sync_client(self) -> anthropic.Anthropic:
        if self._client is None:
            self._client = anthropic.Anthropic(api_key=self.api_key)
        return self._client

    def _get_async_client(self) -> anthropic.AsyncAnthropic:
        if self._async_client is None:
            self._async_client = anthropic.AsyncAnthropic(api_key=self.api_key)
        return self._async_client

    # ── Token Counting (pre-flight) ──

    async def count_tokens(
        self,
        messages: List[Dict[str, Any]],
        model: Optional[str] = None,
        system: str = "",
    ) -> int:
        """
        Count tokens before sending a request.
        Uses the Anthropic Token Counting API.
        """
        client = self._get_async_client()
        model = model or settings.claude_default_model

        try:
            params = {"model": model, "messages": messages}
            if system:
                params["system"] = system

            result = await client.messages.count_tokens(**params)
            return result.input_tokens
        except Exception as e:
            logger.warning("Token counting failed: %s — estimating", e)
            # Rough estimate: ~4 chars per token
            total_chars = sum(len(str(m.get("content", ""))) for m in messages) + len(system)
            return total_chars // 4

    # ── Core Message Call ──

    async def call(
        self,
        messages: List[Dict[str, Any]],
        system: str = "",
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        project_id: str = "default",
        operation: str = "general",
        reference_id: str = "",
        use_cache: bool = False,
    ) -> Dict[str, Any]:
        """
        Make a real Claude API call with full tracking.

        Returns {
            content: str,
            model: str,
            tokens_input: int,
            tokens_output: int,
            cached_tokens: int,
            cost: float,
            stop_reason: str,
        }
        """
        # Enforce cost limits
        cost_check = await check_cost_limit(project_id)
        if not cost_check["allowed"]:
            raise RuntimeError(
                f"Cost limit exceeded. Daily: ${cost_check['daily_cost']:.2f}/{cost_check['daily_limit']:.2f}, "
                f"Monthly: ${cost_check['monthly_cost']:.2f}/{cost_check['monthly_limit']:.2f}"
            )

        client = self._get_async_client()
        model = model or settings.claude_default_model
        temperature = temperature if temperature is not None else settings.claude_default_temperature
        max_tokens = max_tokens or settings.claude_max_tokens

        # Build request params
        params = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }

        # Apply prompt caching for system prompt if requested
        if system:
            if use_cache:
                params["system"] = [
                    {
                        "type": "text",
                        "text": system,
                        "cache_control": {"type": "ephemeral"},
                    }
                ]
            else:
                params["system"] = system

        try:
            response = await client.messages.create(**params)

            # Extract usage
            tokens_input = response.usage.input_tokens
            tokens_output = response.usage.output_tokens
            cached_tokens = getattr(response.usage, "cache_read_input_tokens", 0) or 0

            # Calculate cost
            cost = estimate_cost(model, tokens_input, tokens_output, cached_tokens)

            # Record usage
            await record_token_usage(
                project_id=project_id,
                model=model,
                tokens_input=tokens_input,
                tokens_output=tokens_output,
                cached_tokens=cached_tokens,
                cost=cost,
                operation=operation,
                reference_id=reference_id,
            )

            # Extract text content
            content = ""
            for block in response.content:
                if block.type == "text":
                    content += block.text

            return {
                "content": content,
                "model": response.model,
                "tokens_input": tokens_input,
                "tokens_output": tokens_output,
                "cached_tokens": cached_tokens,
                "cost": cost,
                "stop_reason": response.stop_reason,
            }

        except anthropic.RateLimitError as e:
            logger.error("Claude rate limit hit: %s", e)
            raise RuntimeError(f"Claude API rate limit exceeded. Please wait and retry. Details: {e}")
        except anthropic.AuthenticationError as e:
            raise RuntimeError(f"Claude API key invalid: {e}")
        except anthropic.BadRequestError as e:
            raise RuntimeError(f"Claude API bad request: {e}")
        except anthropic.APIError as e:
            raise RuntimeError(f"Claude API error: {e}")

    # ── Source Analysis ──

    async def analyze_sources(
        self,
        cluster_id: str,
        prompt_type: str = "blog",
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        use_cache: bool = True,
        project_id: str = "default",
    ) -> Dict[str, Any]:
        """
        Run source pattern analysis on a cluster's scraped sources.
        Uses the source_analysis prompt template.
        """
        # Get cluster info
        cluster = await fetch_one("SELECT * FROM clusters WHERE id = ?", (cluster_id,))
        if not cluster:
            raise ValueError(f"Cluster {cluster_id} not found")

        # Get scraped sources for this cluster
        sources = await fetch_all(
            "SELECT s.url, s.title, s.total_citation_count, s.max_citation_rate, "
            "sc.headings, sc.body_text, sc.lists, sc.faqs, sc.word_count, sc.meta_description "
            "FROM sources s "
            "JOIN cluster_sources cs ON s.id = cs.source_id "
            "LEFT JOIN scraped_content sc ON s.id = sc.source_id "
            "WHERE cs.cluster_id = ? AND s.scrape_status = 'scraped' "
            "ORDER BY s.total_citation_count DESC "
            "LIMIT 10",
            (cluster_id,),
        )

        if not sources:
            raise ValueError(f"No scraped sources found for cluster {cluster_id}")

        # Format sources for the prompt
        sources_text = self._format_sources_for_prompt(sources)

        # Get prompt template (custom or default)
        custom_prompt = await fetch_one(
            "SELECT template FROM prompt_templates WHERE project_id = ? "
            "AND category = 'source_analysis' AND sub_type = ? AND is_active = 1 "
            "ORDER BY version DESC LIMIT 1",
            (project_id, prompt_type),
        )

        template = (
            custom_prompt["template"] if custom_prompt
            else DEFAULT_PROMPTS["source_analysis"].get(prompt_type, DEFAULT_PROMPTS["source_analysis"]["blog"])
        )

        cluster_topics = from_json(cluster.get("topics", "[]"), [])
        cluster_models = set()
        for s in sources:
            # model_sources aren't directly on scraped_content join
            pass

        model_sources_list = await fetch_all(
            "SELECT DISTINCT pr.model_source FROM peec_records pr "
            "JOIN cluster_records cr ON pr.id = cr.peec_record_id "
            "WHERE cr.cluster_id = ?",
            (cluster_id,),
        )
        models_str = ", ".join(r["model_source"] for r in model_sources_list) or "Multiple AI models"

        # Fill template
        prompt_text = template.format(
            topic=cluster["name"],
            source_count=len(sources),
            sources=sources_text,
            models=models_str,
        )

        # System prompt for analysis context (cacheable)
        system_prompt = (
            "You are an expert GEO (Generative Engine Optimization) analyst. "
            "You analyze web content that AI models cite in their responses to users. "
            "Your analysis identifies structural, content, and authority patterns "
            "that correlate with higher AI citation rates. "
            "Be specific, data-driven, and actionable in your analysis."
        )

        analysis_id = gen_id("an-")

        # Make the call
        result = await self.call(
            messages=[{"role": "user", "content": prompt_text}],
            system=system_prompt,
            model=model,
            temperature=temperature or 0.3,  # lower temp for analysis
            max_tokens=max_tokens,
            project_id=project_id,
            operation="analysis",
            reference_id=analysis_id,
            use_cache=use_cache,
        )

        # Store analysis
        await execute(
            "INSERT INTO analyses (id, cluster_id, prompt_template, prompt_type, model, "
            "temperature, result, tokens_input, tokens_output, cost, cached_tokens) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                analysis_id, cluster_id, prompt_text, prompt_type,
                result["model"], temperature or 0.3, result["content"],
                result["tokens_input"], result["tokens_output"],
                result["cost"], result["cached_tokens"],
            ),
        )

        return {
            "analysis_id": analysis_id,
            "cluster_id": cluster_id,
            "result": result["content"],
            "model": result["model"],
            "tokens_input": result["tokens_input"],
            "tokens_output": result["tokens_output"],
            "cached_tokens": result["cached_tokens"],
            "cost": result["cost"],
        }

    # ── Brief Generation ──

    async def generate_brief(
        self,
        cluster_id: str,
        analysis_id: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        project_id: str = "default",
    ) -> Dict[str, Any]:
        """Generate a content brief from source analysis results."""
        cluster = await fetch_one("SELECT * FROM clusters WHERE id = ?", (cluster_id,))
        if not cluster:
            raise ValueError(f"Cluster {cluster_id} not found")

        # Get analysis (latest if not specified)
        if analysis_id:
            analysis = await fetch_one("SELECT * FROM analyses WHERE id = ?", (analysis_id,))
        else:
            analysis = await fetch_one(
                "SELECT * FROM analyses WHERE cluster_id = ? ORDER BY created_at DESC LIMIT 1",
                (cluster_id,),
            )

        if not analysis:
            raise ValueError(f"No analysis found for cluster {cluster_id}. Run analysis first.")

        # Get model sources
        model_sources = await fetch_all(
            "SELECT DISTINCT pr.model_source FROM peec_records pr "
            "JOIN cluster_records cr ON pr.id = cr.peec_record_id "
            "WHERE cr.cluster_id = ?",
            (cluster_id,),
        )
        models_str = ", ".join(r["model_source"] for r in model_sources) or "Multiple"

        # Get custom or default template
        custom = await fetch_one(
            "SELECT template FROM prompt_templates WHERE project_id = ? "
            "AND category = 'brief_generation' AND is_active = 1 "
            "ORDER BY version DESC LIMIT 1",
            (project_id,),
        )

        template = custom["template"] if custom else DEFAULT_PROMPTS["brief_generation"]

        prompt_text = template.format(
            cluster=cluster["name"],
            topic=cluster["name"],
            analysis=analysis["result"],
            source_count=cluster.get("url_count", 0),
            avg_citation_rate=f"{(cluster.get('avg_citation_rate', 0) * 100):.1f}",
            models=models_str,
        )

        brief_id = gen_id("br-")

        result = await self.call(
            messages=[{"role": "user", "content": prompt_text}],
            system="You are a GEO content strategist. Generate detailed, actionable content briefs based on source analysis data.",
            model=model,
            temperature=temperature or 0.5,
            project_id=project_id,
            operation="brief",
            reference_id=brief_id,
            use_cache=True,
        )

        await execute(
            "INSERT INTO briefs (id, cluster_id, analysis_id, content, model, "
            "tokens_input, tokens_output, cost) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                brief_id, cluster_id, analysis["id"], result["content"],
                result["model"], result["tokens_input"], result["tokens_output"], result["cost"],
            ),
        )

        return {
            "brief_id": brief_id,
            "cluster_id": cluster_id,
            "content": result["content"],
            "model": result["model"],
            "tokens_input": result["tokens_input"],
            "tokens_output": result["tokens_output"],
            "cost": result["cost"],
        }

    # ── Content Generation ──

    async def generate_content(
        self,
        cluster_id: str,
        brief_id: Optional[str] = None,
        template_type: str = "blog",
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        project_id: str = "default",
    ) -> Dict[str, Any]:
        """Generate content draft from a brief."""
        cluster = await fetch_one("SELECT * FROM clusters WHERE id = ?", (cluster_id,))
        if not cluster:
            raise ValueError(f"Cluster {cluster_id} not found")

        if brief_id:
            brief = await fetch_one("SELECT * FROM briefs WHERE id = ?", (brief_id,))
        else:
            brief = await fetch_one(
                "SELECT * FROM briefs WHERE cluster_id = ? ORDER BY created_at DESC LIMIT 1",
                (cluster_id,),
            )

        if not brief:
            raise ValueError(f"No brief found for cluster {cluster_id}. Generate a brief first.")

        # Get template
        custom = await fetch_one(
            "SELECT template FROM prompt_templates WHERE project_id = ? "
            "AND category = 'content_generation' AND sub_type = ? AND is_active = 1 "
            "ORDER BY version DESC LIMIT 1",
            (project_id, template_type),
        )

        template = (
            custom["template"] if custom
            else DEFAULT_PROMPTS["content_generation"].get(template_type, DEFAULT_PROMPTS["content_generation"]["blog"])
        )

        prompt_text = template.format(brief=brief["content"])

        # Determine version number
        existing = await fetch_one(
            "SELECT MAX(version) as max_v FROM drafts WHERE cluster_id = ? AND template_type = ?",
            (cluster_id, template_type),
        )
        version = (existing["max_v"] or 0) + 1 if existing else 1

        # Find parent draft for version chain
        parent = await fetch_one(
            "SELECT id FROM drafts WHERE cluster_id = ? AND template_type = ? "
            "ORDER BY version DESC LIMIT 1",
            (cluster_id, template_type),
        )

        draft_id = gen_id("dr-")

        # Build source snapshot for traceability
        source_snapshot = {
            "cluster_id": cluster_id,
            "cluster_name": cluster["name"],
            "brief_id": brief["id"],
            "analysis_id": brief.get("analysis_id", ""),
            "template_type": template_type,
            "model": model or settings.claude_default_model,
            "temperature": temperature or settings.claude_default_temperature,
            "generated_at": datetime.utcnow().isoformat(),
        }

        result = await self.call(
            messages=[{"role": "user", "content": prompt_text}],
            system="You are an expert content writer specializing in GEO-optimized content. Write content that AI models will cite in their responses.",
            model=model,
            temperature=temperature or 0.7,
            max_tokens=max_tokens or 8192,  # higher for content gen
            project_id=project_id,
            operation="generation",
            reference_id=draft_id,
            use_cache=True,
        )

        word_count = len(result["content"].split())

        await execute(
            "INSERT INTO drafts (id, cluster_id, brief_id, version, template_type, content, "
            "word_count, model, temperature, tokens_input, tokens_output, cost, "
            "parent_draft_id, source_snapshot, prompt_snapshot) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                draft_id, cluster_id, brief["id"], version, template_type,
                result["content"], word_count, result["model"],
                temperature or settings.claude_default_temperature,
                result["tokens_input"], result["tokens_output"], result["cost"],
                parent["id"] if parent else None,
                to_json(source_snapshot), prompt_text,
            ),
        )

        return {
            "draft_id": draft_id,
            "cluster_id": cluster_id,
            "version": version,
            "content": result["content"],
            "word_count": word_count,
            "model": result["model"],
            "tokens_input": result["tokens_input"],
            "tokens_output": result["tokens_output"],
            "cost": result["cost"],
        }

    # ── Batch Processing (Message Batches API) ──

    async def create_batch(
        self,
        requests: List[Dict[str, Any]],
        project_id: str = "default",
    ) -> Dict[str, Any]:
        """
        Submit multiple requests as a batch via the Message Batches API.
        Each request: {custom_id, messages, system, model, temperature, max_tokens}
        Returns batch job info for polling.
        """
        client = self._get_async_client()

        batch_requests = []
        for req in requests:
            params = {
                "model": req.get("model", settings.claude_default_model),
                "max_tokens": req.get("max_tokens", settings.claude_max_tokens),
                "messages": req["messages"],
            }
            if req.get("system"):
                params["system"] = req["system"]
            if req.get("temperature") is not None:
                params["temperature"] = req["temperature"]

            batch_requests.append({
                "custom_id": req.get("custom_id", gen_id("req-")),
                "params": params,
            })

        try:
            batch = await client.messages.batches.create(requests=batch_requests)

            job_id = gen_id("bj-")
            await execute(
                "INSERT INTO batch_jobs (id, project_id, job_type, status, total_items, "
                "anthropic_batch_id, started_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    job_id, project_id, "claude_batch", "running",
                    len(batch_requests), batch.id, datetime.utcnow().isoformat(),
                ),
            )

            return {
                "job_id": job_id,
                "batch_id": batch.id,
                "status": batch.processing_status,
                "total_requests": len(batch_requests),
            }

        except anthropic.APIError as e:
            raise RuntimeError(f"Batch creation failed: {e}")

    async def poll_batch(self, batch_id: str) -> Dict[str, Any]:
        """Poll a batch job for completion status."""
        client = self._get_async_client()

        try:
            batch = await client.messages.batches.retrieve(batch_id)

            result = {
                "batch_id": batch.id,
                "status": batch.processing_status,
                "counts": {
                    "succeeded": getattr(batch.request_counts, "succeeded", 0),
                    "errored": getattr(batch.request_counts, "errored", 0),
                    "canceled": getattr(batch.request_counts, "canceled", 0),
                    "expired": getattr(batch.request_counts, "expired", 0),
                    "processing": getattr(batch.request_counts, "processing", 0),
                },
            }

            # Update job record
            if batch.processing_status == "ended":
                await execute(
                    "UPDATE batch_jobs SET status = 'completed', completed_items = ?, "
                    "failed_items = ?, completed_at = ? WHERE anthropic_batch_id = ?",
                    (
                        result["counts"]["succeeded"],
                        result["counts"]["errored"],
                        datetime.utcnow().isoformat(),
                        batch_id,
                    ),
                )

            return result

        except anthropic.APIError as e:
            raise RuntimeError(f"Batch polling failed: {e}")

    async def get_batch_results(self, batch_id: str) -> List[Dict[str, Any]]:
        """Retrieve results from a completed batch."""
        client = self._get_async_client()

        results = []
        try:
            async for result in client.messages.batches.results(batch_id):
                entry = {
                    "custom_id": result.custom_id,
                    "type": result.result.type,
                }
                if result.result.type == "succeeded":
                    msg = result.result.message
                    content = ""
                    for block in msg.content:
                        if block.type == "text":
                            content += block.text
                    entry["content"] = content
                    entry["tokens_input"] = msg.usage.input_tokens
                    entry["tokens_output"] = msg.usage.output_tokens
                    entry["model"] = msg.model
                elif result.result.type == "errored":
                    entry["error"] = str(result.result.error)

                results.append(entry)

        except anthropic.APIError as e:
            raise RuntimeError(f"Batch results retrieval failed: {e}")

        return results

    # ── Helpers ──

    def _format_sources_for_prompt(self, sources: List[Dict]) -> str:
        """Format scraped sources into a readable prompt section."""
        parts = []
        for i, src in enumerate(sources, 1):
            headings = from_json(src.get("headings", "[]"), [])
            faqs = from_json(src.get("faqs", "[]"), [])
            body = src.get("body_text", "")

            heading_text = " → ".join(h.get("text", "") for h in headings[:10]) if headings else "No headings extracted"

            # Truncate body to avoid token overflow
            body_preview = body[:1500] + "..." if len(body) > 1500 else body

            parts.append(
                f"--- SOURCE {i} ---\n"
                f"URL: {src.get('url', 'N/A')}\n"
                f"Title: {src.get('title', 'N/A')}\n"
                f"Citations: {src.get('total_citation_count', 0)} | "
                f"Citation Rate: {(src.get('max_citation_rate', 0) * 100):.1f}%\n"
                f"Word Count: {src.get('scraped_word_count', src.get('word_count', 0))}\n"
                f"Headings: {heading_text}\n"
                f"FAQs Found: {len(faqs)}\n"
                f"Content Preview:\n{body_preview}\n"
            )

        return "\n".join(parts)
