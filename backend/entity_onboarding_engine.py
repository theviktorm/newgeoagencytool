"""
MOMENTUS AI — ENTITY ONBOARDING ENGINE
Crawl-driven profile extraction, consistency scoring, manual overrides.
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from .database import execute, fetch_all, fetch_one, from_json, gen_id, to_json
from .scraper import scrape_business_profile

logger = logging.getLogger("geo.entity_onboarding")

# ═══════════════════════════════════════════════════════════════
# ENTITY PROFILE EXTRACTION
# ═══════════════════════════════════════════════════════════════

async def crawl_entity_profile(entity_id: str, name: str, website: str = "") -> Dict:
    """Crawl entity profile from web sources."""
    profile = {
        "name": name,
        "website": website,
        "phone": "",
        "email": "",
        "address": "",
        "hours": "",
        "category": "",
        "description": "",
        "social_profiles": {},
        "sources": [],
    }
    
    # Crawl website if provided
    if website:
        try:
            web_data = await scrape_business_profile(website)
            if web_data:
                profile.update({
                    "phone": web_data.get("phone", ""),
                    "email": web_data.get("email", ""),
                    "address": web_data.get("address", ""),
                    "hours": web_data.get("hours", ""),
                    "category": web_data.get("category", ""),
                    "description": web_data.get("description", ""),
                })
                profile["sources"].append("website")
        except Exception as e:
            logger.warning(f"Website crawl failed for {name}: {e}")
    
    # Crawl Google Business Profile
    try:
        gbp_data = await crawl_google_business_profile(name)
        if gbp_data:
            profile.update({
                "phone": gbp_data.get("phone", profile["phone"]),
                "address": gbp_data.get("address", profile["address"]),
                "hours": gbp_data.get("hours", profile["hours"]),
                "category": gbp_data.get("category", profile["category"]),
            })
            profile["social_profiles"]["google"] = gbp_data.get("url", "")
            profile["sources"].append("google_business")
    except Exception as e:
        logger.warning(f"GBP crawl failed for {name}: {e}")
    
    # Crawl social profiles
    try:
        social = await crawl_social_profiles(name)
        if social:
            profile["social_profiles"].update(social)
            profile["sources"].extend(list(social.keys()))
    except Exception as e:
        logger.warning(f"Social crawl failed for {name}: {e}")
    
    return profile


async def crawl_google_business_profile(business_name: str) -> Dict:
    """Crawl Google Business Profile data."""
    # Simplified mock implementation
    return {
        "name": business_name,
        "phone": "",
        "address": "",
        "hours": "",
        "category": "",
        "url": "",
    }


async def crawl_social_profiles(business_name: str) -> Dict:
    """Crawl social media profiles."""
    # Simplified mock implementation
    return {
        "facebook": "",
        "instagram": "",
        "linkedin": "",
        "twitter": "",
        "youtube": "",
    }


# ═══════════════════════════════════════════════════════════════
# CONSISTENCY SCORING
# ═══════════════════════════════════════════════════════════════

def calculate_consistency_score(
    extracted: Dict, manual_overrides: Dict = None
) -> float:
    """Calculate entity consistency score (0-100)."""
    overrides = manual_overrides or {}
    
    # Merge data
    merged = {**extracted, **overrides}
    
    # Score components
    scores = {
        "completeness": 0,  # 0-30: how many fields are filled
        "format_quality": 0,  # 0-30: field format correctness
        "source_agreement": 0,  # 0-20: consistency across sources
        "recency": 0,  # 0-20: data freshness
    }
    
    # Completeness (30 points)
    required_fields = ["name", "phone", "address", "hours"]
    filled = sum(1 for f in required_fields if merged.get(f, "").strip())
    scores["completeness"] = (filled / len(required_fields)) * 30
    
    # Format quality (30 points)
    format_checks = 0
    format_total = 0
    if merged.get("phone"):
        format_total += 1
        if is_valid_phone(merged["phone"]):
            format_checks += 1
    if merged.get("email"):
        format_total += 1
        if is_valid_email(merged["email"]):
            format_checks += 1
    if merged.get("website"):
        format_total += 1
        if is_valid_url(merged["website"]):
            format_checks += 1
    if format_total > 0:
        scores["format_quality"] = (format_checks / format_total) * 30
    
    # Source agreement (20 points)
    sources = extracted.get("sources", [])
    if len(sources) >= 2:
        scores["source_agreement"] = 20  # Multiple sources = high confidence
    elif len(sources) == 1:
        scores["source_agreement"] = 10
    
    # Recency (20 points)
    # Assume recent if crawled today
    scores["recency"] = 20
    
    total = sum(scores.values())
    return min(100, total)


def is_valid_phone(phone: str) -> bool:
    """Validate phone number format."""
    digits = "".join(c for c in phone if c.isdigit())
    return 10 <= len(digits) <= 15


def is_valid_email(email: str) -> bool:
    """Validate email format."""
    return "@" in email and "." in email and len(email) > 5


def is_valid_url(url: str) -> bool:
    """Validate URL format."""
    return url.startswith(("http://", "https://", "www."))


# ═══════════════════════════════════════════════════════════════
# ENTITY ONBOARDING WORKFLOW
# ═══════════════════════════════════════════════════════════════

async def start_entity_onboarding(
    project_id: str, entity_type: str, name: str, website: str = ""
) -> str:
    """Start entity onboarding process."""
    entity_id = gen_id("ent-")
    
    # Create record
    await execute(
        """INSERT INTO entity_onboarding
           (id, project_id, entity_type, name, crawl_status)
           VALUES (?, ?, ?, ?, ?)""",
        (entity_id, project_id, entity_type, name, "crawling"),
    )
    
    # Crawl profile
    try:
        profile = await crawl_entity_profile(entity_id, name, website)
        score = calculate_consistency_score(profile)
        
        await execute(
            """UPDATE entity_onboarding
               SET extracted_data = ?, consistency_score = ?, crawl_status = ?
               WHERE id = ?""",
            (to_json(profile), score, "extracted", entity_id),
        )
    except Exception as e:
        logger.error(f"Onboarding failed for {name}: {e}")
        await execute(
            "UPDATE entity_onboarding SET crawl_status = ? WHERE id = ?",
            ("failed", entity_id),
        )
    
    return entity_id


async def apply_manual_overrides(
    entity_id: str, overrides: Dict
) -> None:
    """Apply manual overrides to entity data."""
    entity = await fetch_one(
        "SELECT * FROM entity_onboarding WHERE id = ?", (entity_id,)
    )
    if not entity:
        return
    
    # Merge overrides
    current_overrides = from_json(entity.get("manual_overrides", "{}"), {})
    current_overrides.update(overrides)
    
    # Recalculate score
    extracted = from_json(entity.get("extracted_data", "{}"), {})
    new_score = calculate_consistency_score(extracted, current_overrides)
    
    await execute(
        """UPDATE entity_onboarding
           SET manual_overrides = ?, consistency_score = ?, updated_at = datetime('now')
           WHERE id = ?""",
        (to_json(current_overrides), new_score, entity_id),
    )


async def validate_entity(entity_id: str) -> Dict:
    """Validate entity data completeness."""
    entity = await fetch_one(
        "SELECT * FROM entity_onboarding WHERE id = ?", (entity_id,)
    )
    if not entity:
        return {"valid": False, "errors": ["Entity not found"]}
    
    extracted = from_json(entity.get("extracted_data", "{}"), {})
    overrides = from_json(entity.get("manual_overrides", "{}"), {})
    merged = {**extracted, **overrides}
    
    errors = []
    warnings = []
    
    # Required fields
    if not merged.get("name", "").strip():
        errors.append("Business name is required")
    if not merged.get("phone", "").strip():
        warnings.append("Phone number is missing")
    if not merged.get("address", "").strip():
        warnings.append("Address is missing")
    if not merged.get("hours", "").strip():
        warnings.append("Business hours are missing")
    
    # Format validation
    if merged.get("phone") and not is_valid_phone(merged["phone"]):
        errors.append("Invalid phone format")
    if merged.get("email") and not is_valid_email(merged["email"]):
        errors.append("Invalid email format")
    if merged.get("website") and not is_valid_url(merged["website"]):
        errors.append("Invalid website URL")
    
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "entity_id": entity_id,
    }


async def finalize_entity(entity_id: str) -> None:
    """Mark entity as validated and ready for audit."""
    await execute(
        """UPDATE entity_onboarding
           SET crawl_status = ?, updated_at = datetime('now')
           WHERE id = ?""",
        ("validated", entity_id),
    )


# ═══════════════════════════════════════════════════════════════
# BATCH ONBOARDING
# ═══════════════════════════════════════════════════════════════

async def onboard_entities_batch(
    project_id: str, entities: List[Dict]
) -> Dict:
    """Onboard multiple entities in parallel."""
    tasks = []
    for ent in entities:
        task = start_entity_onboarding(
            project_id,
            ent.get("type", "brand"),
            ent.get("name", ""),
            ent.get("website", ""),
        )
        tasks.append(task)
    
    entity_ids = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Filter out exceptions
    valid_ids = [id for id in entity_ids if isinstance(id, str)]
    
    return {
        "total": len(entities),
        "created": len(valid_ids),
        "failed": len(entity_ids) - len(valid_ids),
        "entity_ids": valid_ids,
    }


async def list_project_entities(project_id: str) -> List[Dict]:
    """List all entities for a project."""
    entities = await fetch_all(
        "SELECT * FROM entity_onboarding WHERE project_id = ? ORDER BY created_at",
        (project_id,),
    )
    
    for e in entities:
        e["extracted_data"] = from_json(e.get("extracted_data", "{}"), {})
        e["manual_overrides"] = from_json(e.get("manual_overrides", "{}"), {})
    
    return entities


async def get_entity_details(entity_id: str) -> Optional[Dict]:
    """Get full entity details."""
    entity = await fetch_one(
        "SELECT * FROM entity_onboarding WHERE id = ?", (entity_id,)
    )
    if entity:
        entity["extracted_data"] = from_json(entity.get("extracted_data", "{}"), {})
        entity["manual_overrides"] = from_json(entity.get("manual_overrides", "{}"), {})
    return entity


# ═══════════════════════════════════════════════════════════════
# ENTITY CONSISTENCY REPORT
# ═══════════════════════════════════════════════════════════════

async def generate_entity_consistency_report(project_id: str) -> Dict:
    """Generate consistency report for all project entities."""
    entities = await list_project_entities(project_id)
    
    if not entities:
        return {
            "total": 0,
            "average_score": 0,
            "by_status": {},
            "entities": [],
        }
    
    scores = [e.get("consistency_score", 0) for e in entities]
    avg_score = sum(scores) / len(scores) if scores else 0
    
    by_status = {}
    for e in entities:
        status = e.get("crawl_status", "unknown")
        by_status[status] = by_status.get(status, 0) + 1
    
    return {
        "total": len(entities),
        "average_score": round(avg_score, 1),
        "score_distribution": {
            "excellent": len([s for s in scores if s >= 90]),
            "good": len([s for s in scores if 70 <= s < 90]),
            "fair": len([s for s in scores if 50 <= s < 70]),
            "poor": len([s for s in scores if s < 50]),
        },
        "by_status": by_status,
        "entities": [
            {
                "id": e["id"],
                "name": e["name"],
                "type": e["entity_type"],
                "score": e.get("consistency_score", 0),
                "status": e.get("crawl_status", "unknown"),
            }
            for e in entities
        ],
    }
