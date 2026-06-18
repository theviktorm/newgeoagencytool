"""
MOMENTUS AI — SPECIALIZED WORKFLOWS
Local, GBP, Review, Authority, Off-Site, Reddit, YouTube, PR workflows.
═══════════════════════════════════════════════════════════════
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

from .database import execute, fetch_all, fetch_one, from_json, gen_id, to_json
from .action_engine import execute, gen_id, to_json, ACTION_TYPES

logger = logging.getLogger("geo.workflows")

# ═══════════════════════════════════════════════════════════════
# LOCAL SEO WORKFLOW
# ═══════════════════════════════════════════════════════════════

async def create_local_seo_action(
    project_id: str, entity_id: str, action_type: str, details: Dict
) -> str:
    """Create a local SEO action."""
    action_id = gen_id("act-")
    await execute(
        "INSERT INTO geo_actions (id, workspace_id, action_type, title, description, payload) VALUES (?, ?, ?, ?, ?, ?)",
        (action_id, project_id, f"local_seo_{action_type}", f"Local SEO: {action_type.replace('_', ' ').title()}", details.get("description", ""), to_json(details))
    )
    return action_id


LOCAL_SEO_TEMPLATES = {
    "nap_consistency": {
        "title": "Fix NAP Consistency",
        "description": "Ensure Name, Address, Phone are consistent across all sources",
        "checklist": [
            "Verify business name is identical everywhere",
            "Confirm address format matches across listings",
            "Check phone number consistency",
            "Update any inconsistencies",
        ],
    },
    "local_keywords": {
        "title": "Add Local Keywords",
        "description": "Incorporate location keywords into business description",
        "checklist": [
            "Identify primary service area",
            "Add city/region names to description",
            "Include local landmarks if relevant",
            "Update all listings",
        ],
    },
    "local_citations": {
        "title": "Build Local Citations",
        "description": "Create business listings on local directories",
        "checklist": [
            "Yelp business listing",
            "Local chamber of commerce",
            "Industry-specific directories",
            "Local business directories",
        ],
    },
}


# ═══════════════════════════════════════════════════════════════
# GOOGLE BUSINESS PROFILE WORKFLOW
# ═══════════════════════════════════════════════════════════════

async def create_gbp_action(
    project_id: str, entity_id: str, action_type: str, details: Dict
) -> str:
    """Create a Google Business Profile action."""
    action_id = gen_id("act-")
    await execute(
        "INSERT INTO geo_actions (id, workspace_id, action_type, title, description, payload) VALUES (?, ?, ?, ?, ?, ?)",
        (action_id, project_id, f"gbp_{action_type}", f"Google Business Profile: {action_type.replace('_', ' ').title()}", details.get("description", ""), to_json(details))
    )
    return action_id


GBP_TEMPLATES = {
    "claim_gbp": {
        "title": "Claim Google Business Profile",
        "description": "Claim or verify business on Google Business Profile",
        "checklist": [
            "Go to Google Business Profile",
            "Search for your business",
            "Verify ownership",
            "Complete business information",
            "Add photos and hours",
        ],
    },
    "optimize_gbp": {
        "title": "Optimize GBP Profile",
        "description": "Improve GBP profile completeness and quality",
        "checklist": [
            "Add high-quality business photos (10+)",
            "Write compelling business description",
            "Add service areas if applicable",
            "Enable customer reviews",
            "Post regular updates",
        ],
    },
    "gbp_posts": {
        "title": "Create GBP Posts",
        "description": "Post updates, offers, and events to GBP",
        "checklist": [
            "Create weekly posts",
            "Highlight special offers",
            "Announce events",
            "Share customer testimonials",
        ],
    },
}


# ═══════════════════════════════════════════════════════════════
# REVIEW MANAGEMENT WORKFLOW
# ═══════════════════════════════════════════════════════════════

async def create_review_action(
    project_id: str, entity_id: str, action_type: str, details: Dict
) -> str:
    """Create a review management action."""
    action_id = gen_id("act-")
    await execute(
        "INSERT INTO geo_actions (id, workspace_id, action_type, title, description, payload) VALUES (?, ?, ?, ?, ?, ?)",
        (action_id, project_id, f"review_{action_type}", f"Review Management: {action_type.replace('_', ' ').title()}", details.get("description", ""), to_json(details))
    )
    return action_id


REVIEW_TEMPLATES = {
    "monitor_reviews": {
        "title": "Monitor Customer Reviews",
        "description": "Track and respond to customer reviews",
        "checklist": [
            "Check Google Business Profile reviews daily",
            "Monitor Yelp reviews",
            "Track industry-specific review sites",
            "Set up review alerts",
        ],
    },
    "respond_to_reviews": {
        "title": "Respond to Reviews",
        "description": "Reply to customer reviews professionally",
        "checklist": [
            "Respond to negative reviews within 24 hours",
            "Thank customers for positive reviews",
            "Address specific concerns",
            "Offer solutions when appropriate",
        ],
    },
    "request_reviews": {
        "title": "Request Customer Reviews",
        "description": "Encourage customers to leave reviews",
        "checklist": [
            "Send review request emails",
            "Add review links to website",
            "Include in post-purchase communications",
            "Incentivize (legally) if appropriate",
        ],
    },
}


# ═══════════════════════════════════════════════════════════════
# AUTHORITY & BACKLINK WORKFLOW
# ═══════════════════════════════════════════════════════════════

async def create_authority_action(
    project_id: str, entity_id: str, action_type: str, details: Dict
) -> str:
    """Create an authority-building action."""
    action_id = gen_id("act-")
    await execute(
        "INSERT INTO geo_actions (id, workspace_id, action_type, title, description, payload) VALUES (?, ?, ?, ?, ?, ?)",
        (action_id, project_id, f"authority_{action_type}", f"Authority Building: {action_type.replace('_', ' ').title()}", details.get("description", ""), to_json(details))
    )
    return action_id


AUTHORITY_TEMPLATES = {
    "build_backlinks": {
        "title": "Build High-Quality Backlinks",
        "description": "Acquire links from authoritative domains",
        "checklist": [
            "Identify relevant industry publications",
            "Reach out to local media",
            "Get listed in industry directories",
            "Create linkable content",
        ],
    },
    "improve_domain_authority": {
        "title": "Improve Domain Authority",
        "description": "Increase overall domain strength",
        "checklist": [
            "Create high-quality content",
            "Build internal linking structure",
            "Improve site speed",
            "Fix technical SEO issues",
        ],
    },
    "social_signals": {
        "title": "Build Social Signals",
        "description": "Increase social media presence and engagement",
        "checklist": [
            "Optimize all social profiles",
            "Post consistently",
            "Engage with followers",
            "Share content across platforms",
        ],
    },
}


# ═══════════════════════════════════════════════════════════════
# OFF-SITE AUTHORITY WORKFLOW
# ═══════════════════════════════════════════════════════════════

async def create_offsite_action(
    project_id: str, entity_id: str, action_type: str, details: Dict
) -> str:
    """Create an off-site authority action."""
    action_id = gen_id("act-")
    await execute(
        "INSERT INTO geo_actions (id, workspace_id, action_type, title, description, payload) VALUES (?, ?, ?, ?, ?, ?)",
        (action_id, project_id, f"offsite_{action_type}", f"Off-Site Authority: {action_type.replace('_', ' ').title()}", details.get("description", ""), to_json(details))
    )
    return action_id


OFFSITE_TEMPLATES = {
    "press_releases": {
        "title": "Distribute Press Releases",
        "description": "Publish press releases on news distribution networks",
        "checklist": [
            "Write newsworthy press release",
            "Distribute to PR networks",
            "Target local news outlets",
            "Monitor coverage",
        ],
    },
    "guest_posts": {
        "title": "Create Guest Posts",
        "description": "Write articles for industry and local publications",
        "checklist": [
            "Identify target publications",
            "Pitch article ideas",
            "Write high-quality content",
            "Include relevant links",
        ],
    },
    "industry_partnerships": {
        "title": "Build Industry Partnerships",
        "description": "Establish relationships with complementary businesses",
        "checklist": [
            "Identify partnership opportunities",
            "Reach out to potential partners",
            "Create co-marketing opportunities",
            "Cross-promote on websites",
        ],
    },
}


# ═══════════════════════════════════════════════════════════════
# REDDIT WORKFLOW
# ═══════════════════════════════════════════════════════════════

async def create_reddit_action(
    project_id: str, entity_id: str, action_type: str, details: Dict
) -> str:
    """Create a Reddit engagement action."""
    action_id = gen_id("act-")
    await execute(
        "INSERT INTO geo_actions (id, workspace_id, action_type, title, description, payload) VALUES (?, ?, ?, ?, ?, ?)",
        (action_id, project_id, f"reddit_{action_type}", f"Reddit: {action_type.replace('_', ' ').title()}", details.get("description", ""), to_json(details))
    )
    return action_id


REDDIT_TEMPLATES = {
    "subreddit_engagement": {
        "title": "Engage in Relevant Subreddits",
        "description": "Build authority through Reddit community participation",
        "checklist": [
            "Identify relevant subreddits",
            "Create authentic account",
            "Participate in discussions",
            "Provide value without self-promotion",
        ],
    },
    "ama_sessions": {
        "title": "Host AMA (Ask Me Anything)",
        "description": "Host AMA session to build authority and engagement",
        "checklist": [
            "Prepare for Q&A",
            "Coordinate with subreddit moderators",
            "Promote AMA session",
            "Respond to all questions",
        ],
    },
}


# ═══════════════════════════════════════════════════════════════
# YOUTUBE WORKFLOW
# ═══════════════════════════════════════════════════════════════

async def create_youtube_action(
    project_id: str, entity_id: str, action_type: str, details: Dict
) -> str:
    """Create a YouTube engagement action."""
    action_id = gen_id("act-")
    await execute(
        "INSERT INTO geo_actions (id, workspace_id, action_type, title, description, payload) VALUES (?, ?, ?, ?, ?, ?)",
        (action_id, project_id, f"youtube_{action_type}", f"YouTube: {action_type.replace('_', ' ').title()}", details.get("description", ""), to_json(details))
    )
    return action_id


YOUTUBE_TEMPLATES = {
    "channel_optimization": {
        "title": "Optimize YouTube Channel",
        "description": "Set up and optimize YouTube channel for discovery",
        "checklist": [
            "Create or claim YouTube channel",
            "Optimize channel description and keywords",
            "Add channel art and profile picture",
            "Link to website",
            "Create playlists",
        ],
    },
    "video_content": {
        "title": "Create Video Content",
        "description": "Produce and upload high-quality video content",
        "checklist": [
            "Plan video topics",
            "Record videos",
            "Optimize titles and descriptions",
            "Add relevant tags",
            "Create custom thumbnails",
        ],
    },
}


# ═══════════════════════════════════════════════════════════════
# PULL REQUEST / CONTENT WORKFLOW
# ═══════════════════════════════════════════════════════════════

async def create_pr_action(
    project_id: str, entity_id: str, action_type: str, details: Dict
) -> str:
    """Create a PR/content action."""
    action_id = gen_id("act-")
    await execute(
        "INSERT INTO geo_actions (id, workspace_id, action_type, title, description, payload) VALUES (?, ?, ?, ?, ?, ?)",
        (action_id, project_id, f"pr_{action_type}", f"PR/Content: {action_type.replace('_', ' ').title()}", details.get("description", ""), to_json(details))
    )
    return action_id


PR_TEMPLATES = {
    "media_outreach": {
        "title": "Media Outreach",
        "description": "Reach out to journalists and media outlets",
        "checklist": [
            "Build media contact list",
            "Craft compelling pitch",
            "Send personalized emails",
            "Follow up appropriately",
        ],
    },
    "content_distribution": {
        "title": "Distribute Content",
        "description": "Share content across multiple channels",
        "checklist": [
            "Publish on company blog",
            "Share on social media",
            "Submit to content aggregators",
            "Email to subscriber list",
        ],
    },
}


# ═══════════════════════════════════════════════════════════════
# WORKFLOW TEMPLATE LIBRARY
# ═══════════════════════════════════════════════════════════════

WORKFLOW_TEMPLATES = {
    "local": LOCAL_SEO_TEMPLATES,
    "gbp": GBP_TEMPLATES,
    "review": REVIEW_TEMPLATES,
    "authority": AUTHORITY_TEMPLATES,
    "offsite": OFFSITE_TEMPLATES,
    "reddit": REDDIT_TEMPLATES,
    "youtube": YOUTUBE_TEMPLATES,
    "pr": PR_TEMPLATES,
}


async def get_workflow_template(workflow_type: str, template_name: str) -> Optional[Dict]:
    """Get a workflow template."""
    templates = WORKFLOW_TEMPLATES.get(workflow_type, {})
    return templates.get(template_name)


async def list_workflow_templates(workflow_type: str) -> List[Dict]:
    """List all templates for a workflow type."""
    templates = WORKFLOW_TEMPLATES.get(workflow_type, {})
    return [
        {"id": k, "title": v.get("title"), "description": v.get("description")}
        for k, v in templates.items()
    ]


async def create_action_from_template(
    project_id: str, entity_id: str, workflow_type: str, template_name: str
) -> str:
    """Create an action from a template."""
    template = await get_workflow_template(workflow_type, template_name)
    if not template:
        raise ValueError(f"Template {template_name} not found")
    
    action_id = gen_id("act-")
    await execute(
        "INSERT INTO geo_actions (id, workspace_id, action_type, title, description, payload) VALUES (?, ?, ?, ?, ?, ?)",
        (
            action_id, 
            project_id, 
            f"{workflow_type}_{template_name}", 
            template.get("title"), 
            template.get("description"), 
            to_json({
                "description": template.get("description"),
                "checklist": template.get("checklist", []),
                "template_name": template_name,
            })
        )
    )
    return action_id
