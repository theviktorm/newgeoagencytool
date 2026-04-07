"""
Momentus AI — CMS Publisher
Real integrations for WordPress REST API and Webflow CMS API.
Publishes approved content directly to CMS platforms.
"""
from __future__ import annotations

import base64
import logging
import re
from datetime import datetime
from typing import Any, Dict, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from .config import settings
from .database import execute, fetch_one, gen_id

logger = logging.getLogger("geo.cms")


# ═══════════════════════════════════════════════════════════════
# MARKDOWN → HTML CONVERTER (for CMS publishing)
# ═══════════════════════════════════════════════════════════════

def markdown_to_html(md: str) -> str:
    """Convert markdown to WordPress-ready HTML."""
    html = md

    # Headings
    html = re.sub(r'^######\s+(.+)$', r'<h6>\1</h6>', html, flags=re.MULTILINE)
    html = re.sub(r'^#####\s+(.+)$', r'<h5>\1</h5>', html, flags=re.MULTILINE)
    html = re.sub(r'^####\s+(.+)$', r'<h4>\1</h4>', html, flags=re.MULTILINE)
    html = re.sub(r'^###\s+(.+)$', r'<h3>\1</h3>', html, flags=re.MULTILINE)
    html = re.sub(r'^##\s+(.+)$', r'<h2>\1</h2>', html, flags=re.MULTILINE)
    html = re.sub(r'^#\s+(.+)$', r'<h1>\1</h1>', html, flags=re.MULTILINE)

    # Bold and italic
    html = re.sub(r'\*\*\*(.+?)\*\*\*', r'<strong><em>\1</em></strong>', html)
    html = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', html)
    html = re.sub(r'\*(.+?)\*', r'<em>\1</em>', html)

    # Code
    html = re.sub(r'`(.+?)`', r'<code>\1</code>', html)

    # Blockquotes
    html = re.sub(r'^>\s+(.+)$', r'<blockquote>\1</blockquote>', html, flags=re.MULTILINE)

    # Lists (basic)
    html = re.sub(r'^- (.+)$', r'<li>\1</li>', html, flags=re.MULTILINE)
    html = re.sub(r'^(\d+)\.\s+(.+)$', r'<li>\2</li>', html, flags=re.MULTILINE)

    # Wrap consecutive <li> tags
    html = re.sub(
        r'((?:<li>.*?</li>\n?)+)',
        lambda m: '<ul>\n' + m.group(1) + '</ul>\n',
        html,
    )

    # Tables
    lines = html.split('\n')
    result_lines = []
    in_table = False
    for line in lines:
        if '|' in line and line.strip().startswith('|'):
            cells = [c.strip() for c in line.strip('|').split('|')]
            if all(re.match(r'^[-:]+$', c) for c in cells):
                continue  # skip separator row
            if not in_table:
                result_lines.append('<table>')
                in_table = True
            result_lines.append('<tr>' + ''.join(f'<td>{c}</td>' for c in cells) + '</tr>')
        else:
            if in_table:
                result_lines.append('</table>')
                in_table = False
            result_lines.append(line)
    if in_table:
        result_lines.append('</table>')
    html = '\n'.join(result_lines)

    # Paragraphs (wrap remaining lines)
    html = re.sub(
        r'(?<!</h[1-6]>)(?<!</li>)(?<!</ul>)(?<!</table>)(?<!</tr>)(?<!</blockquote>)\n\n(.+?)(?=\n\n|$)',
        r'\n<p>\1</p>',
        html,
        flags=re.DOTALL,
    )

    # Horizontal rules
    html = re.sub(r'^---+$', '<hr>', html, flags=re.MULTILINE)

    return html.strip()


# ═══════════════════════════════════════════════════════════════
# WORDPRESS PUBLISHER
# ═══════════════════════════════════════════════════════════════

class WordPressPublisher:
    """
    Publishes content to WordPress via the REST API.
    Uses Application Passwords for authentication.
    """

    def __init__(
        self,
        url: str = "",
        username: str = "",
        app_password: str = "",
    ):
        self.base_url = (url or settings.wordpress_url).rstrip("/")
        self.username = username or settings.wordpress_username
        self.app_password = app_password or settings.wordpress_app_password
        self.api_url = f"{self.base_url}/wp-json/wp/v2"

    @property
    def _auth_header(self) -> str:
        creds = f"{self.username}:{self.app_password}"
        encoded = base64.b64encode(creds.encode()).decode()
        return f"Basic {encoded}"

    async def check_connection(self) -> Dict[str, Any]:
        """Test WordPress REST API connectivity."""
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                # Check if REST API is accessible
                resp = await client.get(
                    f"{self.api_url}/posts",
                    headers={"Authorization": self._auth_header},
                    params={"per_page": 1},
                )

                if resp.status_code == 200:
                    return {"connected": True, "message": "WordPress REST API connected"}
                elif resp.status_code == 401:
                    return {"connected": False, "message": "Authentication failed. Check username and application password."}
                elif resp.status_code == 403:
                    return {"connected": False, "message": "Forbidden. User may lack required permissions."}
                else:
                    return {"connected": False, "message": f"WordPress returned HTTP {resp.status_code}"}

        except httpx.ConnectError as e:
            return {"connected": False, "message": f"Cannot connect to WordPress: {e}"}
        except Exception as e:
            return {"connected": False, "message": f"Connection error: {e}"}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    async def publish(
        self,
        draft_id: str,
        title: Optional[str] = None,
        slug: Optional[str] = None,
        status: str = "draft",
    ) -> Dict[str, Any]:
        """
        Publish a draft to WordPress.
        status: 'draft' | 'publish' | 'pending'
        """
        # Get draft
        draft = await fetch_one("SELECT * FROM drafts WHERE id = ?", (draft_id,))
        if not draft:
            raise ValueError(f"Draft {draft_id} not found")

        # Convert markdown to HTML
        html_content = markdown_to_html(draft["content"])

        # Extract title from content if not provided
        if not title:
            first_line = draft["content"].strip().split("\n")[0]
            title = re.sub(r'^#+\s*', '', first_line).strip() or f"Draft {draft_id}"

        # Generate slug from title if not provided
        if not slug:
            slug = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')[:80]

        payload = {
            "title": title,
            "content": html_content,
            "status": status,
            "slug": slug,
        }

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self.api_url}/posts",
                headers={
                    "Authorization": self._auth_header,
                    "Content-Type": "application/json",
                },
                json=payload,
            )

            if resp.status_code in (200, 201):
                post_data = resp.json()
                published_url = post_data.get("link", "")
                post_id = str(post_data.get("id", ""))

                # Record export
                export_id = gen_id("ex-")
                await execute(
                    "INSERT INTO exports (id, draft_id, format, content, published_to, "
                    "published_url, cms_post_id, published_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        export_id, draft_id, "html", html_content,
                        "wordpress", published_url, post_id,
                        datetime.utcnow().isoformat(),
                    ),
                )

                return {
                    "success": True,
                    "cms": "wordpress",
                    "post_id": post_id,
                    "url": published_url,
                    "status": status,
                    "export_id": export_id,
                }
            else:
                error = resp.text[:500]
                return {
                    "success": False,
                    "cms": "wordpress",
                    "error": f"HTTP {resp.status_code}: {error}",
                }


# ═══════════════════════════════════════════════════════════════
# WEBFLOW PUBLISHER
# ═══════════════════════════════════════════════════════════════

class WebflowPublisher:
    """
    Publishes content to Webflow CMS via the Data API v2.
    """

    def __init__(
        self,
        api_token: str = "",
        collection_id: str = "",
    ):
        self.api_token = api_token or settings.webflow_api_token
        self.collection_id = collection_id or settings.webflow_collection_id
        self.api_url = "https://api.webflow.com/v2"

    async def check_connection(self) -> Dict[str, Any]:
        """Test Webflow API connectivity."""
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(
                    f"{self.api_url}/sites",
                    headers={
                        "Authorization": f"Bearer {self.api_token}",
                        "Accept": "application/json",
                    },
                )
                if resp.status_code == 200:
                    return {"connected": True, "message": "Webflow API connected"}
                elif resp.status_code == 401:
                    return {"connected": False, "message": "Invalid Webflow API token"}
                else:
                    return {"connected": False, "message": f"Webflow returned HTTP {resp.status_code}"}

        except Exception as e:
            return {"connected": False, "message": f"Connection error: {e}"}

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    async def publish(
        self,
        draft_id: str,
        title: Optional[str] = None,
        slug: Optional[str] = None,
        status: str = "draft",
    ) -> Dict[str, Any]:
        """Publish a draft to Webflow CMS collection."""
        draft = await fetch_one("SELECT * FROM drafts WHERE id = ?", (draft_id,))
        if not draft:
            raise ValueError(f"Draft {draft_id} not found")

        html_content = markdown_to_html(draft["content"])

        if not title:
            first_line = draft["content"].strip().split("\n")[0]
            title = re.sub(r'^#+\s*', '', first_line).strip() or f"Draft {draft_id}"

        if not slug:
            slug = re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')[:80]

        # Webflow CMS item payload
        payload = {
            "fieldData": {
                "name": title,
                "slug": slug,
                "post-body": html_content,  # common field name
            },
            "isDraft": status != "publish",
        }

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{self.api_url}/collections/{self.collection_id}/items",
                headers={
                    "Authorization": f"Bearer {self.api_token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                json=payload,
            )

            if resp.status_code in (200, 201, 202):
                item_data = resp.json()
                item_id = item_data.get("id", "")

                export_id = gen_id("ex-")
                await execute(
                    "INSERT INTO exports (id, draft_id, format, content, published_to, "
                    "published_url, cms_post_id, published_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        export_id, draft_id, "html", html_content,
                        "webflow", "", item_id, datetime.utcnow().isoformat(),
                    ),
                )

                return {
                    "success": True,
                    "cms": "webflow",
                    "item_id": item_id,
                    "status": status,
                    "export_id": export_id,
                }
            else:
                return {
                    "success": False,
                    "cms": "webflow",
                    "error": f"HTTP {resp.status_code}: {resp.text[:500]}",
                }


# ═══════════════════════════════════════════════════════════════
# PUBLISHER FACTORY
# ═══════════════════════════════════════════════════════════════

async def publish_to_cms(
    draft_id: str,
    cms: str,
    title: Optional[str] = None,
    slug: Optional[str] = None,
    status: str = "draft",
) -> Dict[str, Any]:
    """Route to the correct CMS publisher."""
    if cms == "wordpress":
        if not settings.has_wordpress:
            return {"success": False, "error": "WordPress not configured. Set GEO_WORDPRESS_URL, GEO_WORDPRESS_USERNAME, and GEO_WORDPRESS_APP_PASSWORD."}
        publisher = WordPressPublisher()
        return await publisher.publish(draft_id, title, slug, status)

    elif cms == "webflow":
        if not settings.has_webflow:
            return {"success": False, "error": "Webflow not configured. Set GEO_WEBFLOW_API_TOKEN and GEO_WEBFLOW_COLLECTION_ID."}
        publisher = WebflowPublisher()
        return await publisher.publish(draft_id, title, slug, status)

    else:
        return {"success": False, "error": f"Unknown CMS: {cms}. Supported: wordpress, webflow"}
