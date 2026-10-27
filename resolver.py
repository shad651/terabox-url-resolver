"""
Terabox Streaming URL Resolver
==============================
Core module that extracts HLS (.m3u8) streaming URLs from Terabox share links.

Flow:
  1. Parse share URL → extract short URL (surl)
  2. Call Terabox share-list API → get file metadata (path, fs_id)
  3. Call Terabox streaming API → get HLS playlist URL
  4. Return streaming URL to caller (optionally rewritten through proxy)

Author: Built for shad651/Terabox-link-resolver inspired project
"""

import re
import json
import asyncio
from typing import Optional, Dict, Any
from urllib.parse import quote, urlparse, parse_qs

import aiohttp


# Terabox domains that share the same backend
TERABOX_DOMAINS = [
    "terabox.com",
    "1024terabox.com",
    "teraboxapp.com",
    "nephobox.com",
    "4funbox.com",
    "mirrobox.com",
    "momerybox.com",
    "tibibox.com",
    "terafileshare.com",
    "terasharelink.com",
]

# Default request headers — mimic a real browser to avoid bot detection
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.terabox.com/",
    "Origin": "https://www.terabox.com",
}

# Terabox app_id used across all official API calls
APP_ID = "250528"


class TeraboxResolver:
    """Async resolver that pulls HLS streaming URLs out of Terabox share links."""

    def __init__(
        self,
        cookie: str,
        proxy: Optional[str] = None,
        timeout: int = 15,
    ):
        """
        Args:
            cookie: Either a raw `ndus` token string, or a JSON object string
                    like '{"ndus": "...", "other": "..."}'.
            proxy:  Optional URL of a Cloudflare Worker / reverse proxy that
                    forwards requests to Terabox. If set, all upstream calls
                    go through `<proxy>?url=<terabox_url>` instead of direct.
            timeout: Per-request timeout in seconds.
        """
        self.cookie_raw = (cookie or "").strip()
        self.proxy = (proxy or "").strip() or None
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.headers = {**DEFAULT_HEADERS, "Cookie": self._format_cookie()}

    # ------------------------------------------------------------------ #
    # Cookie helpers
    # ------------------------------------------------------------------ #
    def _format_cookie(self) -> str:
        """Normalise the cookie input into a `key=value; key=value` header."""
        raw = self.cookie_raw
        if not raw:
            return ""
        # JSON form: {"ndus": "xxx", "BDCLND": "yyy"}
        if raw.startswith("{"):
            try:
                data = json.loads(raw)
                return "; ".join(f"{k}={v}" for k, v in data.items())
            except json.JSONDecodeError:
                # Fall through, treat as raw ndus
                pass
        # Plain ndus token
        if "=" in raw:
            return raw
        return f"ndus={raw}"

    # ------------------------------------------------------------------ #
    # URL parsing
    # ------------------------------------------------------------------ #
    @staticmethod
    def extract_surl(url: str) -> str:
        """
        Extract the short URL identifier (surl) from any Terabox share URL.

        Supports formats like:
          - https://www.terabox.com/s/1LNr3tyl5pI5KUM8BecGtyQ
          - https://1024terabox.com/s/1LNr3tyl5pI5KUM8BecGtyQ?path=/
          - https://teraboxapp.com/sharing/link?surl=1LNr3tyl5pI5KUM8BecGtyQ

        Returns the surl WITH the leading '1' (Terabox internal convention).
        """
        if not url:
            raise ValueError("Empty URL provided")

        # Try /s/{surl} pattern first (most common)
        match = re.search(r"/s/([^?/&#]+)", url)
        if match:
            surl = match.group(1)
            return surl if surl.startswith("1") else f"1{surl}"

        # Try ?surl=... query param
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        if "surl" in params:
            surl = params["surl"][0]
            return surl if surl.startswith("1") else f"1{surl}"

        raise ValueError(f"Could not extract surl from URL: {url}")

    # ------------------------------------------------------------------ #
    # HTTP helpers
    # ------------------------------------------------------------------ #
    def _build_upstream_url(self, terabox_url: str) -> str:
        """If a proxy is configured, route the request through it."""
        if not self.proxy:
            return terabox_url
        sep = "&" if "?" in self.proxy else "?"
        return f"{self.proxy}{sep}url={quote(terabox_url, safe='')}"

    async def _fetch_json(self, session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:
        """GET a URL and return parsed JSON, raising on HTTP errors."""
        upstream = self._build_upstream_url(url)
        async with session.get(upstream, headers=self.headers, timeout=self.timeout) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(
                    f"Upstream returned HTTP {resp.status} for {upstream}: {text[:200]}"
                )
            try:
                return json.loads(text)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Invalid JSON from {upstream}: {text[:200]}") from exc

    # ------------------------------------------------------------------ #
    # Main resolver pipeline
    # ------------------------------------------------------------------ #
    async def get_file_info(self, surl: str) -> Dict[str, Any]:
        """
        Step 1 — Call the share-list API to get file metadata.

        Returns the first file's info dict (path, fs_id, size, filename, etc.).
        """
        share_url = (
            "https://www.terabox.com/share/wxlist?"
            f"channel=dubox&clienttype=25&web=1&app_id={APP_ID}"
            f"&shorturl={surl}&root=1"
        )
        async with aiohttp.ClientSession() as session:
            data = await self._fetch_json(session, share_url)

        if data.get("errno") not in (0, None):
            raise RuntimeError(
                f"Terabox share-list error: errno={data.get('errno')} "
                f"msg={data.get('errmsg', 'unknown')}"
            )

        files = data.get("list") or []
        if not files:
            raise RuntimeError("No files found in this share link")

        # Pick the first file (most Terabox shares are single-file)
        return files[0]

    async def get_streaming_url(self, share_url: str) -> Dict[str, Any]:
        """
        Full pipeline — take a share URL, return streaming info.

        Returns dict with:
          - streaming_url  : the m3u8 HLS URL (or None if not a video)
          - download_url   : direct download dlink
          - filename       : original file name
          - size           : file size in bytes
          - path           : internal Terabox path
          - surl           : extracted short URL
        """
        surl = self.extract_surl(share_url)
        file_info = await self.get_file_info(surl)

        path = file_info.get("path", "")
        filename = file_info.get("server_filename") or file_info.get("filename", "unknown")
        size = int(file_info.get("size", 0))
        dlink = file_info.get("dlink", "")

        # Step 2 — fetch HLS streaming URL (only works for video files)
        streaming_url = None
        if path:
            stream_api = (
                "https://www.terabox.com/api/streaming?"
                f"path={quote(path)}&type=M3U8_AUTO&player=3"
                f"&clienttype=0&app_id={APP_ID}&web=1"
            )
            try:
                async with aiohttp.ClientSession() as session:
                    stream_data = await self._fetch_json(session, stream_api)
                if stream_data.get("errno") in (0, None):
                    streaming_url = (
                        stream_data.get("streaming_url")
                        or stream_data.get("l_link")
                        or stream_data.get("url")
                    )
            except RuntimeError:
                # Non-video files won't have a streaming URL — that's OK
                streaming_url = None

        return {
            "streaming_url": streaming_url,
            "download_url": dlink or None,
            "filename": filename,
            "size": size,
            "path": path,
            "surl": surl,
            "is_video": streaming_url is not None,
        }


# ---------------------------------------------------------------------- #
# Convenience sync wrapper for CLI / script usage
# ---------------------------------------------------------------------- #
def resolve_streaming_url(share_url: str, cookie: str, proxy: Optional[str] = None) -> Dict[str, Any]:
    """Synchronous one-shot resolver — handy for quick scripts."""
    resolver = TeraboxResolver(cookie=cookie, proxy=proxy)
    return asyncio.run(resolver.get_streaming_url(share_url))
