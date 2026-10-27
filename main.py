"""
Terabox Streaming URL Resolver — Flask API
==========================================
Lightweight Flask app that exposes a single /api endpoint for resolving
Terabox share links into HLS streaming URLs.

Endpoints:
  GET  /              → landing page (clean light UI)
  GET  /api           → main resolver endpoint (see below)
  GET  /docs          → interactive API documentation page
  GET  /health        → health check

API parameters (GET /api):
  url    (required) — Terabox share URL
  mode   (optional) — 'stream' (default) | 'info'
    - stream : returns the HLS streaming URL only (suitable for VLC/MPV)
    - info   : returns full file metadata + streaming URL

Example:
  curl "http://localhost:5000/api?url=https://1024terabox.com/s/1LNr3tyl5pI5KUM8BecGtyQ"
"""

import os
import time
import asyncio
import logging
from typing import Any, Dict

from flask import Flask, jsonify, request, render_template, abort
from dotenv import load_dotenv

from resolver import TeraboxResolver

# ---------------------------------------------------------------------- #
# Configuration
# ---------------------------------------------------------------------- #
load_dotenv()

COOKIE = os.getenv("COOKIE_JSON", "").strip()
PROXY = os.getenv("PROXY_URL", "").strip()
PORT = int(os.getenv("PORT", "5000"))
HOST = os.getenv("HOST", "0.0.0.0")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# In-memory response cache (surl → (timestamp, data))
CACHE_TTL_SECONDS = 600  # 10 minutes
_cache: Dict[str, tuple] = {}

# ---------------------------------------------------------------------- #
# Flask app
# ---------------------------------------------------------------------- #
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("terabox-api")

app = Flask(__name__, template_folder="templates", static_folder=None)
# Native async support in Flask 3.x
app.config["TESTING"] = False


# ---------------------------------------------------------------------- #
# Helpers
# ---------------------------------------------------------------------- #
def _resolver() -> TeraboxResolver:
    if not COOKIE:
        logger.warning("COOKIE_JSON is not set — Terabox API calls will likely fail.")
    return TeraboxResolver(cookie=COOKIE, proxy=PROXY or None)


def _cache_get(surl: str) -> Dict[str, Any] | None:
    entry = _cache.get(surl)
    if not entry:
        return None
    ts, data = entry
    if time.time() - ts > CACHE_TTL_SECONDS:
        _cache.pop(surl, None)
        return None
    return data


def _cache_set(surl: str, data: Dict[str, Any]) -> None:
    _cache[surl] = (time.time(), data)
    # Bound cache size — naive eviction
    if len(_cache) > 1000:
        oldest = min(_cache.items(), key=lambda kv: kv[1][0])
        _cache.pop(oldest[0], None)


def _error(message: str, status: int = 400, **extra) -> Any:
    payload = {"success": False, "error": message, **extra}
    return jsonify(payload), status


# ---------------------------------------------------------------------- #
# Routes — UI
# ---------------------------------------------------------------------- #
@app.route("/")
def index():
    """Landing page — clean light UI with an inline tester."""
    return render_template(
        "index.html",
        cookie_set=bool(COOKIE),
        proxy_set=bool(PROXY),
    )


@app.route("/docs")
def docs():
    """API documentation page."""
    return render_template("docs.html", cookie_set=bool(COOKIE))


# ---------------------------------------------------------------------- #
# Routes — API
# ---------------------------------------------------------------------- #
@app.route("/health")
def health():
    return jsonify(
        {
            "status": "ok",
            "cookie_configured": bool(COOKIE),
            "proxy_configured": bool(PROXY),
            "cache_size": len(_cache),
            "cache_ttl_seconds": CACHE_TTL_SECONDS,
        }
    )


@app.route("/api", methods=["GET"])
async def api():
    """
    Resolve a Terabox share URL into a streaming URL.

    Query params:
      url  (required) — the Terabox share link
      mode (optional) — 'stream' (default) | 'info'

    Returns JSON:
      {
        "success": true,
        "data": {
          "streaming_url": "https://...m3u8",
          "filename": "...",
          "size": 12345,
          "is_video": true,
          "cached": false
        }
      }
    """
    share_url = (request.args.get("url") or "").strip()
    mode = (request.args.get("mode") or "stream").strip().lower()

    if not share_url:
        return _error("Missing required parameter: url", 400)
    if mode not in ("stream", "info"):
        return _error("Invalid mode. Use 'stream' or 'info'.", 400)

    # Extract surl for cache key
    try:
        from resolver import TeraboxResolver as _TR
        surl = _TR.extract_surl(share_url)
    except ValueError as exc:
        return _error(str(exc), 400)

    # Cache hit?
    cached = _cache_get(surl)
    if cached:
        logger.info(f"Cache hit for surl={surl}")
        if mode == "stream":
            cached = {
                k: cached.get(k)
                for k in ("streaming_url", "filename", "is_video", "cached")
            }
        cached["cached"] = True
        return jsonify({"success": True, "data": cached})

    # Cache miss → resolve upstream
    resolver = _resolver()
    try:
        result = await resolver.get_streaming_url(share_url)
    except RuntimeError as exc:
        logger.error(f"Resolution failed for {share_url}: {exc}")
        return _error(f"Failed to resolve: {exc}", 502, surl=surl)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected error during resolution")
        return _error(f"Unexpected error: {exc}", 500, surl=surl)

    # Cache the full info
    _cache_set(surl, result)

    payload = dict(result)
    payload["cached"] = False

    if mode == "stream":
        # Slim response — only streaming-relevant fields
        payload = {
            "streaming_url": payload.get("streaming_url"),
            "filename": payload.get("filename"),
            "is_video": payload.get("is_video"),
            "cached": False,
        }

    return jsonify({"success": True, "data": payload})


# ---------------------------------------------------------------------- #
# Entry point
# ---------------------------------------------------------------------- #
if __name__ == "__main__":
    logger.info(f"Starting Terabox Streaming API on {HOST}:{PORT}")
    logger.info(f"  Cookie configured : {bool(COOKIE)}")
    logger.info(f"  Proxy configured  : {bool(PROXY) or 'direct mode'}")
    logger.info(f"  Docs URL          : http://localhost:{PORT}/docs")
    # Flask 3.x supports async route handlers natively
    app.run(host=HOST, port=PORT, debug=os.getenv("DEBUG", "0") == "1")
