"""
Terabox Streaming URL Resolver — Flask API
==========================================
Lightweight Flask app that exposes a single /api endpoint for resolving
Terabox share links into HLS streaming URLs.
"""

import os
import time
import logging
from typing import Any, Dict

from flask import Flask, jsonify, request, render_template
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

# In-memory response cache
CACHE_TTL_SECONDS = 600
_cache: Dict[str, tuple] = {}

# ---------------------------------------------------------------------- #
# Logging
# ---------------------------------------------------------------------- #
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger("terabox-api")

# ---------------------------------------------------------------------- #
# Flask app
# ---------------------------------------------------------------------- #
app = Flask(
    __name__,
    template_folder=".",
    static_folder=None,
)

app.config["TESTING"] = False

# ---------------------------------------------------------------------- #
# Helpers
# ---------------------------------------------------------------------- #
def _resolver() -> TeraboxResolver:
    if not COOKIE:
        logger.warning(
            "COOKIE_JSON is not set — Terabox API calls will likely fail."
        )

    return TeraboxResolver(
        cookie=COOKIE,
        proxy=PROXY or None,
    )


def _cache_get(surl: str) -> Dict[str, Any] | None:
    entry = _cache.get(surl)

    if not entry:
        return None

    timestamp, data = entry

    if time.time() - timestamp > CACHE_TTL_SECONDS:
        _cache.pop(surl, None)
        return None

    return data


def _cache_set(surl: str, data: Dict[str, Any]) -> None:
    _cache[surl] = (time.time(), data)

    if len(_cache) > 1000:
        oldest = min(
            _cache.items(),
            key=lambda item: item[1][0],
        )
        _cache.pop(oldest[0], None)


def _error(message: str, status: int = 400, **extra) -> Any:
    payload = {
        "success": False,
        "error": message,
        **extra,
    }

    return jsonify(payload), status


# ---------------------------------------------------------------------- #
# Routes — UI
# ---------------------------------------------------------------------- #
@app.route("/")
def index():
    """Landing page."""
    return render_template(
        "index.html",
        cookie_set=bool(COOKIE),
        proxy_set=bool(PROXY),
    )


@app.route("/docs")
def docs():
    """API documentation page."""
    return render_template(
        "docs.html",
        cookie_set=bool(COOKIE),
    )


# ---------------------------------------------------------------------- #
# Routes — API
# ---------------------------------------------------------------------- #
@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "cookie_configured": bool(COOKIE),
        "proxy_configured": bool(PROXY),
        "cache_size": len(_cache),
        "cache_ttl_seconds": CACHE_TTL_SECONDS,
    })


@app.route("/api", methods=["GET"])
async def api():
    """
    Resolve a Terabox share URL.

    Query parameters:
      url  (required): Terabox share link
      mode (optional): stream or info
    """

    share_url = (request.args.get("url") or "").strip()
    mode = (request.args.get("mode") or "stream").strip().lower()

    if not share_url:
        return _error(
            "Missing required parameter: url",
            400,
        )

    if mode not in ("stream", "info"):
        return _error(
            "Invalid mode. Use 'stream' or 'info'.",
            400,
        )

    # Extract surl for cache key.
    try:
        surl = TeraboxResolver.extract_surl(share_url)
    except ValueError as exc:
        return _error(str(exc), 400)

    # Check cache.
    cached = _cache_get(surl)

    if cached is not None:
        logger.info("Cache hit for surl=%s", surl)

        payload = dict(cached)

        if mode == "stream":
            payload = {
                key: cached.get(key)
                for key in (
                    "streaming_url",
                    "filename",
                    "is_video",
                )
            }

        payload["cached"] = True

        return jsonify({
            "success": True,
            "data": payload,
        })

    # Resolve the share link.
    resolver = _resolver()

    try:
        result = await resolver.get_streaming_url(share_url)

    except RuntimeError as exc:
        logger.error(
            "Resolution failed for surl=%s: %s",
            surl,
            exc,
        )

        return _error(
            f"Failed to resolve: {exc}",
            502,
            surl=surl,
        )

    except Exception as exc:
        logger.exception(
            "Unexpected error during resolution: %s",
            type(exc).__name__,
        )

        error_message = str(exc).strip() or type(exc).__name__

        return _error(
            f"Unexpected error ({type(exc).__name__}): {error_message}",
            500,
            surl=surl,
        )

    # Cache the complete result.
    _cache_set(surl, result)

    payload = dict(result)
    payload["cached"] = False

    if mode == "stream":
        payload = {
            "streaming_url": result.get("streaming_url"),
            "filename": result.get("filename"),
            "is_video": result.get("is_video"),
            "cached": False,
        }

    return jsonify({
        "success": True,
        "data": payload,
    })


# ---------------------------------------------------------------------- #
# Entry point
# ---------------------------------------------------------------------- #
if __name__ == "__main__":
    logger.info(
        "Starting Terabox Streaming API on %s:%s",
        HOST,
        PORT,
    )
    logger.info("Cookie configured: %s", bool(COOKIE))
    logger.info(
        "Proxy configured: %s",
        bool(PROXY) or "direct mode",
    )
    logger.info(
        "Docs URL: http://localhost:%s/docs",
        PORT,
    )

    app.run(
        host=HOST,
        port=PORT,
        debug=os.getenv("DEBUG", "0") == "1",
    )