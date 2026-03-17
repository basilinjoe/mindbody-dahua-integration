from __future__ import annotations

import base64
import hashlib
import io
import logging

import httpx
from PIL import Image

logger = logging.getLogger(__name__)


async def download_photo(url: str) -> bytes | None:
    """Download a photo from a URL. Returns raw bytes or None on failure."""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            content_type = resp.headers.get("content-type", "")
            if "image" not in content_type and len(resp.content) < 1000:
                logger.warning(
                    "URL %s did not return an image (content-type: %s)", url, content_type
                )
                return None
            return resp.content
    except Exception:
        logger.exception("Failed to download photo from %s", url)
        return None


def process_photo_for_dahua(photo_bytes: bytes, max_size_kb: int = 190) -> str | None:
    """
    Process a photo for Dahua face enrollment.
    - Validates it's a real image
    - Converts to RGB JPEG
    - Resizes if needed to stay under max_size_kb
    - Returns clean base64 string (no data URI prefix)
    """
    try:
        img = Image.open(io.BytesIO(photo_bytes))
    except Exception:
        logger.exception("Invalid image data")
        return None

    img = img.convert("RGB")

    # Ensure minimum face recognition size
    min_dim = 150
    if img.width < min_dim or img.height < min_dim:
        scale = max(min_dim / img.width, min_dim / img.height)
        img = img.resize((int(img.width * scale), int(img.height * scale)), Image.LANCZOS)

    max_bytes = max_size_kb * 1024

    # Try progressively lower quality until under size limit
    for quality in (95, 85, 75, 65, 50, 35):
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=quality)
        if buf.tell() <= max_bytes:
            return base64.b64encode(buf.getvalue()).decode("ascii")

    # Still too large — scale down dimensions
    for scale_factor in (0.75, 0.5, 0.35):
        resized = img.resize(
            (int(img.width * scale_factor), int(img.height * scale_factor)),
            Image.LANCZOS,
        )
        buf = io.BytesIO()
        resized.save(buf, format="JPEG", quality=70)
        if buf.tell() <= max_bytes:
            return base64.b64encode(buf.getvalue()).decode("ascii")

    logger.error("Could not compress photo under %d KB", max_size_kb)
    return None


def compute_photo_hash(photo_bytes: bytes) -> str:
    return hashlib.sha256(photo_bytes).hexdigest()
