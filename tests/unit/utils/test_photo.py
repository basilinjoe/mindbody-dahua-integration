from __future__ import annotations

import base64
import io

import httpx
import pytest
import respx
from PIL import Image

from app.utils.photo import download_photo, process_photo_for_dahua


def _jpeg_bytes(width: int, height: int, color: tuple[int, int, int] = (10, 20, 30)) -> bytes:
    img = Image.new("RGB", (width, height), color)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=95)
    return buf.getvalue()


def test_process_photo_invalid_bytes_returns_none() -> None:
    assert process_photo_for_dahua(b"not-an-image") is None


def test_process_photo_upscales_small_images() -> None:
    data = _jpeg_bytes(80, 90)
    out = process_photo_for_dahua(data, max_size_kb=190)
    assert out is not None

    decoded = base64.b64decode(out)
    result = Image.open(io.BytesIO(decoded))
    assert result.width >= 150
    assert result.height >= 150


def test_process_photo_respects_max_size_limit() -> None:
    data = _jpeg_bytes(1400, 1200)
    out = process_photo_for_dahua(data, max_size_kb=40)
    assert out is not None
    assert len(base64.b64decode(out)) <= 40 * 1024


def test_process_photo_fallback_resize_path(monkeypatch: pytest.MonkeyPatch) -> None:
    data = _jpeg_bytes(400, 400)

    def fake_save(self, fp, format=None, quality=None):  # noqa: ANN001, ANN202
        if self.width >= 300:
            fp.write(b"x" * 1800)
        else:
            fp.write(b"x" * 500)

    monkeypatch.setattr(Image.Image, "save", fake_save)
    out = process_photo_for_dahua(data, max_size_kb=1)

    assert out is not None
    assert len(base64.b64decode(out)) == 500


@pytest.mark.asyncio
@respx.mock
async def test_download_photo_success() -> None:
    url = "https://example.test/photo.jpg"
    content = _jpeg_bytes(200, 200)
    respx.get(url).respond(200, content=content, headers={"content-type": "image/jpeg"})

    result = await download_photo(url)
    assert result == content


@pytest.mark.asyncio
@respx.mock
async def test_download_photo_non_image_small_payload_returns_none() -> None:
    url = "https://example.test/not-image"
    respx.get(url).respond(200, content=b"oops", headers={"content-type": "text/plain"})

    result = await download_photo(url)
    assert result is None


@pytest.mark.asyncio
@respx.mock
async def test_download_photo_request_failure_returns_none() -> None:
    url = "https://example.test/fail.jpg"
    respx.get(url).mock(side_effect=httpx.ConnectError("boom"))

    result = await download_photo(url)
    assert result is None
