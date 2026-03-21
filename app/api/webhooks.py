from __future__ import annotations

import logging

from fastapi import APIRouter, Request, Response

from app.utils.hmac_verify import verify_mindbody_signature

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/webhooks", tags=["webhooks"])


@router.head("/mindbody")
async def mindbody_webhook_validation():
    """MindBody sends HEAD to validate the webhook URL when creating a subscription."""
    return Response(status_code=200)


@router.post("/mindbody")
async def mindbody_webhook(request: Request):
    """
    Receive MindBody webhook events.
    Verifies signature and logs the event. The next scheduled integration run
    will pick up any membership changes.
    Returns 200 immediately (MindBody requires response within 10s).
    """
    body = await request.body()
    signature = request.headers.get("X-Mindbody-Signature", "")
    settings = request.app.state.settings

    if not verify_mindbody_signature(body, signature, settings.mindbody_webhook_signature_key):
        logger.warning("Invalid MindBody webhook signature")
        return Response(status_code=401)

    try:
        payload = await request.json()
    except Exception:
        logger.warning("MindBody webhook received non-JSON body")
        return Response(status_code=400)

    if not isinstance(payload, dict):
        logger.warning("MindBody webhook payload is not a JSON object")
        return Response(status_code=400)

    event_id = payload.get("eventId", "")
    if not event_id:
        logger.warning("MindBody webhook missing eventId field")
        return Response(status_code=400)

    event_data = payload.get("eventData", {})
    client_id = event_data.get("clientId") or event_data.get("ClientId")
    if not client_id:
        logger.warning("MindBody webhook event=%s missing clientId in eventData", event_id)

    logger.info("MindBody webhook received: event=%s clientId=%s", event_id, client_id)

    return Response(status_code=200)
