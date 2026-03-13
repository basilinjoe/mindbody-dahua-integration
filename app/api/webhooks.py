from __future__ import annotations

import logging

from fastapi import APIRouter, BackgroundTasks, Request, Response
from prefect.deployments import run_deployment

from app.utils.hmac_verify import verify_mindbody_signature

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/webhooks", tags=["webhooks"])

# MindBody event types that indicate a membership change
RELEVANT_EVENTS = {
    "client.created",
    "client.updated",
    "clientContract.created",
    "clientContract.updated",
    "sale.completed",
}


@router.head("/mindbody")
async def mindbody_webhook_validation():
    """MindBody sends HEAD to validate the webhook URL when creating a subscription."""
    return Response(status_code=200)


@router.post("/mindbody")
async def mindbody_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Receive MindBody webhook events.
    Verifies signature, extracts clientId, and triggers background sync.
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
        return Response(status_code=400)

    event_id = payload.get("eventId", "")
    event_data = payload.get("eventData", {})
    client_id = event_data.get("clientId") or event_data.get("ClientId")

    logger.info("MindBody webhook: event=%s clientId=%s", event_id, client_id)

    if event_id in RELEVANT_EVENTS and client_id:
        async def _trigger_sync():
            try:
                await run_deployment(
                    "sync-member/default",
                    parameters={"client_id": str(client_id), "sync_type": "webhook"},
                    timeout=0,  # fire and forget
                )
                logger.info("Triggered Prefect sync-member/default for clientId=%s", client_id)
            except Exception:
                logger.exception("Failed to trigger Prefect deployment for clientId=%s", client_id)

        background_tasks.add_task(_trigger_sync)

    return Response(status_code=200)
