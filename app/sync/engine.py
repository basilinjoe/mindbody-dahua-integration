from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.clients.dahua import DahuaClient
from app.clients.mindbody import MindBodyClient
from app.config import Settings
from app.models.device import DahuaDevice
from app.models.member import SyncedMember
from app.models.mindbody_client import MindBodyClient as MindBodyClientModel
from app.sync.mindbody_client_service import refresh_mindbody_clients
from app.models.sync_log import SyncLog
from app.utils.photo import download_photo, process_photo_for_dahua

logger = logging.getLogger(__name__)


@dataclass
class SyncReport:
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None
    total_clients: int = 0
    active_members: int = 0
    enrolled: int = 0
    deactivated: int = 0
    reactivated: int = 0
    photos_uploaded: int = 0
    photos_missing: int = 0
    errors: list[str] = field(default_factory=list)


def _make_dahua_user_id(client_id: str) -> str:
    """Deterministic UserID from MindBody client ID."""
    return str(int(client_id)) if client_id.isdigit() else client_id


def _make_card_no(client_id: str) -> str:
    """Synthetic card number for face-only auth."""
    num = int(client_id) if client_id.isdigit() else abs(hash(client_id)) % 100_000_000
    return f"MB{num:08d}"


class SyncEngine:
    """
    Orchestrates reconciliation between MindBody membership state
    and multiple Dahua devices.
    """

    def __init__(
        self,
        mindbody: MindBodyClient,
        db_session_factory,
        settings: Settings,
    ) -> None:
        self.mindbody = mindbody
        self.db_session_factory = db_session_factory
        self.settings = settings
        self._dahua_clients: dict[int, DahuaClient] = {}  # device_id -> client

    # ---- Device Management --------------------------------------------------

    def get_dahua_clients(self) -> list[DahuaClient]:
        return list(self._dahua_clients.values())

    def refresh_devices(self, db: Session) -> None:
        """Rebuild DahuaClient instances from enabled devices in DB."""
        devices = db.query(DahuaDevice).filter(DahuaDevice.is_enabled.is_(True)).all()
        # Close removed clients
        current_ids = {d.id for d in devices}
        for did in list(self._dahua_clients):
            if did not in current_ids:
                # can't await here, but the old client will be garbage-collected
                del self._dahua_clients[did]
        # Create / update clients
        for dev in devices:
            if dev.id not in self._dahua_clients:
                self._dahua_clients[dev.id] = DahuaClient(
                    host=dev.host,
                    port=dev.port,
                    username=dev.username,
                    password=dev.password,
                    door_ids=dev.door_ids,
                    device_id=dev.id,
                    device_name=dev.name,
                )
        logger.info("Refreshed Dahua clients: %d enabled devices", len(self._dahua_clients))

    # ---- Full Sync ----------------------------------------------------------

    async def full_sync(self) -> SyncReport:
        """Full reconciliation across all enabled devices."""
        report = SyncReport()
        db: Session = self.db_session_factory()
        try:
            self.refresh_devices(db)
            if not self._dahua_clients:
                report.errors.append("No enabled Dahua devices configured")
                report.completed_at = datetime.now(timezone.utc)
                return report

            # 1. Fetch all MindBody clients
            try:
                mb_clients = await self.mindbody.get_all_clients()
            except Exception as e:
                report.errors.append(f"Failed to fetch MindBody clients: {e}")
                report.completed_at = datetime.now(timezone.utc)
                return report

            report.total_clients = len(mb_clients)

            # 1b. Persist raw MindBody client data for the MindBody Users page
            try:
                self._refresh_mindbody_clients(db, mb_clients)
            except Exception as e:
                logger.warning("Failed to refresh mindbody_clients table: %s", e)

            # 2. Check membership status for each client
            active_ids: set[str] = set()
            client_map: dict[str, dict] = {}
            for client in mb_clients:
                cid = str(client.get("Id", ""))
                if not cid:
                    continue
                client_map[cid] = client
                try:
                    if await self.mindbody.is_member_active(cid):
                        active_ids.add(cid)
                except Exception as e:
                    logger.warning("Could not check membership for client %s: %s", cid, e)

            report.active_members = len(active_ids)

            # 3. Load existing synced members from DB
            existing: dict[str, SyncedMember] = {
                m.mindbody_client_id: m
                for m in db.query(SyncedMember).filter(SyncedMember.is_manual.is_(False)).all()
            }

            # 4. Determine actions
            existing_ids = set(existing.keys())
            to_enroll = active_ids - existing_ids
            to_deactivate = {cid for cid in existing_ids if cid not in active_ids and existing[cid].is_active_in_dahua}
            to_reactivate = {cid for cid in active_ids & existing_ids if not existing[cid].is_active_in_dahua}

            # 5. Execute enroll
            for cid in to_enroll:
                client = client_map.get(cid, {})
                try:
                    await self._enroll_member(client, cid, db, report, sync_type="full_poll")
                    report.enrolled += 1
                except Exception as e:
                    report.errors.append(f"Enroll {cid}: {e}")
                    logger.exception("Failed to enroll %s", cid)

            # 6. Execute deactivate
            for cid in to_deactivate:
                member = existing[cid]
                try:
                    await self._deactivate_member(member, db, sync_type="full_poll")
                    report.deactivated += 1
                except Exception as e:
                    report.errors.append(f"Deactivate {cid}: {e}")
                    logger.exception("Failed to deactivate %s", cid)

            # 7. Execute reactivate
            for cid in to_reactivate:
                member = existing[cid]
                client = client_map.get(cid, {})
                try:
                    await self._reactivate_member(member, client, db, sync_type="full_poll")
                    report.reactivated += 1
                except Exception as e:
                    report.errors.append(f"Reactivate {cid}: {e}")
                    logger.exception("Failed to reactivate %s", cid)

            db.commit()
        except Exception as e:
            db.rollback()
            report.errors.append(f"Full sync error: {e}")
            logger.exception("Full sync failed")
        finally:
            db.close()

        report.completed_at = datetime.now(timezone.utc)
        logger.info(
            "Full sync complete: enrolled=%d deactivated=%d reactivated=%d errors=%d",
            report.enrolled,
            report.deactivated,
            report.reactivated,
            len(report.errors),
        )
        return report

    # ---- Single Member Sync -------------------------------------------------

    async def sync_single_member(self, mindbody_client_id: str, sync_type: str = "webhook") -> None:
        """Sync a single member (webhook-triggered or manual)."""
        db: Session = self.db_session_factory()
        try:
            self.refresh_devices(db)
            cid = str(mindbody_client_id)
            is_active = await self.mindbody.is_member_active(cid)
            member = db.query(SyncedMember).filter_by(mindbody_client_id=cid).first()

            if member is None and is_active:
                # New active member — enroll
                clients = await self.mindbody.get_clients(search_text=cid, limit=1)
                client = clients[0] if clients else {}
                report = SyncReport()
                await self._enroll_member(client, cid, db, report, sync_type=sync_type)
            elif member is not None and not is_active and member.is_active_in_dahua:
                # Member lost membership — deactivate
                await self._deactivate_member(member, db, sync_type=sync_type)
            elif member is not None and is_active and not member.is_active_in_dahua:
                # Member regained membership — reactivate
                clients = await self.mindbody.get_clients(search_text=cid, limit=1)
                client = clients[0] if clients else {}
                await self._reactivate_member(member, client, db, sync_type=sync_type)
            else:
                logger.debug("No action needed for client %s", cid)

            db.commit()
        except Exception:
            db.rollback()
            logger.exception("Single member sync failed for %s", mindbody_client_id)
        finally:
            db.close()

    # ---- Internal Actions ---------------------------------------------------

    async def _enroll_member(
        self,
        client: dict,
        client_id: str,
        db: Session,
        report: SyncReport,
        sync_type: str = "full_poll",
    ) -> None:
        user_id = _make_dahua_user_id(client_id)
        card_no = _make_card_no(client_id)
        first_name = client.get("FirstName", "")
        last_name = client.get("LastName", "")
        card_name = f"{first_name} {last_name}".strip() or f"Member-{client_id}"

        # Add user to all devices in parallel
        results = await asyncio.gather(
            *[
                dc.add_user(user_id=user_id, card_name=card_name, card_no=card_no)
                for dc in self._dahua_clients.values()
            ],
            return_exceptions=True,
        )
        success = any(r is True for r in results)

        # Attempt face photo
        has_photo = False
        photo_source = None
        photo_url = client.get("PhotoUrl")
        if photo_url and "default" not in photo_url.lower():
            photo_bytes = await download_photo(photo_url)
            if photo_bytes:
                b64 = process_photo_for_dahua(photo_bytes, self.settings.photo_max_size_kb)
                if b64:
                    photo_results = await asyncio.gather(
                        *[dc.upload_face_photo(user_id, b64, card_name) for dc in self._dahua_clients.values()],
                        return_exceptions=True,
                    )
                    if any(r is True for r in photo_results):
                        has_photo = True
                        photo_source = "mindbody"
                        report.photos_uploaded += 1

        if not has_photo:
            report.photos_missing += 1

        # Save to DB
        member = SyncedMember(
            mindbody_client_id=client_id,
            dahua_user_id=user_id,
            card_no=card_no,
            first_name=first_name,
            last_name=last_name,
            email=client.get("Email"),
            is_active_in_mindbody=True,
            is_active_in_dahua=success,
            has_face_photo=has_photo,
            face_photo_source=photo_source,
            last_synced_at=datetime.now(timezone.utc),
        )
        db.add(member)

        self._log(db, sync_type, "enroll", client_id, card_name, success)

    async def _deactivate_member(self, member: SyncedMember, db: Session, sync_type: str = "full_poll") -> None:
        results = await asyncio.gather(
            *[dc.update_user_status(member.dahua_user_id, card_status=4) for dc in self._dahua_clients.values()],
            return_exceptions=True,
        )
        success = any(r is True for r in results)
        member.is_active_in_mindbody = False
        member.is_active_in_dahua = False
        member.last_synced_at = datetime.now(timezone.utc)
        self._log(db, sync_type, "deactivate", member.mindbody_client_id, member.full_name, success)

    async def _reactivate_member(
        self, member: SyncedMember, client: dict, db: Session, sync_type: str = "full_poll"
    ) -> None:
        results = await asyncio.gather(
            *[dc.update_user_status(member.dahua_user_id, card_status=0) for dc in self._dahua_clients.values()],
            return_exceptions=True,
        )
        success = any(r is True for r in results)
        member.is_active_in_mindbody = True
        member.is_active_in_dahua = success
        member.last_synced_at = datetime.now(timezone.utc)

        # If photo was missing, try again
        if not member.has_face_photo:
            photo_url = client.get("PhotoUrl")
            if photo_url and "default" not in photo_url.lower():
                photo_bytes = await download_photo(photo_url)
                if photo_bytes:
                    b64 = process_photo_for_dahua(photo_bytes, self.settings.photo_max_size_kb)
                    if b64:
                        await asyncio.gather(
                            *[
                                dc.upload_face_photo(member.dahua_user_id, b64, member.full_name)
                                for dc in self._dahua_clients.values()
                            ],
                            return_exceptions=True,
                        )
                        member.has_face_photo = True
                        member.face_photo_source = "mindbody"

        self._log(db, sync_type, "reactivate", member.mindbody_client_id, member.full_name, success)

    async def upload_photo_to_all_devices(self, member: SyncedMember, photo_base64: str) -> bool:
        """Upload a face photo to all enabled devices for a given member."""
        results = await asyncio.gather(
            *[dc.upload_face_photo(member.dahua_user_id, photo_base64, member.full_name) for dc in self._dahua_clients.values()],
            return_exceptions=True,
        )
        return any(r is True for r in results)

    async def enroll_manual_member(
        self,
        first_name: str,
        last_name: str,
        email: str | None,
        manual_id: str,
        photo_base64: str | None,
        db: Session,
    ) -> SyncedMember:
        """Enroll a manually-added member (not from MindBody) on all devices."""
        self.refresh_devices(db)
        user_id = _make_dahua_user_id(manual_id)
        card_no = _make_card_no(manual_id)
        card_name = f"{first_name} {last_name}".strip()

        await asyncio.gather(
            *[dc.add_user(user_id=user_id, card_name=card_name, card_no=card_no) for dc in self._dahua_clients.values()],
            return_exceptions=True,
        )

        has_photo = False
        if photo_base64:
            photo_results = await asyncio.gather(
                *[dc.upload_face_photo(user_id, photo_base64, card_name) for dc in self._dahua_clients.values()],
                return_exceptions=True,
            )
            has_photo = any(r is True for r in photo_results)

        member = SyncedMember(
            mindbody_client_id=manual_id,
            dahua_user_id=user_id,
            card_no=card_no,
            first_name=first_name,
            last_name=last_name,
            email=email,
            is_active_in_mindbody=True,
            is_active_in_dahua=True,
            has_face_photo=has_photo,
            face_photo_source="manual" if has_photo else None,
            is_manual=True,
            last_synced_at=datetime.now(timezone.utc),
        )
        db.add(member)
        self._log(db, "manual", "enroll", manual_id, card_name, True)
        return member

    # ---- Device Health ------------------------------------------------------

    async def check_device_health(self) -> None:
        """Check health of all enabled devices and update DB status."""
        db: Session = self.db_session_factory()
        try:
            self.refresh_devices(db)
            for dev_id, client in self._dahua_clients.items():
                device = db.query(DahuaDevice).get(dev_id)
                if not device:
                    continue
                try:
                    online = await client.health_check()
                    device.status = "online" if online else "offline"
                    if online:
                        device.last_seen_at = datetime.now(timezone.utc)
                except Exception:
                    device.status = "error"
            db.commit()
        except Exception:
            db.rollback()
            logger.exception("Device health check failed")
        finally:
            db.close()

    # ---- MindBody Client Cache ----------------------------------------------

    def _refresh_mindbody_clients(self, db: Session, mb_clients: list[dict]) -> None:
        refresh_mindbody_clients(db, mb_clients)

    # ---- Logging ------------------------------------------------------------

    def _log(
        self,
        db: Session,
        sync_type: str,
        action: str,
        client_id: str | None,
        member_name: str | None,
        success: bool,
        error_message: str | None = None,
    ) -> None:
        entry = SyncLog(
            sync_type=sync_type,
            action=action,
            mindbody_client_id=client_id,
            member_name=member_name,
            success=success,
            error_message=error_message,
        )
        db.add(entry)
