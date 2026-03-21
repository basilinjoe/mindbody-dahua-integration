from __future__ import annotations

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.export_job import ExportJob, ExportStatus


async def create(db: AsyncSession) -> ExportJob:
    """Create a new ExportJob in pending state, commit, and return the refreshed instance."""
    job = ExportJob(status=ExportStatus.pending)
    db.add(job)
    await db.commit()
    await db.refresh(job)
    return job


async def get(db: AsyncSession, job_id: int) -> ExportJob | None:
    """Return a single ExportJob by primary key, or None."""
    result = await db.execute(select(ExportJob).where(ExportJob.id == job_id))
    return result.scalar_one_or_none()


async def list_all(db: AsyncSession, limit: int = 20) -> list[ExportJob]:
    """Return the most recent export jobs ordered by created_at descending."""
    result = await db.execute(select(ExportJob).order_by(ExportJob.created_at.desc()).limit(limit))
    return list(result.scalars().all())


async def update(
    db: AsyncSession,
    job_id: int,
    *,
    status: ExportStatus | None = None,
    error_msg: str | None = None,
    zip_path: str | None = None,
    file_name: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> None:
    """Apply partial updates to an ExportJob. No-op if job not found."""
    result = await db.execute(select(ExportJob).where(ExportJob.id == job_id))
    job = result.scalar_one_or_none()
    if job is None:
        return
    if status is not None:
        job.status = status
    if error_msg is not None:
        job.error_msg = error_msg
    if zip_path is not None:
        job.zip_path = zip_path
    if file_name is not None:
        job.file_name = file_name
    if started_at is not None:
        job.started_at = started_at
    if finished_at is not None:
        job.finished_at = finished_at
    await db.commit()
