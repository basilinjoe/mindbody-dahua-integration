from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator


class DahuaDeviceCreate(BaseModel):
    name: str
    host: str
    port: int = 80
    username: str = "admin"
    password: str
    door_ids: str = "0"
    is_enabled: bool = True
    gate_type: Literal["male", "female", "all"] = "all"
    enable_integration: bool = True

    @field_validator("host", mode="before")
    @classmethod
    def strip_host(cls, v: str) -> str:
        return v.strip()

    @field_validator("port")
    @classmethod
    def valid_port(cls, v: int) -> int:
        if not (1 <= v <= 65535):
            raise ValueError("Port must be between 1 and 65535")
        return v

    @field_validator("door_ids", mode="before")
    @classmethod
    def strip_door_ids(cls, v: str) -> str:
        return v.strip()


class DahuaDeviceUpdate(BaseModel):
    name: str
    host: str
    port: int = 80
    username: str = "admin"
    password: str | None = None  # None means keep existing password
    door_ids: str = "0"
    is_enabled: bool = True
    gate_type: Literal["male", "female", "all"] = "all"
    enable_integration: bool = True

    @field_validator("host", mode="before")
    @classmethod
    def strip_host(cls, v: str) -> str:
        return v.strip()

    @field_validator("port")
    @classmethod
    def valid_port(cls, v: int) -> int:
        if not (1 <= v <= 65535):
            raise ValueError("Port must be between 1 and 65535")
        return v

    @field_validator("door_ids", mode="before")
    @classmethod
    def strip_door_ids(cls, v: str) -> str:
        return v.strip()


class DahuaDeviceRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    host: str
    port: int
    username: str
    door_ids: str
    gate_type: str
    enable_integration: bool
    is_enabled: bool
    status: str
    last_seen_at: datetime | None = None
    created_at: datetime
