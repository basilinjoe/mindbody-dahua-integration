from __future__ import annotations

from datetime import datetime

import pytest
from pydantic import ValidationError

from app.schemas.device import DahuaDeviceCreate, DahuaDeviceRead, DahuaDeviceUpdate


class TestDahuaDeviceCreate:
    def test_defaults(self) -> None:
        d = DahuaDeviceCreate(name="Gate", host="192.168.1.1", password="secret")
        assert d.port == 80
        assert d.username == "admin"
        assert d.door_ids == "0"
        assert d.is_enabled is True
        assert d.gate_type == "all"
        assert d.enable_integration is True

    def test_strip_host(self) -> None:
        d = DahuaDeviceCreate(name="G", host="  10.0.0.1  ", password="p")
        assert d.host == "10.0.0.1"

    def test_strip_door_ids(self) -> None:
        d = DahuaDeviceCreate(name="G", host="10.0.0.1", password="p", door_ids="  0,1  ")
        assert d.door_ids == "0,1"

    def test_valid_port_range(self) -> None:
        d = DahuaDeviceCreate(name="G", host="h", password="p", port=443)
        assert d.port == 443

    def test_invalid_port_zero(self) -> None:
        with pytest.raises(ValidationError, match="Port must be between"):
            DahuaDeviceCreate(name="G", host="h", password="p", port=0)

    def test_invalid_port_too_high(self) -> None:
        with pytest.raises(ValidationError, match="Port must be between"):
            DahuaDeviceCreate(name="G", host="h", password="p", port=70000)

    def test_gate_type_literal(self) -> None:
        for gt in ("male", "female", "all"):
            d = DahuaDeviceCreate(name="G", host="h", password="p", gate_type=gt)
            assert d.gate_type == gt

    def test_invalid_gate_type(self) -> None:
        with pytest.raises(ValidationError):
            DahuaDeviceCreate(name="G", host="h", password="p", gate_type="unknown")


class TestDahuaDeviceUpdate:
    def test_defaults(self) -> None:
        d = DahuaDeviceUpdate(name="Gate", host="192.168.1.1")
        assert d.password is None
        assert d.port == 80
        assert d.username == "admin"

    def test_strip_host(self) -> None:
        d = DahuaDeviceUpdate(name="G", host="  10.0.0.1  ")
        assert d.host == "10.0.0.1"

    def test_strip_door_ids(self) -> None:
        d = DahuaDeviceUpdate(name="G", host="h", door_ids="  1,2  ")
        assert d.door_ids == "1,2"

    def test_invalid_port(self) -> None:
        with pytest.raises(ValidationError, match="Port must be between"):
            DahuaDeviceUpdate(name="G", host="h", port=0)

    def test_gate_types(self) -> None:
        for gt in ("male", "female", "all"):
            d = DahuaDeviceUpdate(name="G", host="h", gate_type=gt)
            assert d.gate_type == gt


class TestDahuaDeviceRead:
    def test_from_attributes(self) -> None:
        class FakeDevice:
            id = 1
            name = "Gate 1"
            host = "10.0.0.1"
            port = 80
            username = "admin"
            door_ids = "0"
            gate_type = "male"
            enable_integration = True
            is_enabled = True
            status = "online"
            last_seen_at = None
            created_at = datetime(2026, 1, 1)

        d = DahuaDeviceRead.model_validate(FakeDevice())
        assert d.id == 1
        assert d.name == "Gate 1"
        assert d.status == "online"
        assert d.last_seen_at is None
