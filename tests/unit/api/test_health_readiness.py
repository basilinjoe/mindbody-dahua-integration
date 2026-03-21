from __future__ import annotations

from app.api.health import _check_dahua_devices, _check_database


class TestCheckDatabase:
    def test_success(self) -> None:
        class FakeSession:
            def execute(self, _):
                pass

            def close(self):
                pass

        def factory():
            return FakeSession()

        assert _check_database(factory) is True

    def test_failure(self) -> None:
        class FakeSession:
            def execute(self, _):
                raise RuntimeError("db down")

            def close(self):
                pass

        def factory():
            return FakeSession()

        assert _check_database(factory) is False

    def test_factory_raises(self) -> None:
        def factory():
            raise RuntimeError("cannot connect")

        assert _check_database(factory) is False


class TestCheckDahuaDevices:
    async def test_no_clients(self) -> None:
        class FakeEngine:
            def get_dahua_clients(self):
                return []

        result = await _check_dahua_devices(FakeEngine())
        assert result is False

    async def test_health_check_passes(self) -> None:
        class FakeClient:
            async def health_check(self):
                return True

        class FakeEngine:
            def get_dahua_clients(self):
                return [FakeClient()]

        result = await _check_dahua_devices(FakeEngine())
        assert result is True

    async def test_health_check_fails(self) -> None:
        class FakeClient:
            async def health_check(self):
                return False

        class FakeEngine:
            def get_dahua_clients(self):
                return [FakeClient()]

        result = await _check_dahua_devices(FakeEngine())
        assert result is False

    async def test_exception_returns_false(self) -> None:
        class FakeEngine:
            def get_dahua_clients(self):
                raise RuntimeError("error")

        result = await _check_dahua_devices(FakeEngine())
        assert result is False
