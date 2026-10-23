"""Tests for Petkit client session recovery without Home Assistant dependencies."""

from __future__ import annotations

import asyncio
from collections import Counter
import importlib.util
import logging
from pathlib import Path
import sys
from types import ModuleType

import pytest


class _SessionExpired(Exception):
    """Fake library session-expired error."""


class _FakePetKitClient:
    """Small pypetkitapi stand-in to exercise the integration client."""

    def __init__(self, *args, **kwargs) -> None:
        self._session = object()
        self._login_lock = asyncio.Lock()
        self.calls: Counter[str] = Counter()
        self.login_calls = 0
        self.fail_login = False
        self.fail_retry = False

    async def login(self, valid_code: str | None = None) -> None:
        self.login_calls += 1
        await asyncio.sleep(0)
        if self.fail_login:
            raise RuntimeError("credential-secret")
        self._session = object()

    async def _run(self, name: str) -> str:
        self.calls[name] += 1
        attempt = self.calls[name]
        if attempt == 1:
            self._session = None
            if name == "get_devices_data":

                async def expired_task() -> None:
                    raise _SessionExpired

                await self._safe_gather([expired_task()], "main_tasks")
            raise _SessionExpired
        if self.fail_retry:
            raise TimeoutError("credential-secret")
        return name

    async def get_devices_data(self, device_id: int | None = None) -> None:
        await self._run("get_devices_data")

    async def get_iot_mqtt_config(self) -> object:
        return await self._run("get_iot_mqtt_config")

    async def get_live_feed(self, device_id: int) -> object:
        return await self._run("get_live_feed")

    async def send_api_request(self, device_id, action, setting=None) -> bool:
        await self._run("send_api_request")
        return True


def _load_client_class():
    """Load client.py against isolated pypetkitapi and logger stubs."""
    package_name = "_petkit_client_test"
    package = ModuleType(package_name)
    package.__path__ = []
    const = ModuleType(f"{package_name}.const")
    const.LOGGER = logging.getLogger(package_name)

    api = ModuleType("pypetkitapi")
    api.IotInfo = type("IotInfo", (), {})
    api.LiveFeed = type("LiveFeed", (), {})
    api.PetKitClient = _FakePetKitClient
    api.PetkitSessionExpiredError = _SessionExpired

    previous_api = sys.modules.get("pypetkitapi")
    sys.modules[package_name] = package
    sys.modules[f"{package_name}.const"] = const
    sys.modules["pypetkitapi"] = api
    try:
        source = (
            Path(__file__).resolve().parents[1]
            / "custom_components"
            / "petkit"
            / "client.py"
        )
        spec = importlib.util.spec_from_file_location(
            f"{package_name}.client", source
        )
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module.PetkitApiClient
    finally:
        for name in (f"{package_name}.client", f"{package_name}.const", package_name):
            sys.modules.pop(name, None)
        if previous_api is None:
            sys.modules.pop("pypetkitapi", None)
        else:
            sys.modules["pypetkitapi"] = previous_api


PetkitApiClient = _load_client_class()


def test_authenticated_api_methods_share_one_recovery_path():
    """All session-bound public methods use one login-and-retry implementation."""

    async def exercise() -> None:
        cases = (
            ("get_devices_data", lambda client: client.get_devices_data()),
            ("get_iot_mqtt_config", lambda client: client.get_iot_mqtt_config()),
            ("get_live_feed", lambda client: client.get_live_feed(42)),
            (
                "send_api_request",
                lambda client: client.send_api_request(42, "action"),
            ),
        )
        for method_name, call in cases:
            client = PetkitApiClient()
            await call(client)
            assert client.calls[method_name] == 2
            assert client.login_calls == 1

    asyncio.run(exercise())


def test_concurrent_expiry_uses_one_login():
    """Concurrent expired requests share the successful session refresh."""

    async def exercise() -> None:
        client = PetkitApiClient()
        attempts: Counter[str] = Counter()

        async def operation(name: str) -> str:
            attempts[name] += 1
            if attempts[name] == 1:
                await asyncio.sleep(0.01)
                client._session = None  # noqa: SLF001
                raise _SessionExpired
            return name

        results = await asyncio.gather(
            client.async_call_with_session_recovery(operation, "first"),
            client.async_call_with_session_recovery(operation, "second"),
        )

        assert results == ["first", "second"]
        assert client.login_calls == 1

    asyncio.run(exercise())


def test_successful_library_login_is_logged_once(caplog):
    """A login performed inside a library fallback is visible in integration logs."""

    async def exercise() -> str:
        client = PetkitApiClient()

        async def operation() -> str:
            await client.login()
            return "ok"

        return await client.async_call_with_session_recovery(operation)

    caplog.set_level(logging.INFO, logger="_petkit_client_test")
    assert asyncio.run(exercise()) == "ok"
    assert "session refreshed while operation completed successfully" in caplog.text
    assert "Petkit login failed" not in caplog.text


def test_successful_session_recovery_does_not_log_login_failure(caplog):
    """Successful re-login and retry must not also produce a failure log."""

    async def exercise() -> None:
        await PetkitApiClient().get_live_feed(42)

    caplog.set_level(logging.INFO, logger="_petkit_client_test")
    asyncio.run(exercise())

    assert "Petkit session recovery succeeded" in caplog.text
    assert "Petkit login failed" not in caplog.text
    assert "Petkit session recovery failed" not in caplog.text


def test_non_session_failure_is_not_retried():
    """Only the library's explicit expired-session error triggers login recovery."""

    async def exercise() -> None:
        client = PetkitApiClient()
        calls = 0

        async def operation() -> None:
            nonlocal calls
            calls += 1
            raise TimeoutError

        with pytest.raises(TimeoutError):
            await client.async_call_with_session_recovery(operation)

        assert calls == 1
        assert client.login_calls == 0

    asyncio.run(exercise())


def test_login_failure_does_not_log_exception_contents_or_retry(caplog):
    """Login failures expose the exception type, not its potentially sensitive text."""
    client = PetkitApiClient()
    client.fail_login = True
    caplog.set_level(logging.ERROR, logger="_petkit_client_test")

    with pytest.raises(RuntimeError, match="credential-secret"):
        asyncio.run(client.get_live_feed(42))

    assert client.calls["get_live_feed"] == 1
    assert "Petkit login failed" in caplog.text
    assert "RuntimeError" in caplog.text
    assert "credential-secret" not in caplog.text


def test_retry_is_limited_to_once_and_reports_failure_type_only(caplog):
    """A failed retry is logged once with a sanitized exception type."""
    client = PetkitApiClient()
    client.fail_retry = True
    caplog.set_level(logging.ERROR, logger="_petkit_client_test")

    with pytest.raises(TimeoutError, match="credential-secret"):
        asyncio.run(client.get_live_feed(42))

    assert client.calls["get_live_feed"] == 2
    assert client.login_calls == 1
    assert "after new login (TimeoutError)" in caplog.text
    assert "credential-secret" not in caplog.text
