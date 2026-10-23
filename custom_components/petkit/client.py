"""Petkit API client with shared session-expiry recovery."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from pypetkitapi import IotInfo, LiveFeed, PetKitClient, PetkitSessionExpiredError

from .const import LOGGER

_ResultT = TypeVar("_ResultT")


class PetkitApiClient(PetKitClient):
    """Retry one API operation after a single session refresh."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the client and track successful logins."""
        super().__init__(*args, **kwargs)
        self._session_generation = 0

    async def login(self, valid_code: str | None = None) -> None:
        """Track successful logins, including those initiated by the library."""
        try:
            await super().login(valid_code)
        except Exception as exception:
            LOGGER.error("Petkit login failed (%s)", type(exception).__name__)
            raise
        self._session_generation += 1

    async def _safe_gather(self, tasks: list[Any], label: str) -> bool:
        """Let session expiry escape the library's concurrent-task collector."""
        if not tasks:
            return True

        results = await asyncio.gather(*tasks, return_exceptions=True)
        has_errors = False
        for result in results:
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, PetkitSessionExpiredError):
                raise result
            if isinstance(result, Exception):
                has_errors = True
                LOGGER.error("Petkit task %s failed (%s)", label, type(result).__name__)
        return not has_errors

    async def async_call_with_session_recovery(
        self,
        operation: Callable[..., Awaitable[_ResultT]],
        *args: Any,
        **kwargs: Any,
    ) -> _ResultT:
        """Retry once after re-authenticating an expired Petkit session."""
        generation = self._session_generation
        operation_name = getattr(operation, "__name__", "operation")
        try:
            result = await operation(*args, **kwargs)
        except PetkitSessionExpiredError:
            relogin_performed = False
            async with self._login_lock:
                needs_login = (
                    self._session is None
                    or generation == self._session_generation
                )
                if needs_login:
                    await self.login()
                    relogin_performed = True

            try:
                result = await operation(*args, **kwargs)
            except Exception as exception:
                refresh_source = "new login" if relogin_performed else "shared login"
                LOGGER.error(
                    "Petkit session recovery failed for %s after %s (%s)",
                    operation_name,
                    refresh_source,
                    type(exception).__name__,
                )
                raise

            refresh_source = "new login" if relogin_performed else "shared login"
            LOGGER.info(
                "Petkit session recovery succeeded for %s after %s",
                operation_name,
                refresh_source,
            )
            return result
        else:
            if generation != self._session_generation:
                LOGGER.info(
                    "Petkit session refreshed while %s completed successfully",
                    operation_name,
                )
            return result

    async def get_devices_data(self, device_id: int | None = None) -> None:
        """Fetch device data with shared session recovery."""
        await self.async_call_with_session_recovery(
            super().get_devices_data,
            device_id,
        )

    async def get_iot_mqtt_config(self) -> IotInfo:
        """Fetch MQTT credentials with shared session recovery."""
        return await self.async_call_with_session_recovery(
            super().get_iot_mqtt_config
        )

    async def get_live_feed(self, device_id: int) -> LiveFeed | None:
        """Fetch the camera feed with shared session recovery."""
        return await self.async_call_with_session_recovery(
            super().get_live_feed,
            device_id,
        )

    async def send_api_request(
        self,
        device_id: int,
        action: Any,
        setting: dict | int | None = None,
    ) -> bool:
        """Apply the shared session recovery to device control commands."""
        return await self.async_call_with_session_recovery(
            super().send_api_request,
            device_id,
            action,
            setting,
        )
