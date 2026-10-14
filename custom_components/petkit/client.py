"""Petkit API client with shared session-expiry recovery."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from pypetkitapi import PetKitClient, PetkitSessionExpiredError

from .const import LOGGER

_ResultT = TypeVar("_ResultT")


class PetkitApiClient(PetKitClient):
    """Retry one API operation after a single session refresh."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the client and its single-flight login guard."""
        super().__init__(*args, **kwargs)
        self._session_refresh_lock = asyncio.Lock()
        self._session_generation = 0

    async def async_call_with_session_recovery(
        self,
        operation: Callable[..., Awaitable[_ResultT]],
        *args: Any,
        **kwargs: Any,
    ) -> _ResultT:
        """Retry once after re-authenticating an expired Petkit session."""
        generation = self._session_generation
        try:
            return await operation(*args, **kwargs)
        except PetkitSessionExpiredError:
            async with self._session_refresh_lock:
                if generation == self._session_generation:
                    LOGGER.warning(
                        "Petkit session expired; attempting one automatic re-login"
                    )
                    await self.login()
                    self._session_generation += 1

            return await operation(*args, **kwargs)

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
