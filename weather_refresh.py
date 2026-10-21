"""Asynchronous bounded-timeout weather refresh with a last-good cache."""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from typing import Awaitable, Callable

from streaming_hazard_grid import StreamingHazardGrid, WeatherObservation


WeatherProvider = Callable[[], Awaitable[WeatherObservation]]


@dataclass(frozen=True, slots=True)
class WeatherSnapshot:
    observation: WeatherObservation | None
    state: str  # fresh, stale, or unavailable
    age_seconds: float | None
    last_refresh_error: str | None


class AsyncWeatherRefresh:
    """Poll an injected asynchronous provider and update the grid weather cache.

    The provider owns endpoint/configuration details; this class performs no
    hard-coded HTTP requests. `asyncio.wait` bounds waiting even when a provider
    does not promptly finish cancellation after its timeout.
    """

    def __init__(
        self,
        grid: StreamingHazardGrid,
        provider: WeatherProvider,
        *,
        refresh_interval_seconds: float = 30.0,
        request_timeout_seconds: float = 3.0,
        max_age_seconds: float | None = None,
        clock: Callable[[], float] = time.time,
    ):
        self.grid = grid
        self.provider = provider
        self.refresh_interval_seconds = refresh_interval_seconds
        self.request_timeout_seconds = request_timeout_seconds
        self.max_age_seconds = grid.config.weather_max_age_seconds if max_age_seconds is None else max_age_seconds
        for name, value in (
            ("refresh_interval_seconds", refresh_interval_seconds),
            ("request_timeout_seconds", request_timeout_seconds),
            ("max_age_seconds", self.max_age_seconds),
        ):
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive and finite")
        self.clock = clock
        self._cached: WeatherObservation | None = None
        self._last_refresh_error: str | None = None
        self._task: asyncio.Task | None = None
        self._stop_event: asyncio.Event | None = None

    async def refresh_once(self) -> WeatherSnapshot:
        task = asyncio.create_task(self.provider())
        _, pending = await asyncio.wait({task}, timeout=self.request_timeout_seconds)
        if pending:
            task.cancel()
            task.add_done_callback(_consume_task_result)
            self._last_refresh_error = f"TimeoutError after {self.request_timeout_seconds:g}s"
            return self.snapshot()
        try:
            observation = task.result()
            if not isinstance(observation, WeatherObservation):
                raise TypeError("weather provider must return WeatherObservation")
            if self._cached is not None and observation.timestamp < self._cached.timestamp:
                raise ValueError("weather provider returned an older observation")
            if not self.grid.update_weather(observation):
                raise ValueError("grid rejected an older weather observation")
        except Exception as exc:
            self._last_refresh_error = f"{type(exc).__name__}: {exc}"
            return self.snapshot()
        self._cached = observation
        self._last_refresh_error = None
        return self.snapshot()

    def snapshot(self, *, now: float | None = None) -> WeatherSnapshot:
        now = self.clock() if now is None else now
        if self._cached is None:
            return WeatherSnapshot(None, "unavailable", None, self._last_refresh_error)
        age = max(0.0, now - self._cached.timestamp)
        state = "fresh" if age <= self.max_age_seconds else "stale"
        return WeatherSnapshot(self._cached, state, age, self._last_refresh_error)

    def start(self) -> asyncio.Task:
        if self._task is not None and not self._task.done():
            return self._task
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(self._run(), name="weather-refresh")
        return self._task

    async def stop(self) -> None:
        if self._stop_event is not None:
            self._stop_event.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def _run(self) -> None:
        assert self._stop_event is not None
        while not self._stop_event.is_set():
            await self.refresh_once()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.refresh_interval_seconds)
            except asyncio.TimeoutError:
                pass


def _consume_task_result(task: asyncio.Task) -> None:
    try:
        task.result()
    except (asyncio.CancelledError, Exception):
        pass

