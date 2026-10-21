import asyncio
import unittest

from streaming_hazard_grid import GridConfig, StreamingHazardGrid, WeatherObservation
from weather_refresh import AsyncWeatherRefresh


class AsyncWeatherRefreshTests(unittest.IsolatedAsyncioTestCase):
    async def test_provider_failure_without_cache_is_unavailable(self):
        async def provider():
            raise OSError("weather endpoint unavailable")

        service = AsyncWeatherRefresh(StreamingHazardGrid(GridConfig(0, 0)), provider, clock=lambda: 100.0)
        snapshot = await service.refresh_once()
        self.assertEqual(snapshot.state, "unavailable")
        self.assertIsNone(snapshot.observation)
        self.assertIn("OSError", snapshot.last_refresh_error)

    async def test_timeout_is_bounded_and_reported(self):
        async def provider():
            await asyncio.sleep(1.0)
            return WeatherObservation(100, 20.0, 2.0, 90.0)

        service = AsyncWeatherRefresh(
            StreamingHazardGrid(GridConfig(0, 0)), provider,
            request_timeout_seconds=0.01, clock=lambda: 100.0,
        )
        snapshot = await service.refresh_once()
        self.assertEqual(snapshot.state, "unavailable")
        self.assertIn("TimeoutError", snapshot.last_refresh_error)

    async def test_last_good_weather_is_cached_and_becomes_stale_after_failure(self):
        clock = [100.0]
        calls = [0]

        async def provider():
            calls[0] += 1
            if calls[0] == 1:
                return WeatherObservation(100, 22.0, 3.0, 180.0)
            raise ConnectionError("temporary failure")

        grid = StreamingHazardGrid(GridConfig(0, 0, weather_max_age_seconds=5))
        service = AsyncWeatherRefresh(grid, provider, max_age_seconds=5, clock=lambda: clock[0])
        fresh = await service.refresh_once()
        self.assertEqual(fresh.state, "fresh")
        failed = await service.refresh_once()
        self.assertEqual(failed.state, "fresh")
        self.assertEqual(failed.observation.temperature_c, 22.0)
        self.assertIn("ConnectionError", failed.last_refresh_error)
        clock[0] = 106.0
        stale = service.snapshot()
        self.assertEqual(stale.state, "stale")
        self.assertEqual(stale.observation.wind_speed_mps, 3.0)

    async def test_successful_refresh_updates_grid_weather_cache(self):
        async def provider():
            return WeatherObservation(100, 19.0, 1.5, 270.0)

        grid = StreamingHazardGrid(GridConfig(0, 0))
        service = AsyncWeatherRefresh(grid, provider, clock=lambda: 100.0)
        await service.refresh_once()
        features = grid.build_features((0, 0), now=100)
        self.assertEqual(features["weather_temperature_c"], 19.0)
        self.assertEqual(features["weather_wind_speed_mps"], 1.5)


if __name__ == "__main__":
    unittest.main()

