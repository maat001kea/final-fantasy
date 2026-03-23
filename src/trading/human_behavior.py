"""Human-like browser interaction helpers – pure functions, no side effects."""

from __future__ import annotations

import asyncio
import math
import random
from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class Point:
    """A 2D screen coordinate."""

    x: float
    y: float


_TERMINAL_MICRO_CORRECTION_DISTANCE_PX = 80.0
_TERMINAL_MICRO_CORRECTION_MIN_POINTS = 2
_TERMINAL_MICRO_CORRECTION_MAX_POINTS = 3
_TERMINAL_MICRO_CORRECTION_MAX_OFFSET_PX = 1.0


def _ease_in_out_progress(t: float) -> float:
    """Remap linear progress to a smooth ease-in/ease-out profile."""
    if t <= 0.0:
        return 0.0
    if t >= 1.0:
        return 1.0
    numerator = t * t
    denominator = numerator + ((1.0 - t) * (1.0 - t))
    if denominator <= 0.0:
        return t
    return numerator / denominator


def _bezier_curve(p0: Point, p1: Point, p2: Point, p3: Point, num_points: int = 20) -> list[Point]:
    """Generate cubic Bézier curve points with eased timing between p0 and p3."""
    points: list[Point] = []
    for i in range(num_points):
        t_linear = i / max(num_points - 1, 1)
        t = _ease_in_out_progress(t_linear)
        mt = 1.0 - t
        x = (
            mt ** 3 * p0.x
            + 3 * mt ** 2 * t * p1.x
            + 3 * mt * t ** 2 * p2.x
            + t ** 3 * p3.x
        )
        y = (
            mt ** 3 * p0.y
            + 3 * mt ** 2 * t * p1.y
            + 3 * mt * t ** 2 * p2.y
            + t ** 3 * p3.y
        )
        points.append(Point(x=x, y=y))
    return points


def _append_terminal_micro_corrections(
    path: Sequence[Point],
    *,
    end: Point,
    travel_distance: float,
) -> list[Point]:
    """Add 2-3 tiny near-target tremor points at the end of long movements."""
    if travel_distance < _TERMINAL_MICRO_CORRECTION_DISTANCE_PX or len(path) < 3:
        return list(path)

    correction_points = random.randint(
        _TERMINAL_MICRO_CORRECTION_MIN_POINTS,
        _TERMINAL_MICRO_CORRECTION_MAX_POINTS,
    )
    micro_points: list[Point] = []
    for idx in range(correction_points):
        decay = max(0.35, 1.0 - (idx * 0.25))
        micro_points.append(
            Point(
                x=end.x
                + random.uniform(
                    -_TERMINAL_MICRO_CORRECTION_MAX_OFFSET_PX,
                    _TERMINAL_MICRO_CORRECTION_MAX_OFFSET_PX,
                )
                * decay,
                y=end.y
                + random.uniform(
                    -_TERMINAL_MICRO_CORRECTION_MAX_OFFSET_PX,
                    _TERMINAL_MICRO_CORRECTION_MAX_OFFSET_PX,
                )
                * decay,
            )
        )
    return list(path[:-1]) + micro_points + [path[-1]]


def generate_human_mouse_path(
    start: Point,
    end: Point,
    num_points: int = 20,
    jitter_px: float = 3.0,
) -> list[Point]:
    """Generate a human-like mouse path with eased timing, jitter, and terminal tremor."""
    dx = end.x - start.x
    dy = end.y - start.y
    distance = math.hypot(dx, dy)

    # Randomise control points relative to start/end
    cp1 = Point(
        x=start.x + dx * 0.25 + random.uniform(-abs(dx) * 0.1 - 5, abs(dx) * 0.1 + 5),
        y=start.y + dy * 0.1 + random.uniform(-abs(dy) * 0.1 - 5, abs(dy) * 0.1 + 5),
    )
    cp2 = Point(
        x=start.x + dx * 0.75 + random.uniform(-abs(dx) * 0.1 - 5, abs(dx) * 0.1 + 5),
        y=start.y + dy * 0.9 + random.uniform(-abs(dy) * 0.1 - 5, abs(dy) * 0.1 + 5),
    )

    curve = _bezier_curve(start, cp1, cp2, end, num_points=num_points)

    # Add per-point jitter (except start and end)
    result: list[Point] = [curve[0]]
    for pt in curve[1:-1]:
        result.append(
            Point(
                x=pt.x + random.uniform(-jitter_px, jitter_px),
                y=pt.y + random.uniform(-jitter_px, jitter_px),
            )
        )
    result.append(curve[-1])
    return _append_terminal_micro_corrections(result, end=end, travel_distance=distance)


def human_typing_delays(text: str, base_wpm: float = 60.0, variance: float = 0.3) -> list[float]:
    """Return per-character typing delays in seconds.

    Models realistic typing speed with per-character variance.
    """
    if not text:
        return []
    # Base delay per character (60 WPM ≈ 5 chars/word → 200 ms/char)
    base_delay = 60.0 / (base_wpm * 5.0)
    delays: list[float] = []
    for _ in text:
        jitter = random.uniform(-variance * base_delay, variance * base_delay)
        delay = max(0.02, base_delay + jitter)
        delays.append(delay)
    return delays


def random_click_offset(width: float, height: float, margin_pct: float = 0.2) -> Point:
    """Return a random click offset within an element, avoiding exact centre.

    margin_pct controls the minimum distance from the element edge.
    The offset is relative to the element's top-left corner.
    """
    margin_x = width * margin_pct
    margin_y = height * margin_pct
    x = random.uniform(margin_x, width - margin_x)
    y = random.uniform(margin_y, height - margin_y)
    return Point(x=x, y=y)


async def human_pause(min_seconds: float = 0.3, max_seconds: float = 1.2) -> None:
    """Async pause with random duration to simulate human hesitation."""
    delay = random.uniform(min_seconds, max_seconds)
    await asyncio.sleep(delay)
