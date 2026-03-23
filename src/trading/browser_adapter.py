"""Browser-based broker adapter using Playwright."""

from __future__ import annotations

import asyncio
import random
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .broker_adapter_base import BrokerAdapter, OrderRequest, OrderResult, OrderSide, OrderStatus
from .human_behavior import generate_human_mouse_path, human_pause, human_typing_delays, random_click_offset, Point
from .platform_map import PLATFORM_REGISTRY, PlatformSelectors

try:
    from playwright.async_api import async_playwright, Browser, BrowserContext, Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:  # pragma: no cover
    PLAYWRIGHT_AVAILABLE = False


_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

_VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1440, "height": 900},
    {"width": 1366, "height": 768},
]
_NUMBER_FRAGMENT_RE = re.compile(r"[-+]?\d[\d.,\s\u00a0\u202f]*")


def _parse_numeric_token(token: str) -> float | None:
    cleaned = token.replace("\u00a0", " ").replace("\u202f", " ").replace(" ", "").strip()
    if not cleaned:
        return None

    sign = 1.0
    if cleaned.startswith("(") and cleaned.endswith(")"):
        sign = -1.0
        cleaned = cleaned[1:-1]
    if cleaned.startswith("-"):
        sign = -1.0
        cleaned = cleaned[1:]
    elif cleaned.startswith("+"):
        cleaned = cleaned[1:]

    numeric = "".join(ch for ch in cleaned if ch.isdigit() or ch in ".,")
    if not numeric or not any(ch.isdigit() for ch in numeric):
        return None

    if "," in numeric and "." in numeric:
        decimal_sep = "," if numeric.rfind(",") > numeric.rfind(".") else "."
        thousand_sep = "." if decimal_sep == "," else ","
        numeric = numeric.replace(thousand_sep, "").replace(decimal_sep, ".")
    elif "," in numeric:
        if re.search(r",\d{1,2}$", numeric):
            numeric = numeric.replace(".", "").replace(",", ".")
        else:
            numeric = numeric.replace(",", "")
    elif "." in numeric:
        if re.search(r"\.\d{1,2}$", numeric):
            numeric = numeric.replace(",", "")
        elif numeric.count(".") > 1 or re.search(r"\.\d{3}$", numeric):
            numeric = numeric.replace(".", "")

    try:
        return sign * float(numeric)
    except ValueError:
        return None


def _extract_numeric_values(text: str) -> list[float]:
    values: list[float] = []
    for token in _NUMBER_FRAGMENT_RE.findall(text or ""):
        parsed = _parse_numeric_token(token)
        if parsed is not None:
            values.append(parsed)
    return values


def _selector_candidates(selector_csv: str, extra: list[str] | None = None) -> list[str]:
    candidates = [part.strip() for part in selector_csv.split(",") if part.strip()]
    if extra:
        for item in extra:
            cleaned = item.strip()
            if cleaned and cleaned not in candidates:
                candidates.append(cleaned)
    return candidates


async def _find_first_visible_selector(
    page: Any,
    selector_csv: str,
    *,
    extra: list[str] | None = None,
    timeout_ms: int = 15000,
) -> str:
    candidates = _selector_candidates(selector_csv, extra=extra)
    if not candidates:
        raise RuntimeError("No selector candidates provided.")

    per_selector_timeout = max(1200, int(timeout_ms / max(1, len(candidates))))
    last_error: Exception | None = None
    for selector in candidates:
        try:
            await page.wait_for_selector(selector, state="visible", timeout=per_selector_timeout)
            return selector
        except Exception as exc:
            last_error = exc
            continue

    selector_text = ", ".join(candidates)
    page_url = str(getattr(page, "url", ""))
    raise RuntimeError(
        f"No visible login element matched selectors: {selector_text}. URL: {page_url}"
    ) from last_error


def _balance_text_score(text: str) -> int:
    lowered = (text or "").lower()
    score = 0
    if "balance" in lowered:
        score += 5
    if "equity" in lowered:
        score += 4
    if "account" in lowered:
        score += 2
    if "cash" in lowered:
        score += 2
    if "available" in lowered:
        score -= 1
    if "margin" in lowered:
        score -= 2
    if "pnl" in lowered or "p/l" in lowered or "profit" in lowered or "loss" in lowered:
        score -= 3
    return score


class BrowserBrokerAdapter(BrokerAdapter):
    """Browser-based broker adapter with human-like behaviour and anti-detection."""

    def __init__(
        self,
        platform: str,
        username: str,
        password: str,
        headless: bool = True,
        screenshot_dir: str | Path | None = None,
    ) -> None:
        if not PLAYWRIGHT_AVAILABLE:
            raise ImportError(
                "playwright is required for BrowserBrokerAdapter. "
                "Install it with: pip install playwright && playwright install chromium"
            )
        self.platform = platform.lower()
        self.username = username
        self.password = password
        self.headless = headless
        self.screenshot_dir = Path(screenshot_dir) if screenshot_dir else None
        self._platform_cfg = PLATFORM_REGISTRY.get(self.platform, {})
        self._selectors: PlatformSelectors | None = self._platform_cfg.get("selectors")
        self._instrument_map: dict[str, str] = self._platform_cfg.get("instrument_map", {})
        self._browser: Any = None
        self._context: Any = None
        self._page: Any = None

    async def connect(self) -> None:
        """Start browser with anti-detection, login with human behaviour."""
        pw = await async_playwright().__aenter__()
        viewport = random.choice(_VIEWPORTS)
        user_agent = random.choice(_USER_AGENTS)
        self._browser = await pw.chromium.launch(headless=self.headless)
        self._context = await self._browser.new_context(
            viewport=viewport,
            user_agent=user_agent,
            locale="en-GB",
        )
        # Anti-detection: remove webdriver flag
        await self._context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        self._page = await self._context.new_page()
        base_url = self._platform_cfg.get("base_url", "")
        if base_url:
            await self._page.goto(base_url)
            await human_pause(1.0, 2.5)
            await self._login()

    async def _login(self) -> None:
        """Perform login with human-like typing."""
        if not self._selectors or not self._page:
            return
        page = self._page
        sel = self._selectors
        username_selector = await _find_first_visible_selector(
            page,
            sel.login_username,
            extra=["#login_userid", "input[autocomplete='username']"],
        )
        await page.fill(username_selector, "")
        await page.click(username_selector)
        await human_pause(0.2, 0.6)
        for char, delay in zip(self.username, human_typing_delays(self.username)):
            await page.keyboard.type(char)
            await asyncio.sleep(delay)
        await human_pause(0.3, 0.8)
        password_selector = await _find_first_visible_selector(
            page,
            sel.login_password,
            extra=["#login_password", "input[autocomplete='current-password']"],
        )
        await page.fill(password_selector, "")
        await page.click(password_selector)
        await human_pause(0.2, 0.5)
        for char, delay in zip(self.password, human_typing_delays(self.password)):
            await page.keyboard.type(char)
            await asyncio.sleep(delay)
        await human_pause(0.4, 1.0)
        submit_selector = await _find_first_visible_selector(
            page,
            sel.login_submit,
            extra=["button:has-text('Log in')", "button:has-text('Login')"],
        )
        await page.click(submit_selector)
        try:
            await page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass
        await human_pause(2.0, 4.0)

        # Basic login confirmation: if username field remains visible, login likely failed.
        try:
            if await page.locator(username_selector).first.is_visible():
                raise RuntimeError(f"Login not confirmed for {self.platform}: username field still visible.")
        except RuntimeError:
            raise
        except Exception:
            pass

    async def place_order(self, request: OrderRequest) -> OrderResult:
        """Navigate to instrument, fill order form, confirm, take screenshot."""
        if not self._page or not self._selectors:
            return OrderResult(
                order_id="",
                status=OrderStatus.ERROR,
                instrument=request.instrument,
                side=request.side,
                quantity=request.quantity,
                fill_price=None,
                error_message="Browser not connected",
                raw_response={},
            )
        page = self._page
        sel = self._selectors
        instrument_name = self._instrument_map.get(request.instrument, request.instrument)
        try:
            # Fill quantity
            await page.wait_for_selector(sel.order_quantity, timeout=10000)
            await page.fill(sel.order_quantity, str(request.quantity))
            await human_pause(0.3, 0.8)

            # Set stop loss if provided.
            # CRITICAL: swallow NEVER. A stop-fill failure means we would send a
            # naked order with unlimited downside — abort instead.
            if request.stop_price is not None:
                try:
                    await page.fill(sel.order_stop_field, str(request.stop_price))
                    await human_pause(0.2, 0.5)
                except Exception as stop_exc:
                    raise RuntimeError(
                        f"Stop-price felt kunne ikke udfyldes ({request.stop_price}): {stop_exc}. "
                        f"Ordre afbrudt – ubeskyttet position undgået."
                    ) from stop_exc

            # Click buy or sell
            if request.side == OrderSide.BUY:
                await page.click(sel.order_buy_button)
            else:
                await page.click(sel.order_sell_button)
            await human_pause(0.5, 1.5)

            # Confirm
            await page.click(sel.order_confirm)
            await human_pause(1.0, 2.0)

            # Take screenshot
            if self.screenshot_dir:
                self.screenshot_dir.mkdir(parents=True, exist_ok=True)
                ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
                await page.screenshot(
                    path=str(self.screenshot_dir / f"order_{ts}_{request.idempotency_key[:8]}.png")
                )

            return OrderResult(
                order_id=request.idempotency_key,
                status=OrderStatus.FILLED,
                instrument=request.instrument,
                side=request.side,
                quantity=request.quantity,
                fill_price=request.entry_price,
                error_message=None,
                raw_response={"instrument_name": instrument_name},
            )
        except Exception as exc:
            return OrderResult(
                order_id="",
                status=OrderStatus.ERROR,
                instrument=request.instrument,
                side=request.side,
                quantity=request.quantity,
                fill_price=None,
                error_message=str(exc),
                raw_response={},
            )

    async def get_account_balance(self) -> float:
        """Read equity/balance from the platform."""
        if not self._page or not self._selectors:
            return 0.0
        selector_candidates: list[str] = []
        selector_candidates.extend(_selector_candidates(self._selectors.account_equity))
        selector_candidates.extend(_selector_candidates(self._selectors.account_balance))
        selector_candidates.extend(
            [
                "[data-field='equity']",
                "[data-field='balance']",
                "[class*='equity']",
                "[class*='balance']",
            ]
        )

        unique_selectors: list[str] = []
        seen: set[str] = set()
        for selector in selector_candidates:
            if selector and selector not in seen:
                seen.add(selector)
                unique_selectors.append(selector)

        weighted_candidates: list[tuple[int, float]] = []
        fallback_value: float | None = None
        for selector in unique_selectors:
            try:
                locator = self._page.locator(selector)
                count = await locator.count()
                for idx in range(min(count, 30)):
                    node = locator.nth(idx)
                    try:
                        if not await node.is_visible():
                            continue
                        raw_text = await node.inner_text()
                    except Exception:
                        continue
                    values = _extract_numeric_values(raw_text)
                    if not values:
                        continue
                    score = _balance_text_score(raw_text)
                    for value in values:
                        if value > 0:
                            weighted_candidates.append((score, float(value)))
                        elif fallback_value is None:
                            fallback_value = float(value)
            except Exception:
                continue

        # Fallback scan in whole page text for lines containing account/balance keywords.
        if not weighted_candidates:
            try:
                body_text = await self._page.inner_text("body")
                for line in body_text.splitlines():
                    if not line.strip():
                        continue
                    score = _balance_text_score(line)
                    if score <= 0:
                        continue
                    values = _extract_numeric_values(line)
                    for value in values:
                        if value > 0:
                            weighted_candidates.append((score, float(value)))
                        elif fallback_value is None:
                            fallback_value = float(value)
            except Exception:
                pass

        if weighted_candidates:
            # Prefer highest score (balance/equity context), then highest positive value.
            weighted_candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
            return float(weighted_candidates[0][1])
        return float(fallback_value) if fallback_value is not None else 0.0

    async def get_open_positions(self) -> list[dict[str, Any]]:
        """Read open positions from the platform."""
        if not self._page or not self._selectors:
            return []
        try:
            rows = await self._page.query_selector_all(
                f"{self._selectors.positions_table} tr"
            )
            positions = []
            for row in rows:
                text = await row.inner_text()
                positions.append({"raw": text})
            return positions
        except Exception:
            return []

    async def get_position(self, instrument: str) -> dict[str, Any] | None:
        """Get position for a specific instrument."""
        positions = await self.get_open_positions()
        instrument_name = self._instrument_map.get(instrument, instrument)
        for pos in positions:
            if instrument_name in str(pos.get("raw", "")):
                return pos
        return None

    async def close_all_positions(self) -> None:
        """Emergency flatten – close all open positions."""
        if not self._page or not self._selectors:
            return
        try:
            buttons = await self._page.query_selector_all(
                self._selectors.position_close_button
            )
            for btn in buttons:
                await btn.click()
                await human_pause(0.5, 1.0)
        except Exception:
            pass

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order (not directly supported via browser)."""
        return False

    async def disconnect(self) -> None:
        """Close browser."""
        if self._browser:
            await self._browser.close()
            self._browser = None
            self._context = None
            self._page = None
