"""CDP Direct adapter with human-like cursor for Tradovate.

This adapter uses Chrome DevTools Protocol directly for maximum stealth
and human-like browser automation with visible cursor overlay.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import os
import random
import re
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, TypeVar
from urllib.parse import urlparse

from .broker_adapter_base import BrokerAdapter, OrderRequest, OrderResult, OrderSide, OrderStatus
from .human_behavior import (
    generate_human_mouse_path,
    human_pause,
    human_typing_delays,
    random_click_offset,
    Point,
)
from .platform_map import PLATFORM_REGISTRY, PlatformSelectors
from .cursor_overlay import CURSOR_CLICK_FN, CURSOR_EL_KEY, CURSOR_POS_KEY, get_full_injection_js

try:
    import websockets
    from websockets.exceptions import ConnectionClosed
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False
    ConnectionClosed = Exception

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False


_LOGGER = logging.getLogger(__name__)

# Chrome remote debugging port
_CDP_PORT_ENV_VAR = "CDP_PORT"
_DEFAULT_CDP_PORT = 9255


def _env_cdp_port() -> int:
    raw = os.getenv(_CDP_PORT_ENV_VAR, str(_DEFAULT_CDP_PORT))
    try:
        port = int(raw)
    except (TypeError, ValueError):
        return _DEFAULT_CDP_PORT
    return port if 1024 <= port <= 65535 else _DEFAULT_CDP_PORT


CDP_PORT = _env_cdp_port()


def resolve_cdp_port(raw: Any | None = None) -> int:
    if raw not in (None, ""):
        try:
            port = int(raw)
        except (TypeError, ValueError):
            return CDP_PORT
        return port if 1024 <= port <= 65535 else CDP_PORT
    return CDP_PORT

# Additional stealth JavaScript that clears ChromeDriver-specific fingerprint
# properties.  Injected via Page.addScriptToEvaluateOnNewDocument so it runs
# *before* the page bootstraps, making the CDP session invisible to anti-bot
# checks on platforms such as Tradovate.
_CDP_STEALTH_EXTRA_JS: str = """
(function() {
    try { Object.defineProperty(navigator, 'webdriver', {get: () => undefined}); } catch(e) {}
    window.cdc_adoQpoasnfa76pfcZLmcfl_Array = undefined;
    window.cdc_adoQpoasnfa76pfcZLmcfl_Promise = undefined;
    window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol = undefined;
})();
"""

# Human-like timing constants
MIN_CLICK_DELAY = 0.5
MAX_CLICK_DELAY = 2.0
MIN_CLICK_HOLD_SECONDS = 0.08
MAX_CLICK_HOLD_SECONDS = 0.24
CLICK_HOLD_LOGNORM_MU = -2.3
CLICK_HOLD_LOGNORM_SIGMA = 0.23
MIN_TYPING_WPM = 40
MAX_TYPING_WPM = 80
_T = TypeVar("_T")

# Parent module selector — used to scope button searches to the active chart/order panel.
# Falls back to full-document search when the chart wrapper is absent (e.g. toolbar-only layout).
_TRADOVATE_PARENT_MODULE_SELECTOR = "div.module.chart.chart-wrapper"

# Selector bundles for Tradovate button discovery.
# Priority order: most specific first (text-based XPath), then class-based fallbacks.
#
# "Buy Bid" / "Sell Ask" are LIMIT order buttons shown in the top toolbar.
# "Buy Mkt" / "Sell Mkt" are MARKET order buttons in the chart-embedded panel.
# Both sets share the same btn-success / btn-danger Bootstrap classes, so we
# use text content to target the correct one first, then fall back to class.
#
# The parent-module restriction is intentionally bypassed for toolbar buttons by
# searching the full document when the chart wrapper is absent (see inspect logic).
_TRADOVATE_SELECTOR_BUNDLES: dict[str, tuple[str, ...]] = {
    # Confirmed live Tradovate CSS classes (user-verified):
    #   BUY / ADD-TO-BUY  → div.btn.btn-success
    #   SELL / ADD-TO-SELL → div.btn.btn-danger
    #   EXIT (Flatten)     → button.btn.btn-default  or  div.btn-split > button.btn-default
    #   CONTRACT SIZE      → input.form-control
    #
    # CSS classes are PRIMARY — simple, reliable, tested in prod.
    # XPath text-selectors are FALLBACK only (for edge cases / future UI changes).
    "buy": (
        # Primary — confirmed CSS class
        "div.btn.btn-success",
        "div.btn-success",
        # XPath fallbacks
        "//div[normalize-space(text())='Buy Bid']",
        "//button[normalize-space(text())='Buy Bid']",
        "//div[normalize-space(text())='Buy Mkt']",
    ),
    "sell": (
        # Primary — confirmed CSS class (exclude panic/flatten button)
        "div.btn.btn-danger:not(.panic-button)",
        "div.btn.btn-danger",
        # XPath fallbacks
        "//div[normalize-space(text())='Sell Ask']",
        "//button[normalize-space(text())='Sell Ask']",
        "//div[normalize-space(text())='Sell Mkt']",
    ),
    "exit": (
        # Primary — confirmed CSS class
        "button.btn.btn-default:not(.dropdown-toggle)",
        "div.btn-split button.btn-default",
        # XPath fallbacks
        "//button[contains(normalize-space(text()), 'Exit at Mkt')]",
        "//button[normalize-space(text())='Flatten']",
    ),
    # Add-to-winners clicks the exact same button as entry (confirmed by user).
    # Signal type (add vs entry) is set by the observer, not the selector.
    "add": (
        "div.btn.btn-success",
        "div.btn-success",
        "//div[normalize-space(text())='Buy Bid']",
        "//div[normalize-space(text())='Buy Mkt']",
    ),
}

_TRADOVATE_SELECTOR_HEALTH_MAP_BASE: dict[str, dict[str, Any]] = {
    "buy_button": {
        "label": "Buy Button",
        "kind": "entry",
        "action": "buy",
        "aria": (
            "[aria-label='Buy Market']",
            "[aria-label='Buy Mkt']",
            "[title='Buy Market']",
            "[title='Buy Mkt']",
        ),
        "semantic": (
            "//div[contains(normalize-space(), 'Buy Mkt')]",
            "//button[contains(normalize-space(), 'Buy Market')]",
        ),
    },
    "sell_button": {
        "label": "Sell Button",
        "kind": "entry",
        "action": "sell",
        "aria": (
            "[aria-label='Sell Market']",
            "[aria-label='Sell Mkt']",
            "[title='Sell Market']",
            "[title='Sell Mkt']",
        ),
        "semantic": (
            "//div[contains(normalize-space(), 'Sell Mkt')]",
            "//button[contains(normalize-space(), 'Sell Market')]",
        ),
    },
    "flat_button": {
        "label": "Flat Button",
        "kind": "exit",
        "action": "flat",
        "aria": (
            "[aria-label='Exit at Mkt & Cxl']",
            "[aria-label='Exit at Market and Cancel']",
            "[title='Exit at Mkt & Cxl']",
            "[title='Flatten']",
        ),
        "semantic": (
            "//button[contains(normalize-space(), 'Exit at Mkt')]",
            "//button[contains(normalize-space(), 'Flatten')]",
            "//div[contains(normalize-space(), 'Exit at Mkt')]",
        ),
    },
    "quantity_input": {
        "label": "Quantity Input",
        "kind": "entry",
        "action": "quantity",
        # Confirmed live Tradovate DOM: <input class="form-control" placeholder="Select value">
        # primary list is overwritten at runtime from TRADOVATE_SELECTORS.order_quantity;
        # the first entry there is now input.form-control[placeholder='Select value'].
        "primary": (
            "input.form-control[placeholder='Select value']",
            ".select-input.combobox input.form-control",
        ),
        "aria": (
            "[aria-label='Quantity']",
            "[aria-label='Order Quantity']",
            "[title='Quantity']",
        ),
        "semantic": (
            "//input[@placeholder='Select value']",
            "//input[contains(@placeholder, 'Select value')]",
            "//input[contains(@aria-label, 'Quantity')]",
            "//input[contains(@title, 'Quantity')]",
        ),
    },
    "instrument_header": {
        "label": "Instrument Header",
        "kind": "entry",
        "action": "instrument",
        "primary": (
            "[class*='chart-title']",
            "[class*='symbol']",
            "[class*='instrument']",
            "[class*='header']",
        ),
        "aria": (
            "[aria-label*='Instrument']",
            "[aria-label*='Symbol']",
            "[title*='Instrument']",
            "[title*='Symbol']",
        ),
        "semantic": (),
        "interactable": False,
    },
}

_TRADOVATE_ACTION_HEALTH_KEYS: dict[str, str] = {
    "buy": "buy_button",
    "sell": "sell_button",
    "exit": "flat_button",
}


class CDPConnectionError(Exception):
    """Raised when CDP connection fails."""
    pass


def _coerce_numeric_text(value: Any) -> float | None:
    token = str(value or "").strip()
    if not token:
        return None
    token = token.replace(" ", "").replace(",", "")
    try:
        return float(token)
    except ValueError:
        return None


def _coerce_tradovate_position_qty(value: Any) -> float | None:
    token = str(value or "").strip()
    if not token:
        return None
    numeric = _coerce_numeric_text(token)
    if numeric is not None:
        return numeric
    compact = token.replace(" ", "")
    match = re.match(r"^([+-]?\d+(?:\.\d+)?)(?:@.+)?$", compact)
    if match is None:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _extract_labeled_visible_value(text: str, label: str) -> str | None:
    lines = [line.strip() for line in str(text or "").splitlines()]
    target = str(label or "").strip().upper()
    for index, line in enumerate(lines):
        if line.strip().upper() != target:
            continue
        for candidate in lines[index + 1:]:
            cleaned = candidate.strip()
            if cleaned:
                return cleaned
    return None


def _extract_visible_clock_value(text: str) -> str | None:
    lines = [line.strip() for line in str(text or "").splitlines()]
    for line in lines:
        if re.search(r"\b\d{1,2}:\d{2}:\d{2}\s*(?:AM|PM)?\s*[A-Z]{2,5}\b", line, flags=re.IGNORECASE):
            return line
    return None


def _selector_bundle_action(platform: str, selector: str) -> str | None:
    if str(platform or "").strip().lower() != "tradovate":
        return None
    normalized = str(selector or "").strip()
    if not normalized:
        return None
    for action, bundle in _TRADOVATE_SELECTOR_BUNDLES.items():
        if normalized in bundle:
            return action
    return None


def _build_tradovate_selector_health_map(
    *,
    quantity_selectors: list[str],
    expected_instrument_token: str = "",
) -> dict[str, dict[str, Any]]:
    """Build the prioritized Tradovate selector health map."""
    health_map = {
        key: {
            "label": str(payload.get("label", "") or "").strip(),
            "kind": str(payload.get("kind", "") or "").strip(),
            "action": str(payload.get("action", "") or "").strip(),
            "interactable": bool(payload.get("interactable", True)),
            "strategies": {
                "primary": list(payload.get("primary", ())),
                "aria": list(payload.get("aria", ())),
                "semantic": list(payload.get("semantic", ())),
            },
        }
        for key, payload in _TRADOVATE_SELECTOR_HEALTH_MAP_BASE.items()
    }

    health_map["buy_button"]["strategies"]["primary"] = list(_TRADOVATE_SELECTOR_BUNDLES["buy"])
    health_map["sell_button"]["strategies"]["primary"] = list(_TRADOVATE_SELECTOR_BUNDLES["sell"])
    health_map["flat_button"]["strategies"]["primary"] = list(_TRADOVATE_SELECTOR_BUNDLES["exit"])
    health_map["quantity_input"]["strategies"]["primary"] = [str(selector).strip() for selector in quantity_selectors if str(selector).strip()]

    instrument_token = str(expected_instrument_token or "").strip().upper()
    instrument_semantic = list(health_map["instrument_header"]["strategies"]["semantic"])
    if instrument_token:
        instrument_semantic.extend(
            (
                f"//*[contains(translate(normalize-space(.), 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), '{instrument_token}')]",
                f"//*[contains(translate(@title, 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), '{instrument_token}')]",
                f"//*[contains(translate(@aria-label, 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), '{instrument_token}')]",
            )
        )
    health_map["instrument_header"]["strategies"]["semantic"] = instrument_semantic
    health_map["instrument_header"]["expected_token"] = instrument_token
    return health_map


def _build_tradovate_execution_health_config(action: str) -> dict[str, Any] | None:
    action_key = str(action or "").strip().lower()
    health_key = _TRADOVATE_ACTION_HEALTH_KEYS.get(action_key)
    if not health_key:
        return None
    return _build_tradovate_selector_health_map(
        quantity_selectors=[],
        expected_instrument_token="",
    ).get(health_key)


def _human_click_hold_seconds() -> float:
    """Return a realistic random click hold duration."""
    hold_seconds = random.lognormvariate(CLICK_HOLD_LOGNORM_MU, CLICK_HOLD_LOGNORM_SIGMA)
    clamped_hold_seconds = max(MIN_CLICK_HOLD_SECONDS, min(MAX_CLICK_HOLD_SECONDS, hold_seconds))
    _LOGGER.debug("[CDPHumanAdapter] Click hold %.3fs", clamped_hold_seconds)
    return clamped_hold_seconds


def _build_human_cursor_path(
    start: Point,
    end: Point,
    *,
    num_points: int = 30,
    jitter_px: float = 2.0,
    overshoot: bool | None = None,
) -> list[Point]:
    """Build a human-like cursor path with eased timing, overshoot, and terminal tremor."""
    dx = end.x - start.x
    dy = end.y - start.y
    distance = max(1.0, (dx ** 2 + dy ** 2) ** 0.5)
    should_overshoot = bool(overshoot) if overshoot is not None else (distance >= 80.0 and random.random() < 0.7)
    if not should_overshoot:
        return generate_human_mouse_path(start, end, num_points=num_points, jitter_px=jitter_px)

    ux = dx / distance
    uy = dy / distance
    overshoot_distance = min(18.0, max(4.0, distance * 0.04))
    lateral_distance = min(6.0, max(1.5, distance * 0.012))
    perp_x = -uy
    perp_y = ux
    overshoot_point = Point(
        x=end.x + ux * overshoot_distance + perp_x * random.uniform(-lateral_distance, lateral_distance),
        y=end.y + uy * overshoot_distance + perp_y * random.uniform(-lateral_distance, lateral_distance),
    )

    first_points = max(10, int(num_points * 0.75))
    second_points = max(6, num_points - first_points + 1)
    path_out = generate_human_mouse_path(start, overshoot_point, num_points=first_points, jitter_px=jitter_px)
    path_back = generate_human_mouse_path(
        overshoot_point,
        end,
        num_points=second_points,
        jitter_px=max(0.6, jitter_px * 0.5),
    )
    return path_out[:-1] + path_back


def _extract_tradovate_broker_snapshot_from_text(
    text: str,
    *,
    instrument_token: str = "",
    order_quantity_value: str | None = None,
    expected_account_tokens: list[str] | tuple[str, ...] = (),
) -> dict[str, Any]:
    visible_text = str(text or "")
    normalized_tokens = [str(token).strip() for token in expected_account_tokens if str(token).strip()]
    instrument_root = str(instrument_token or "").strip().upper()
    account_value = _extract_labeled_visible_value(visible_text, "ACCOUNT")
    position_label_value = _extract_labeled_visible_value(visible_text, "POSITION")
    position_qty = _coerce_tradovate_position_qty(position_label_value)
    bid_label_value = _extract_labeled_visible_value(visible_text, "BID")
    ask_label_value = _extract_labeled_visible_value(visible_text, "ASK")
    last_label_value = _extract_labeled_visible_value(visible_text, "LAST")
    market_clock = _extract_visible_clock_value(visible_text)

    instrument_match = ""
    if instrument_root:
        match = re.search(rf"\b{re.escape(instrument_root)}[A-Z0-9]*\b", visible_text, flags=re.IGNORECASE)
        if match:
            instrument_match = str(match.group(0)).strip().upper()

    account_tokens_present: list[str] = []
    haystack = visible_text.casefold()
    for token in normalized_tokens:
        if token.casefold() in haystack:
            account_tokens_present.append(token)

    snapshot = {
        "account_value": str(account_value or "").strip(),
        "account_ok": not normalized_tokens or len(account_tokens_present) == len(normalized_tokens),
        "account_tokens_present": account_tokens_present,
        "instrument_root": instrument_root,
        "instrument_match": instrument_match,
        "instrument_visible": bool(instrument_match),
        "position_qty": float(position_qty or 0.0),
        "position_open": bool(position_qty is not None and abs(float(position_qty)) > 0.0),
        "position_side": (
            "long"
            if position_qty is not None and position_qty > 0
            else "short"
            if position_qty is not None and position_qty < 0
            else "flat"
        ),
        "order_quantity_value": str(order_quantity_value or "").strip(),
        "last_price_text": str(last_label_value or "").strip(),
        "last_price": _coerce_numeric_text(last_label_value),
        "bid_price_text": str(bid_label_value or "").strip(),
        "bid_price": _coerce_numeric_text(bid_label_value),
        "ask_price_text": str(ask_label_value or "").strip(),
        "ask_price": _coerce_numeric_text(ask_label_value),
        "market_clock_text": str(market_clock or "").strip(),
        "quote_ready": any(
            _coerce_numeric_text(value) is not None for value in (last_label_value, bid_label_value, ask_label_value)
        ),
        "text_sample": visible_text[:400],
        "source": "tradovate_visible_text",
    }
    bid_price = snapshot["bid_price"]
    ask_price = snapshot["ask_price"]
    snapshot["spread"] = (
        float(ask_price - bid_price)
        if bid_price is not None and ask_price is not None
        else None
    )
    return snapshot


def _score_cdp_page_target(platform: str, base_url: str, target: dict[str, Any]) -> int:
    """Rank available CDP page targets so manual-login sessions attach to the right tab.

    Scoring tiers (highest wins):
      +200..+300  ── real Tradovate trader tab
        0.. +60   ── unknown https page (neutral)
       -6 000     ── about:blank / about:newtab  (useless tabs)
       -8 000     ── chrome-extension://, chrome://, devtools://, data:, blob:
      -10 000     ── non-"page" target types (background_page, service_worker …)
    """
    if str(target.get("type", "")).strip().lower() != "page":
        return -10_000

    url = str(target.get("url", "")).strip()
    title = str(target.get("title", "")).strip()
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    scheme = parsed.scheme.lower()

    # ── Hard penalties for non-web / internal Chrome URLs ─────────────────
    # These appear as type "page" but are never tradeable browser tabs.
    if scheme in ("chrome-extension", "chrome", "devtools", "data", "blob"):
        return -8_000
    if scheme == "about" or url in ("", "about:blank", "about:newtab", "about:new-tab-page"):
        return -6_000

    score = 0

    # Tabs that still have a free webSocketDebuggerUrl are immediately usable;
    # an absent URL means another debugger session is already attached.
    if target.get("webSocketDebuggerUrl"):
        score += 5

    if base_url:
        base_host = urlparse(base_url).netloc.lower()
        if base_host and host == base_host:
            score += 50
        if url.startswith(base_url):
            score += 10

    if platform == "tradovate":
        if host == "trader.tradovate.com":
            score += 200
            if path in {"", "/"}:
                score += 80
            if "welcome" in path:
                score -= 25
            if "login" in path or "signin" in path:
                score -= 20
            if "dark default" in title.lower():
                score += 15
        elif host.endswith("tradovate.com"):
            score += 10
            if host == "www.tradovate.com":
                score -= 40

    return score


def _should_preserve_manual_cdp_page(
    *,
    platform: str,
    username: str,
    password: str,
    target: dict[str, Any],
) -> bool:
    """Keep the current page when Custom Human attaches to an already logged-in browser tab."""
    if str(username or "").strip() or str(password or "").strip():
        return False

    url = str(target.get("url", "")).strip()
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()

    if platform == "tradovate":
        return host == "trader.tradovate.com" and path not in {"", "/welcome"} and "login" not in path

    return False


class CDPHumanAdapter(BrokerAdapter):
    """CDP Direct adapter with human-like behaviour and visible cursor.
    
    Features:
    - Direct Chrome DevTools Protocol connection (no Playwright overhead)
    - Visible cursor overlay on page
    - Bézier curve mouse movements
    - Random click offsets (not pixel-perfect)
    - Human-like typing delays
    - Random pauses (0.5-2.0 seconds)
    - Anti-detection scripts
    """
    
    def __init__(
        self,
        platform: str,
        username: str,
        password: str,
        headless: bool = False,
        screenshot_dir: str | Path | None = None,
        cdp_port: int = CDP_PORT,
    ) -> None:
        if not WEBSOCKETS_AVAILABLE:
            raise ImportError(
                "websockets is required for CDPHumanAdapter. "
                "Install it with: pip install websockets"
            )
        if not AIOHTTP_AVAILABLE:
            raise ImportError(
                "aiohttp is required for CDPHumanAdapter. "
                "Install it with: pip install aiohttp"
            )
        
        self.platform = platform.lower()
        self.username = username
        self.password = password
        self.headless = headless
        self.screenshot_dir = Path(screenshot_dir) if screenshot_dir else None
        self.cdp_port = resolve_cdp_port(cdp_port)
        
        # Platform configuration
        self._platform_cfg = PLATFORM_REGISTRY.get(self.platform, {})
        self._selectors: PlatformSelectors | None = self._platform_cfg.get("selectors")
        self._instrument_map: dict[str, str] = self._platform_cfg.get("instrument_map", {})
        self._base_url = self._platform_cfg.get("base_url", "")
        
        # CDP state
        self._ws = None
        self._message_id = 0
        self._page_url = ""
        self._connected = False
        self._runner_loop: asyncio.AbstractEventLoop | None = None
        self._runner_thread: threading.Thread | None = None
        self._runner_ready = threading.Event()
        self._runner_boot_lock = threading.Lock()
        self._runner_call_lock = threading.Lock()
        self._reconnect_lock = threading.Lock()

    def _runner_main(self) -> None:
        loop = asyncio.new_event_loop()
        self._runner_loop = loop
        asyncio.set_event_loop(loop)
        self._runner_ready.set()
        try:
            loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()
            self._runner_loop = None
            self._runner_ready.clear()

    def _ensure_runner_loop(self) -> asyncio.AbstractEventLoop:
        loop = self._runner_loop
        thread = self._runner_thread
        if loop is not None and not loop.is_closed() and thread is not None and thread.is_alive():
            return loop

        with self._runner_boot_lock:
            loop = self._runner_loop
            thread = self._runner_thread
            if loop is not None and not loop.is_closed() and thread is not None and thread.is_alive():
                return loop

            self._runner_ready.clear()
            self._runner_thread = threading.Thread(
                target=self._runner_main,
                daemon=True,
                name=f"cdp-adapter-{self.platform}",
            )
            self._runner_thread.start()
            if not self._runner_ready.wait(timeout=5.0):
                raise RuntimeError("CDP adapter event loop failed to start.")

            loop = self._runner_loop
            if loop is None or loop.is_closed():
                raise RuntimeError("CDP adapter event loop is unavailable.")
            return loop

    def run_sync(self, task_factory: Callable[[], Awaitable[_T]], timeout: float | None = None) -> _T:
        """Run adapter coroutine work on a dedicated persistent event loop."""
        loop = self._ensure_runner_loop()
        with self._runner_call_lock:
            future: concurrent.futures.Future[_T] = asyncio.run_coroutine_threadsafe(task_factory(), loop)
            return future.result(timeout=timeout)

    def shutdown_sync(self, timeout: float = 5.0) -> None:
        """Best-effort sync shutdown for the adapter loop and websocket."""
        loop = self._runner_loop
        thread = self._runner_thread
        if loop is None or loop.is_closed():
            return

        try:
            if self._ws is not None or self._connected:
                future = asyncio.run_coroutine_threadsafe(self.disconnect(), loop)
                future.result(timeout=timeout)
        except Exception:
            pass

        loop.call_soon_threadsafe(loop.stop)
        if thread is not None and thread.is_alive() and threading.current_thread() is not thread:
            thread.join(timeout=timeout)
        self._runner_thread = None

    def reconnect_sync(self, timeout: float = 10.0) -> None:
        """Best-effort reconnect for long-running worker threads."""
        loop = self._ensure_runner_loop()
        with self._reconnect_lock:
            if self._connected and self._ws is not None:
                return
            if self._ws is not None:
                try:
                    future = asyncio.run_coroutine_threadsafe(self.disconnect(), loop)
                    future.result(timeout=timeout)
                except Exception:
                    self._mark_disconnected()
            future = asyncio.run_coroutine_threadsafe(self.connect(), loop)
            future.result(timeout=timeout)
    
    def _mark_disconnected(self) -> None:
        self._connected = False
        self._ws = None

    async def _send_cdp_command(self, method: str, params: dict | None = None) -> dict:
        """Send a CDP command and return the response."""
        if not self._ws:
            raise CDPConnectionError("Not connected to Chrome")
        
        self._message_id += 1
        message = {
            "id": self._message_id,
            "method": method,
            "params": params or {},
        }
        
        try:
            await self._ws.send(json.dumps(message))
        except ConnectionClosed as exc:
            self._mark_disconnected()
            raise CDPConnectionError(
                "CDP connection was closed. Reconnect to Chrome and try again."
            ) from exc
        
        # Wait for response
        while True:
            try:
                response = await self._ws.recv()
            except ConnectionClosed as exc:
                self._mark_disconnected()
                raise CDPConnectionError(
                    "CDP connection was closed while waiting for Chrome. Reconnect to Chrome and try again."
                ) from exc
            data = json.loads(response)
            if data.get("id") == self._message_id:
                if "error" in data:
                    raise CDPConnectionError(f"CDP error: {data['error']}")
                return data.get("result", {})
    
    async def connect(self) -> None:
        """Connect to Chrome via CDP and set up the browser."""
        try:
            # Get available targets from Chrome
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://localhost:{self.cdp_port}/json") as resp:
                    if resp.status != 200:
                        raise CDPConnectionError(
                            f"Cannot connect to Chrome on port {self.cdp_port}. "
                            f"Make sure Chrome is running with --remote-debugging-port={self.cdp_port}"
                        )
                    targets = await resp.json()
            
            # Find or create a page target
            # ── Rank all "page" targets; highest score first ───────────────
            ranked_pages = sorted(
                (t for t in targets if t.get("type") == "page"),
                key=lambda t: _score_cdp_page_target(
                    self.platform, str(self._base_url or ""), t
                ),
                reverse=True,
            )
            _LOGGER.debug(
                "[CDPHumanAdapter] connect() – %d page target(s) found: %s",
                len(ranked_pages),
                [
                    (t.get("url", "")[:80], _score_cdp_page_target(
                        self.platform, str(self._base_url or ""), t))
                    for t in ranked_pages[:5]
                ],
            )

            # ── Walk ranked candidates; skip tabs occupied by another debugger
            page_target = None
            ws_url = ""
            for candidate in ranked_pages:
                _ws_candidate = str(candidate.get("webSocketDebuggerUrl") or "").strip()
                if _ws_candidate:
                    page_target = candidate
                    ws_url = _ws_candidate
                    break

            if not page_target:
                _LOGGER.warning(
                    "[CDPHumanAdapter] No usable page target found – creating new tab at %s",
                    self._base_url or "about:blank",
                )
                # Create new page
                async with aiohttp.ClientSession() as session:
                    async with session.put(
                        f"http://localhost:{self.cdp_port}/json/new?{self._base_url or 'about:blank'}"
                    ) as resp:
                        page_target = await resp.json()
                ws_url = str(page_target.get("webSocketDebuggerUrl") or "").strip()

            if not ws_url:
                raise CDPConnectionError(
                    "No WebSocket URL found for page — tab may already be attached "
                    "to another CDP client. Close other DevTools windows and retry."
                )
            
            self._ws = await websockets.connect(
                ws_url,
                ping_interval=None,   # Tradovate DOM payloads are heavy; let the
                ping_timeout=None,    # engine drive the connection, not WS keepalive
                close_timeout=5,
                max_size=2**24,       # 16 MiB – needed for large CDP Runtime.evaluate
            )                         # responses from the Tradovate SPA
            self._connected = True
            self._page_url = str(page_target.get("url", "") or "")
            
            # Enable required domains
            await self._send_cdp_command("Page.enable")
            await self._send_cdp_command("Runtime.enable")
            
            # Inject anti-detection and cursor overlay
            await self._inject_scripts()
            
            # Navigate only when we are not already attached to the desired manual session.
            preserve_existing_page = _should_preserve_manual_cdp_page(
                platform=self.platform,
                username=self.username,
                password=self.password,
                target=page_target,
            )
            if self._base_url and not preserve_existing_page:
                await self._navigate(self._base_url)
                self._page_url = str(self._base_url)
                await human_pause(1.0, 2.5)
                if str(self.username or "").strip() or str(self.password or "").strip():
                    await self._login()
            
            _LOGGER.info(f"[CDPHumanAdapter] Connected to {self.platform}")
            
        except Exception as e:
            self._mark_disconnected()
            raise CDPConnectionError(f"Failed to connect: {e}") from e
    
    async def _inject_scripts(self) -> None:
        """Inject anti-detection and cursor overlay scripts."""
        js_code = get_full_injection_js()
        await self._send_cdp_command(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": js_code}
        )
        # Also evaluate immediately for current page
        await self._send_cdp_command("Runtime.evaluate", {"expression": js_code})

        # Inject additional stealth: clear ChromeDriver-specific properties
        # before each new document loads so the page never sees them.
        await self._send_cdp_command(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": _CDP_STEALTH_EXTRA_JS},
        )
        await self._send_cdp_command(
            "Runtime.evaluate",
            {"expression": _CDP_STEALTH_EXTRA_JS},
        )
    
    async def _navigate(self, url: str) -> None:
        """Navigate to a URL."""
        await self._send_cdp_command("Page.navigate", {"url": url})
        await asyncio.sleep(1.0)  # Wait for page load
    
    async def _login(self) -> None:
        """Perform login with human-like typing."""
        if not self._selectors:
            return
        
        _LOGGER.info(f"[CDPHumanAdapter] Starting login for {self.platform}")
        
        # Wait for page to load
        await human_pause(1.0, 2.0)
        
        # Find and fill username field
        username_selectors = self._selectors.login_username.split(", ")
        for selector in username_selectors:
            try:
                await self._human_click_selector(selector)
                await human_pause(0.2, 0.5)
                await self._human_type(self.username)
                break
            except Exception:
                continue
        
        await human_pause(0.3, 0.8)
        
        # Find and fill password field
        password_selectors = self._selectors.login_password.split(", ")
        for selector in password_selectors:
            try:
                await self._human_click_selector(selector)
                await human_pause(0.2, 0.5)
                await self._human_type(self.password)
                break
            except Exception:
                continue
        
        await human_pause(0.4, 1.0)
        
        # Click submit button
        submit_selectors = self._selectors.login_submit.split(", ")
        for selector in submit_selectors:
            try:
                await self._human_click_selector(selector)
                break
            except Exception:
                continue
        
        # Wait for login to complete
        await human_pause(2.0, 4.0)
        
        _LOGGER.info(f"[CDPHumanAdapter] Login completed for {self.platform}")
    
    async def _resolve_tradovate_bundle_target(self, selector: str) -> dict[str, Any] | None:
        """Resolve Tradovate action buttons inside the active chart module with fallbacks."""
        action = _selector_bundle_action(self.platform, selector)
        if action is None:
            return None

        health_config = _build_tradovate_execution_health_config(action)
        if not isinstance(health_config, dict):
            return None
        js_code = f"""
        (function() {{
            const moduleSelector = {json.dumps(_TRADOVATE_PARENT_MODULE_SELECTOR)};
            const healthConfig = {json.dumps(health_config)};
            const normalizeText = (value) => String(value || '').replace(/\\s+/g, ' ').trim();
            const isVisible = (el) => {{
                if (!el) return false;
                const rect = el.getBoundingClientRect();
                const style = window.getComputedStyle(el);
                // NOTE: pointer-events is intentionally NOT checked here.
                // CDP sends raw Input.dispatchMouseEvent at screen coordinates which
                // bypasses CSS pointer-events entirely – so a button with
                // pointer-events:none is still clickable via CDP and must be found.
                return rect.width > 0 &&
                    rect.height > 0 &&
                    style.visibility !== 'hidden' &&
                    style.display !== 'none';
            }};
            const isInteractable = (el) => {{
                if (!isVisible(el)) return false;
                const style = window.getComputedStyle(el);
                const className = normalizeText(el.className || '').toLowerCase();
                // pointer-events:none is NOT a hard block for CDP clicks (CDP bypasses it).
                // Only block on genuinely disabled states (disabled attr, aria-disabled, opacity 0).
                return !(
                    el.hasAttribute('disabled') ||
                    String(el.getAttribute('aria-disabled') || '').toLowerCase() === 'true' ||
                    className.includes('disabled') ||
                    style.opacity === '0'
                );
            }};
            const scoreModule = (el) => {{
                if (!isVisible(el)) return -1;
                const rect = el.getBoundingClientRect();
                const style = window.getComputedStyle(el);
                const activeClass = /\\b(active|selected|focused|current|primary)\\b/i.test(el.className || '') ? 40 : 0;
                const aria = (
                    el.getAttribute('aria-selected') === 'true' ||
                    el.getAttribute('aria-current') ||
                    el.getAttribute('data-active') === 'true'
                ) ? 35 : 0;
                const focus = el.matches(':focus-within') ? 25 : 0;
                const z = Number.parseInt(style.zIndex || '0', 10);
                const zScore = Number.isFinite(z) ? Math.min(z, 30) : 0;
                const areaScore = Math.min((rect.width * rect.height) / 5000, 25);
                return activeClass + aria + focus + zScore + areaScore;
            }};
            const findWithinModule = (root, selector) => {{
                try {{
                    if (selector.startsWith('//')) {{
                        const xpath = document.evaluate(
                            selector,
                            root,
                            null,
                            XPathResult.ORDERED_NODE_SNAPSHOT_TYPE,
                            null
                        );
                        for (let i = 0; i < xpath.snapshotLength; i += 1) {{
                            const node = xpath.snapshotItem(i);
                            if (node instanceof Element && isVisible(node)) return node;
                        }}
                        return null;
                    }}
                    const elements = Array.from(root.querySelectorAll(selector));
                    for (const element of elements) {{
                        if (isVisible(element)) return element;
                    }}
                }} catch (_) {{
                    return null;
                }}
                return null;
            }};

            // --- Pass 1: search inside the active chart module (preferred) ---
            const modules = Array.from(document.querySelectorAll(moduleSelector)).filter(isVisible);
            const searchRoots = [];
            if (modules.length) {{
                modules.sort((a, b) => scoreModule(b) - scoreModule(a));
                searchRoots.push({{ root: modules[0], label: moduleSelector, score: scoreModule(modules[0]) }});
            }}
            // --- Pass 2: full-document fallback (runs when module not found OR
            //     none of the module-scoped queries matched) ---
            searchRoots.push({{ root: document, label: 'document', score: -1 }});

            for (const {{ root, label }} of searchRoots) {{
                for (const strategy of ['primary', 'aria', 'semantic']) {{
                    const selectors = Array.isArray(healthConfig.strategies && healthConfig.strategies[strategy])
                        ? healthConfig.strategies[strategy]
                        : [];
                    for (let index = 0; index < selectors.length; index += 1) {{
                        const candidateSelector = selectors[index];
                        const element = findWithinModule(root, candidateSelector);
                        if (!element) continue;
                        const rect = element.getBoundingClientRect();
                        return {{
                            x: rect.x,
                            y: rect.y,
                            width: rect.width,
                            height: rect.height,
                            selector_used: candidateSelector,
                            selector_index: index,
                            strategy,
                            interactable: isInteractable(element),
                            module_selector: label,
                            module_class: root === document ? 'document' : normalizeText((root.className || '')),
                            module_score: label === 'document' ? -1 : scoreModule(root),
                            document_fallback: label === 'document',
                            error_code: isInteractable(element)
                                ? ''
                                : 'ERR_DOM_DISABLED_' + String(healthConfig.action || action || 'ACTION').toUpperCase(),
                        }};
                    }}
                }}
            }}
            return null;
        }})();
        """
        try:
            result = await asyncio.wait_for(
                self._send_cdp_command(
                    "Runtime.evaluate",
                    {"expression": js_code, "returnByValue": True},
                ),
                timeout=6.0,
            )
        except Exception:
            # DOM query timed-out or connection dropped – return None gracefully
            # so callers can schedule a retry on the next poll tick.
            return None
        payload = result.get("result", {}).get("value")
        return dict(payload) if isinstance(payload, dict) else None

    async def inspect_tradovate_ui_contract(
        self,
        *,
        required_actions: tuple[str, ...] = ("buy", "sell"),
        expected_instrument_token: str = "",
    ) -> dict[str, Any]:
        """Inspect the active Tradovate chart module without clicking anything."""
        if str(self.platform or "").strip().lower() != "tradovate":
            return {
                "ok": True,
                "status": "skipped_non_tradovate",
                "required_actions": list(required_actions),
                "actions": {},
                "issues": [],
            }

        required = [str(action).strip().lower() for action in required_actions if str(action).strip()]
        quantity_selectors = []
        order_quantity_selectors = ""
        if isinstance(self._selectors, dict):
            order_quantity_selectors = str(self._selectors.get("order_quantity", "") or "")
        elif self._selectors is not None:
            order_quantity_selectors = str(getattr(self._selectors, "order_quantity", "") or "")
        if order_quantity_selectors:
            quantity_selectors = [
                str(selector).strip()
                for selector in order_quantity_selectors.split(",")
                if str(selector).strip()
            ]
        selector_health_map = _build_tradovate_selector_health_map(
            quantity_selectors=quantity_selectors,
            expected_instrument_token=expected_instrument_token,
        )
        js_code = """
        (function() {
            const moduleSelector = __MODULE_SELECTOR__;
            const selectorHealthMap = __HEALTH_MAP__;
            const requiredActions = __REQUIRED_ACTIONS__;
            const normalizeText = (value) => String(value || '').replace(/\\s+/g, ' ').trim();
            const normalizeUpper = (value) => normalizeText(value).toUpperCase();
            const isVisible = (el) => {
                if (!el) return false;
                const rect = el.getBoundingClientRect();
                const style = window.getComputedStyle(el);
                // pointer-events is intentionally NOT checked: CDP Input.dispatchMouseEvent
                // bypasses pointer-events css entirely so a button with pointer-events:none
                // is still reachable via CDP and should be treated as visible.
                return rect.width > 0 &&
                    rect.height > 0 &&
                    style.visibility !== 'hidden' &&
                    style.display !== 'none';
            };
            const isInteractable = (el) => {
                if (!isVisible(el)) return false;
                const style = window.getComputedStyle(el);
                const className = normalizeText(el.className || '').toLowerCase();
                // pointer-events:none is omitted – CDP clicks bypass it.
                return !(
                    el.hasAttribute('disabled') ||
                    String(el.getAttribute('aria-disabled') || '').toLowerCase() === 'true' ||
                    className.includes('disabled') ||
                    style.opacity === '0'
                );
            };
            const scoreModule = (el) => {
                if (!isVisible(el)) return -1;
                const rect = el.getBoundingClientRect();
                const style = window.getComputedStyle(el);
                const activeClass = /\\b(active|selected|focused|current|primary)\\b/i.test(el.className || '') ? 40 : 0;
                const aria = (
                    el.getAttribute('aria-selected') === 'true' ||
                    el.getAttribute('aria-current') ||
                    el.getAttribute('data-active') === 'true'
                ) ? 35 : 0;
                const focus = el.matches(':focus-within') ? 25 : 0;
                const z = Number.parseInt(style.zIndex || '0', 10);
                const zScore = Number.isFinite(z) ? Math.min(z, 30) : 0;
                const areaScore = Math.min((rect.width * rect.height) / 5000, 25);
                return activeClass + aria + focus + zScore + areaScore;
            };
            const makeElementMeta = (el, selectorUsed, selectorIndex, strategy) => {
                const rect = el.getBoundingClientRect();
                return {
                    selector_used: selectorUsed,
                    selector_index: selectorIndex,
                    strategy,
                    x: rect.x,
                    y: rect.y,
                    width: rect.width,
                    height: rect.height,
                    text: normalizeText(el.innerText || el.textContent || ''),
                    class_name: normalizeText(el.className || ''),
                    data_qa: String(el.getAttribute('data-qa') || ''),
                    data_id: String(el.getAttribute('data-id') || ''),
                    data_testid: String(el.getAttribute('data-testid') || ''),
                    aria_label: String(el.getAttribute('aria-label') || ''),
                    title: String(el.getAttribute('title') || ''),
                    visible: isVisible(el),
                    interactable: isInteractable(el),
                };
            };
            const queryWithinModule = (root, selector) => {
                try {
                    if (!selector) return [];
                    if (selector.startsWith('//')) {
                        const xpath = document.evaluate(
                            selector,
                            root,
                            null,
                            XPathResult.ORDERED_NODE_SNAPSHOT_TYPE,
                            null
                        );
                        const matches = [];
                        for (let i = 0; i < xpath.snapshotLength; i += 1) {
                            const node = xpath.snapshotItem(i);
                            if (node instanceof Element && isVisible(node) && root.contains(node)) {
                                matches.push(node);
                            }
                        }
                        return matches;
                    }
                    return Array.from(root.querySelectorAll(selector)).filter(isVisible);
                } catch (_) {
                    return [];
                }
            };
            const buildCriticalResult = (config, code, message) => ({
                label: String(config.label || ''),
                kind: String(config.kind || ''),
                action: String(config.action || ''),
                status: 'critical',
                strategy: '',
                selector_used: '',
                selector_index: -1,
                found: false,
                visible: false,
                interactable: false,
                match_count: 0,
                duplicates: 0,
                primary: null,
                message,
                error_code: code,
            });
            const modules = Array.from(document.querySelectorAll(moduleSelector)).filter(isVisible);
            modules.sort((a, b) => scoreModule(b) - scoreModule(a));
            const activeModule = modules[0] || null;
            // When the chart-wrapper module is absent (e.g. Tradovate loaded a
            // different layout), fall back to searching the full document so that
            // Buy / Sell / Qty inputs are still found.  This keeps module_found
            // truthy and quantity_found accurate without hard-failing everything.
            const usingDocumentFallback = !activeModule;
            const searchRoot = activeModule || document;
            const issues = [];
            const warnings = [];
            const actions = {};
            const healthMap = {};
            const quantity = {
                found: false,
                match_count: 0,
                duplicates: 0,
                selector_used: '',
                selector_index: -1,
                primary: null,
                value: '',
                status: 'critical',
                strategy: '',
                interactable: false,
                error_code: '',
                message: '',
            };

            if (usingDocumentFallback) {
                warnings.push('Chart-module container ikke fundet – søger i hele dokumentet (document fallback).');
            }

            for (const [key, config] of Object.entries(selectorHealthMap)) {
                const requiresInteractable = config.interactable !== false;
                const expectedToken = normalizeUpper(config.expected_token || '');
                let resolved = null;
                for (const strategy of ['primary', 'aria', 'semantic']) {
                    const selectors = Array.isArray(config.strategies && config.strategies[strategy])
                        ? config.strategies[strategy]
                        : [];
                    for (let index = 0; index < selectors.length; index += 1) {
                        const selector = selectors[index];
                        const matches = queryWithinModule(searchRoot, selector);
                        if (!matches.length) continue;
                        const element = matches[0];
                        const meta = makeElementMeta(element, selector, index, strategy);
                        const searchCorpus = normalizeUpper([
                            meta.text,
                            meta.class_name,
                            meta.aria_label,
                            meta.title,
                            meta.data_qa,
                            meta.data_id,
                            meta.data_testid,
                        ].join(' '));
                        if (expectedToken && !searchCorpus.includes(expectedToken)) {
                            continue;
                        }
                        const payload = {
                            label: String(config.label || ''),
                            kind: String(config.kind || ''),
                            action: String(config.action || ''),
                            status: strategy === 'primary' ? 'ok' : 'degraded',
                            strategy,
                            selector_used: selector,
                            selector_index: index,
                            found: true,
                            visible: true,
                            interactable: meta.interactable,
                            match_count: matches.length,
                            duplicates: Math.max(0, matches.length - 1),
                            primary: meta,
                            message: strategy === 'primary'
                                ? String(config.label || '') + ' found via primary selector.'
                                : 'Warning: ' + String(config.label || '') + ' found via ' + strategy + ' fallback.',
                            error_code: strategy === 'primary'
                                ? ''
                                : 'WARN_DOM_' + String(config.action || key || '').toUpperCase() + '_DEGRADED',
                        };
                        if (requiresInteractable && !meta.interactable) {
                            payload.status = 'critical';
                            payload.error_code = 'ERR_DOM_DISABLED_' + String(config.action || key || '').toUpperCase();
                            payload.message = String(config.label || '') + ' er synlig, men ikke interactable.';
                        }
                        if (key === 'quantity_input') {
                            const rawValue = ('value' in element) ? element.value : (element.innerText || element.textContent || '');
                            quantity.value = normalizeText(rawValue);
                        }
                        resolved = payload;
                        break;
                    }
                    if (resolved) break;
                }
                if (!resolved) {
                    resolved = buildCriticalResult(
                        config,
                        'ERR_DOM_MISSING_' + String(config.action || key || '').toUpperCase(),
                        String(config.label || '') + ' selector blev ikke fundet i aktiv chart-module.'
                    );
                }
                healthMap[key] = resolved;
                if (resolved.status === 'critical') {
                    issues.push(resolved.message);
                } else if (resolved.status === 'degraded') {
                    warnings.push(resolved.message);
                }
            }

            actions.buy = healthMap.buy_button ? {
                found: healthMap.buy_button.found,
                match_count: healthMap.buy_button.match_count,
                duplicates: healthMap.buy_button.duplicates,
                selector_used: healthMap.buy_button.selector_used,
                selector_index: healthMap.buy_button.selector_index,
                primary: healthMap.buy_button.primary,
                status: healthMap.buy_button.status,
                strategy: healthMap.buy_button.strategy,
                interactable: healthMap.buy_button.interactable,
                error_code: healthMap.buy_button.error_code,
                message: healthMap.buy_button.message,
            } : { found: false, match_count: 0, duplicates: 0, selector_used: '', selector_index: -1, primary: null };
            actions.sell = healthMap.sell_button ? {
                found: healthMap.sell_button.found,
                match_count: healthMap.sell_button.match_count,
                duplicates: healthMap.sell_button.duplicates,
                selector_used: healthMap.sell_button.selector_used,
                selector_index: healthMap.sell_button.selector_index,
                primary: healthMap.sell_button.primary,
                status: healthMap.sell_button.status,
                strategy: healthMap.sell_button.strategy,
                interactable: healthMap.sell_button.interactable,
                error_code: healthMap.sell_button.error_code,
                message: healthMap.sell_button.message,
            } : { found: false, match_count: 0, duplicates: 0, selector_used: '', selector_index: -1, primary: null };
            actions.flat = healthMap.flat_button ? {
                found: healthMap.flat_button.found,
                match_count: healthMap.flat_button.match_count,
                duplicates: healthMap.flat_button.duplicates,
                selector_used: healthMap.flat_button.selector_used,
                selector_index: healthMap.flat_button.selector_index,
                primary: healthMap.flat_button.primary,
                status: healthMap.flat_button.status,
                strategy: healthMap.flat_button.strategy,
                interactable: healthMap.flat_button.interactable,
                error_code: healthMap.flat_button.error_code,
                message: healthMap.flat_button.message,
            } : { found: false, match_count: 0, duplicates: 0, selector_used: '', selector_index: -1, primary: null };

            if (healthMap.quantity_input) {
                quantity.found = healthMap.quantity_input.found;
                quantity.match_count = healthMap.quantity_input.match_count;
                quantity.duplicates = healthMap.quantity_input.duplicates;
                quantity.selector_used = healthMap.quantity_input.selector_used;
                quantity.selector_index = healthMap.quantity_input.selector_index;
                quantity.primary = healthMap.quantity_input.primary;
                quantity.status = healthMap.quantity_input.status;
                quantity.strategy = healthMap.quantity_input.strategy;
                quantity.interactable = healthMap.quantity_input.interactable;
                quantity.error_code = healthMap.quantity_input.error_code;
                quantity.message = healthMap.quantity_input.message;
            }

            const entryCriticalKeys = [];
            for (const key of ['quantity_input', 'instrument_header']) {
                if (healthMap[key] && healthMap[key].status === 'critical') {
                    entryCriticalKeys.push(key);
                }
            }
            for (const action of requiredActions) {
                const key = action === 'buy' ? 'buy_button' : action === 'sell' ? 'sell_button' : '';
                if (key && healthMap[key] && healthMap[key].status === 'critical') {
                    entryCriticalKeys.push(key);
                }
            }
            const entryDegradedKeys = [];
            for (const key of ['quantity_input', 'instrument_header']) {
                if (healthMap[key] && healthMap[key].status === 'degraded') {
                    entryDegradedKeys.push(key);
                }
            }
            for (const action of requiredActions) {
                const key = action === 'buy' ? 'buy_button' : action === 'sell' ? 'sell_button' : '';
                if (key && healthMap[key] && healthMap[key].status === 'degraded') {
                    entryDegradedKeys.push(key);
                }
            }
            const exitCriticalKeys = healthMap.flat_button && healthMap.flat_button.status === 'critical'
                ? ['flat_button']
                : [];
            const exitDegradedKeys = healthMap.flat_button && healthMap.flat_button.status === 'degraded'
                ? ['flat_button']
                : [];

            const entryIntegrity = {
                ok: entryCriticalKeys.length === 0,
                status: entryCriticalKeys.length ? 'critical' : (entryDegradedKeys.length ? 'degraded' : 'ok'),
                critical_keys: entryCriticalKeys,
                degraded_keys: entryDegradedKeys,
            };
            const exitIntegrity = {
                ok: exitCriticalKeys.length === 0,
                status: exitCriticalKeys.length ? 'critical' : (exitDegradedKeys.length ? 'degraded' : 'ok'),
                critical_keys: exitCriticalKeys,
                degraded_keys: exitDegradedKeys,
            };
            const blockingErrorCode = entryCriticalKeys.length
                ? String(healthMap[entryCriticalKeys[0]].error_code || 'ERR_DOM_INTEGRITY_COMPROMISED')
                : (exitCriticalKeys.length ? String(healthMap[exitCriticalKeys[0]].error_code || 'ERR_DOM_INTEGRITY_COMPROMISED') : '');

            return {
                ok: entryIntegrity.ok && exitIntegrity.ok,
                status: entryIntegrity.ok && exitIntegrity.ok ? 'ok' : 'invalid',
                required_actions: requiredActions,
                module_selector: moduleSelector,
                module_count: modules.length,
                active_module: activeModule ? {
                    class_name: normalizeText(activeModule.className || ''),
                    score: scoreModule(activeModule),
                    document_fallback: false,
                } : (usingDocumentFallback ? {
                    // Document-fallback sentinel – truthy so module_found stays green.
                    class_name: 'document',
                    score: 0,
                    document_fallback: true,
                } : null),
                actions,
                quantity,
                health_map: healthMap,
                entry_integrity: entryIntegrity,
                exit_integrity: exitIntegrity,
                warnings,
                issues,
                blocking_error_code: blockingErrorCode,
            };
        })();
        """
        js_code = js_code.replace("__MODULE_SELECTOR__", json.dumps(_TRADOVATE_PARENT_MODULE_SELECTOR))
        js_code = js_code.replace("__HEALTH_MAP__", json.dumps(selector_health_map))
        js_code = js_code.replace("__REQUIRED_ACTIONS__", json.dumps(required))
        result = await self._send_cdp_command(
            "Runtime.evaluate",
            {"expression": js_code, "returnByValue": True},
        )
        payload = result.get("result", {}).get("value")
        return dict(payload) if isinstance(payload, dict) else {
            "ok": False,
            "status": "invalid_payload",
            "required_actions": required,
            "actions": {},
            "quantity": {
                "found": False,
                "match_count": 0,
                "duplicates": 0,
                "selector_used": "",
                "selector_index": -1,
                "primary": None,
                "value": "",
            },
            "issues": ["Tradovate UI contract returnerede et ugyldigt payload."],
        }

    async def _get_element_bounds_basic(self, selector: str) -> dict | None:
        """Get the bounding box of the first visible element matching ``selector``."""
        js_code = f"""
        (function() {{
            const elements = Array.from(document.querySelectorAll('{selector}'));
            for (const element of elements) {{
                const rect = element.getBoundingClientRect();
                const style = window.getComputedStyle(element);
                const visible = rect.width > 0 &&
                    rect.height > 0 &&
                    style.visibility !== 'hidden' &&
                    style.display !== 'none' &&
                    style.pointerEvents !== 'none';
                if (!visible) continue;
                return {{
                    x: rect.x,
                    y: rect.y,
                    width: rect.width,
                    height: rect.height
                }};
            }}
            return null;
        }})();
        """
        result = await self._send_cdp_command(
            "Runtime.evaluate",
            {"expression": js_code, "returnByValue": True}
        )
        bounds = result.get("result", {}).get("value")
        return bounds

    async def _get_element_bounds(self, selector: str) -> dict | None:
        """Get visible bounds for a selector, using Tradovate selector bundles when relevant."""
        action = _selector_bundle_action(self.platform, selector)
        bundle_target = await self._resolve_tradovate_bundle_target(selector)
        if isinstance(bundle_target, dict):
            if not bool(bundle_target.get("interactable", True)):
                return None
            return {
                "x": bundle_target.get("x"),
                "y": bundle_target.get("y"),
                "width": bundle_target.get("width"),
                "height": bundle_target.get("height"),
            }
        if action is not None:
            return None
        return await self._get_element_bounds_basic(selector)

    async def _get_cursor_overlay_position(
        self,
        *,
        default_x: float = 100.0,
        default_y: float = 100.0,
    ) -> dict[str, float]:
        """Return the current visible cursor overlay position."""
        js_code = """
        (function() {
            const posKey = __POS_KEY__;
            const fallback = { x: __DEFAULT_X__, y: __DEFAULT_Y__ };
            if (window[posKey]) {
                return window[posKey];
            }
            return fallback;
        })();
        """
        js_code = js_code.replace("__POS_KEY__", json.dumps(CURSOR_POS_KEY))
        js_code = js_code.replace("__DEFAULT_X__", json.dumps(float(default_x)))
        js_code = js_code.replace("__DEFAULT_Y__", json.dumps(float(default_y)))
        result = await self._send_cdp_command(
            "Runtime.evaluate",
            {"expression": js_code, "returnByValue": True},
        )
        value = result.get("result", {}).get("value", {"x": default_x, "y": default_y})
        return {
            "x": float(value.get("x", default_x)),
            "y": float(value.get("y", default_y)),
        }

    async def _set_cursor_overlay_position(self, x: float, y: float) -> None:
        """Update the visible cursor overlay and its stored coordinates."""
        js_code = """
        (function() {
            const elKey = __EL_KEY__;
            const posKey = __POS_KEY__;
            const x = __X__;
            const y = __Y__;
            const cursor = window[elKey];
            if (cursor) {
                cursor.style.left = x + 'px';
                cursor.style.top = y + 'px';
            }
            window[posKey] = { x: x, y: y };
        })();
        """
        js_code = js_code.replace("__EL_KEY__", json.dumps(CURSOR_EL_KEY))
        js_code = js_code.replace("__POS_KEY__", json.dumps(CURSOR_POS_KEY))
        js_code = js_code.replace("__X__", json.dumps(float(x)))
        js_code = js_code.replace("__Y__", json.dumps(float(y)))
        await self._send_cdp_command("Runtime.evaluate", {"expression": js_code})

    async def _dispatch_mouse_move(self, x: float, y: float) -> None:
        """Dispatch a native mouseMoved event to keep page state aligned."""
        await self._send_cdp_command(
            "Input.dispatchMouseEvent",
            {
                "type": "mouseMoved",
                "x": float(x),
                "y": float(y),
            },
        )

    async def _ensure_cursor_sync(self, target_x: float, target_y: float, *, tolerance_px: float = 3.0) -> None:
        """Verify that overlay state matches the expected cursor target."""
        current = await self._get_cursor_overlay_position(default_x=target_x, default_y=target_y)
        dx = abs(float(current.get("x", target_x)) - float(target_x))
        dy = abs(float(current.get("y", target_y)) - float(target_y))
        if max(dx, dy) <= float(tolerance_px):
            return
        _LOGGER.warning(
            "[CDPHumanAdapter] Cursor overlay drift detected (dx=%.2f, dy=%.2f); correcting.",
            dx,
            dy,
        )
        await self._set_cursor_overlay_position(target_x, target_y)
        await self._dispatch_mouse_move(target_x, target_y)
    
    async def _move_cursor_human(
        self,
        target_x: float,
        target_y: float,
        *,
        duration_ms: float | None = None,
        should_abort: Callable[[], bool] | None = None,
    ) -> bool:
        """Move cursor to target with an eased Bézier path and near-target micro-corrections."""
        current = await self._get_cursor_overlay_position(default_x=100.0, default_y=100.0)
        start = Point(x=current["x"], y=current["y"])
        end = Point(x=target_x, y=target_y)

        # Generate human-like path
        path = _build_human_cursor_path(start, end, num_points=30, jitter_px=2.0)

        # Calculate duration based on distance (longer distance = more time)
        distance = ((end.x - start.x) ** 2 + (end.y - start.y) ** 2) ** 0.5
        total_duration_ms = float(duration_ms) if duration_ms is not None else max(200, min(1500, distance * 3))
        step_delay_s = max(0.008, (total_duration_ms / 1000.0) / max(len(path), 1))

        for point in path:
            if callable(should_abort) and bool(should_abort()):
                return False
            await self._set_cursor_overlay_position(point.x, point.y)
            await self._dispatch_mouse_move(point.x, point.y)
            await asyncio.sleep(step_delay_s)

        if callable(should_abort) and bool(should_abort()):
            return False
        await self._ensure_cursor_sync(target_x, target_y)
        return True

    async def _get_primary_chart_safe_zone(self) -> dict[str, float] | None:
        """Return a neutral point within the primary visible chart module."""
        js_code = """
        (function() {
            const moduleSelector = __MODULE_SELECTOR__;
            const isVisible = (element) => {
                if (!(element instanceof Element)) return false;
                const rect = element.getBoundingClientRect();
                const style = window.getComputedStyle(element);
                return (
                    rect.width > 0 &&
                    rect.height > 0 &&
                    style.visibility !== 'hidden' &&
                    style.display !== 'none'
                );
            };
            const scoreModule = (module) => {
                if (!isVisible(module)) return -1;
                const rect = module.getBoundingClientRect();
                const style = window.getComputedStyle(module);
                const activeClass = /\\b(active|selected|focused|current|primary)\\b/i.test(module.className) ? 40 : 0;
                const aria = (
                    module.getAttribute('aria-selected') === 'true' ||
                    module.getAttribute('aria-current') ||
                    module.getAttribute('data-active') === 'true'
                ) ? 35 : 0;
                const focus = module.matches(':focus-within') ? 25 : 0;
                const z = Number.parseInt(style.zIndex, 10);
                const zScore = Number.isFinite(z) ? Math.min(z, 30) : 0;
                const areaScore = Math.min((rect.width * rect.height) / 5000, 25);
                return activeClass + aria + focus + zScore + areaScore;
            };
            const modules = Array.from(document.querySelectorAll(moduleSelector)).filter(isVisible);
            if (!modules.length) return null;
            modules.sort((a, b) => scoreModule(b) - scoreModule(a));
            const rect = modules[0].getBoundingClientRect();
            return { x: rect.x, y: rect.y, width: rect.width, height: rect.height };
        })();
        """
        js_code = js_code.replace("__MODULE_SELECTOR__", json.dumps(_TRADOVATE_PARENT_MODULE_SELECTOR))
        result = await self._send_cdp_command(
            "Runtime.evaluate",
            {"expression": js_code, "returnByValue": True},
        )
        payload = result.get("result", {}).get("value")
        return dict(payload) if isinstance(payload, dict) else None

    async def reset_ui_focus(self, *, should_abort: Callable[[], bool] | None = None) -> bool:
        """Glide the cursor to a neutral chart coordinate after a close-out action."""
        if not self._ws:
            raise CDPConnectionError("Not connected to Chrome")
        safe_zone = await self._get_primary_chart_safe_zone()
        if not isinstance(safe_zone, dict):
            return False
        if callable(should_abort) and bool(should_abort()):
            return False
        target_x = float(safe_zone["x"]) + float(safe_zone["width"]) * random.uniform(0.72, 0.88)
        target_y = float(safe_zone["y"]) + float(safe_zone["height"]) * random.uniform(0.40, 0.62)
        completed = await self._move_cursor_human(
            target_x,
            target_y,
            duration_ms=random.uniform(1500.0, 3000.0),
            should_abort=should_abort,
        )
        return bool(completed)

    async def _dispatch_mouse_wheel(self, x: float, y: float, *, delta_x: float = 0.0, delta_y: float = 0.0) -> None:
        """Dispatch a tiny wheel gesture to simulate idle human scroll noise."""
        await self._send_cdp_command(
            "Input.dispatchMouseEvent",
            {
                "type": "mouseWheel",
                "x": float(x),
                "y": float(y),
                "deltaX": float(delta_x),
                "deltaY": float(delta_y),
            },
        )

    async def perform_idle_scroll_noise(self) -> bool:
        """Emit a tiny up/down scroll gesture while waiting in armed state."""
        if not self._ws:
            raise CDPConnectionError("Not connected to Chrome")

        js_code = """
        (function() {
            if (window.%s) {
                return window.%s;
            }
            return { x: window.innerWidth * 0.55, y: window.innerHeight * 0.45 };
        })();
        """ % (CURSOR_POS_KEY, CURSOR_POS_KEY)
        result = await self._send_cdp_command(
            "Runtime.evaluate",
            {"expression": js_code, "returnByValue": True},
        )
        current = result.get("result", {}).get("value", {"x": 100, "y": 100})
        x = float(current.get("x", 100.0))
        y = float(current.get("y", 100.0))
        delta_y = float(random.choice((-14.0, -11.0, -8.0, 8.0, 11.0, 14.0)))
        delta_x = float(random.uniform(-1.5, 1.5))
        await self._dispatch_mouse_wheel(x, y, delta_x=delta_x, delta_y=delta_y)
        await asyncio.sleep(random.uniform(0.05, 0.18))
        await self._dispatch_mouse_wheel(
            x,
            y,
            delta_x=random.uniform(-1.0, 1.0),
            delta_y=float(-delta_y * random.uniform(0.85, 1.1)),
        )
        return True

    async def _human_click_selector(self, selector: str) -> bool:
        """Click an element with human-like behaviour."""
        bounds = await self._get_element_bounds(selector)
        if not bounds:
            raise CDPConnectionError(f"Element not found: {selector}")
        
        # Calculate random click offset within element
        offset = random_click_offset(
            width=bounds["width"],
            height=bounds["height"],
            margin_pct=0.2
        )
        
        target_x = bounds["x"] + offset.x
        target_y = bounds["y"] + offset.y
        
        # Move cursor to element
        await self._move_cursor_human(target_x, target_y)
        
        # Random delay before click (0.5-2.0 seconds)
        await human_pause(MIN_CLICK_DELAY, MAX_CLICK_DELAY)
        
        # Perform click via CDP
        await self._send_cdp_command(
            "Input.dispatchMouseEvent",
            {
                "type": "mousePressed",
                "x": target_x,
                "y": target_y,
                "button": "left",
                "clickCount": 1,
            }
        )
        
        # Human-like click duration (80-180ms)
        await asyncio.sleep(_human_click_hold_seconds())
        
        await self._send_cdp_command(
            "Input.dispatchMouseEvent",
            {
                "type": "mouseReleased",
                "x": target_x,
                "y": target_y,
                "button": "left",
                "clickCount": 1,
            }
        )
        
        # Trigger click effect on cursor overlay
        js_click = "window.%s && window.%s();" % (CURSOR_CLICK_FN, CURSOR_CLICK_FN)
        await self._send_cdp_command("Runtime.evaluate", {"expression": js_click})
        
        return True
    
    async def _human_type(self, text: str) -> None:
        """Type text with human-like delays between keystrokes."""
        wpm = random.uniform(MIN_TYPING_WPM, MAX_TYPING_WPM)
        delays = human_typing_delays(text, base_wpm=wpm, variance=0.3)
        
        for char, delay in zip(text, delays):
            # Dispatch keydown
            await self._send_cdp_command(
                "Input.dispatchKeyEvent",
                {
                    "type": "keyDown",
                    "text": char,
                }
            )
            
            # Dispatch keypress
            await self._send_cdp_command(
                "Input.dispatchKeyEvent",
                {
                    "type": "char",
                    "text": char,
                }
            )
            
            await asyncio.sleep(delay)
            
            # Dispatch keyup
            await self._send_cdp_command(
                "Input.dispatchKeyEvent",
                {
                    "type": "keyUp",
                    "text": char,
                }
            )
    
    async def place_order(self, request: OrderRequest) -> OrderResult:
        """Place an order with human-like browser interaction."""
        if not self._connected or not self._selectors:
            return OrderResult(
                order_id="",
                status=OrderStatus.ERROR,
                instrument=request.instrument,
                side=request.side,
                quantity=request.quantity,
                fill_price=None,
                error_message="Not connected to browser",
                raw_response={},
            )
        
        instrument_name = self._instrument_map.get(request.instrument, request.instrument)
        
        try:
            _LOGGER.info(f"[CDPHumanAdapter] Placing {request.side.value} order for {instrument_name}")
            
            # Random pause before starting order entry
            await human_pause(0.5, 1.5)
            
            # Fill quantity
            qty_selectors = self._selectors.order_quantity.split(", ")
            for selector in qty_selectors:
                try:
                    await self._human_click_selector(selector)
                    await human_pause(0.2, 0.5)
                    # Clear existing value
                    await self._send_cdp_command(
                        "Runtime.evaluate",
                        {"expression": f"document.querySelector('{selector}').value = ''"}
                    )
                    await self._human_type(str(int(request.quantity)))
                    break
                except Exception:
                    continue
            
            await human_pause(0.3, 0.8)
            
            # Set stop loss if provided
            if request.stop_price is not None:
                stop_selectors = self._selectors.order_stop_field.split(", ")
                for selector in stop_selectors:
                    try:
                        await self._human_click_selector(selector)
                        await human_pause(0.2, 0.5)
                        await self._send_cdp_command(
                            "Runtime.evaluate",
                            {"expression": f"document.querySelector('{selector}').value = ''"}
                        )
                        await self._human_type(str(request.stop_price))
                        break
                    except Exception:
                        continue
            
            await human_pause(0.3, 0.8)
            
            # Click buy or sell button
            if request.side == OrderSide.BUY:
                button_selectors = self._selectors.order_buy_button.split(", ")
            else:
                button_selectors = self._selectors.order_sell_button.split(", ")
            
            for selector in button_selectors:
                try:
                    await self._human_click_selector(selector)
                    break
                except Exception:
                    continue
            
            await human_pause(0.5, 1.5)
            
            # Confirm order
            confirm_selectors = self._selectors.order_confirm.split(", ")
            for selector in confirm_selectors:
                try:
                    await self._human_click_selector(selector)
                    break
                except Exception:
                    continue
            
            await human_pause(1.0, 2.0)
            
            # Take screenshot if configured
            if self.screenshot_dir:
                self.screenshot_dir.mkdir(parents=True, exist_ok=True)
                ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
                screenshot = await self._send_cdp_command("Page.captureScreenshot")
                if screenshot.get("data"):
                    screenshot_path = self.screenshot_dir / f"order_{ts}_{request.idempotency_key[:8]}.png"
                    import base64
                    with open(screenshot_path, "wb") as f:
                        f.write(base64.b64decode(screenshot["data"]))
            
            _LOGGER.info(f"[CDPHumanAdapter] Order placed successfully for {instrument_name}")
            
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
            
        except Exception as e:
            _LOGGER.error(f"[CDPHumanAdapter] Order failed: {e}")
            return OrderResult(
                order_id="",
                status=OrderStatus.ERROR,
                instrument=request.instrument,
                side=request.side,
                quantity=request.quantity,
                fill_price=None,
                error_message=str(e),
                raw_response={},
            )
    
    async def get_account_balance(self) -> float:
        """Get account balance from the platform."""
        if not self._connected or not self._selectors:
            return 0.0
        
        try:
            equity_selectors = self._selectors.account_equity.split(", ")
            for selector in equity_selectors:
                js_code = f"""
                (function() {{
                    const el = document.querySelector('{selector}');
                    if (el) return el.innerText || el.textContent;
                    return null;
                }})();
                """
                result = await self._send_cdp_command(
                    "Runtime.evaluate",
                    {"expression": js_code, "returnByValue": True}
                )
                text = result.get("result", {}).get("value")
                if text:
                    # Extract numeric value
                    import re
                    numbers = re.findall(r'[-+]?\d[\d,.]*', text.replace(',', '.'))
                    if numbers:
                        return float(numbers[0].replace(',', '').replace(' ', ''))
        except Exception:
            pass
        
        return 0.0
    
    async def get_open_positions(self) -> list[dict[str, Any]]:
        """Get open positions from the platform."""
        if not self._connected or not self._selectors:
            return []
        
        try:
            js_code = f"""
            (function() {{
                const table = document.querySelector('{self._selectors.positions_table}');
                if (!table) return [];
                const rows = table.querySelectorAll('tr');
                return Array.from(rows).map(row => ({{
                    text: row.innerText
                }}));
            }})();
            """
            result = await self._send_cdp_command(
                "Runtime.evaluate",
                {"expression": js_code, "returnByValue": True}
            )
            return result.get("result", {}).get("value", [])
        except Exception:
            return []

    async def get_visible_page_text(self) -> str:
        """Return visible page text for reconciliation and safety checks."""
        if not self._connected:
            return ""

        try:
            js_code = """
            (function() {
                const body = document.body;
                if (!body) return '';
                return String(body.innerText || body.textContent || '');
            })();
            """
            result = await self._send_cdp_command(
                "Runtime.evaluate",
                {"expression": js_code, "returnByValue": True},
            )
            return str(result.get("result", {}).get("value") or "")
        except Exception:
            return ""

    async def get_visible_page_context(self) -> dict[str, str]:
        """Return visible text plus page metadata from the active CDP target."""
        if not self._connected:
            return {"text": "", "title": "", "url": ""}

        try:
            js_code = """
            (function() {
                const body = document.body;
                return {
                    text: String(body ? (body.innerText || body.textContent || '') : ''),
                    title: String(document.title || ''),
                    url: String(window.location.href || ''),
                };
            })();
            """
            result = await self._send_cdp_command(
                "Runtime.evaluate",
                {"expression": js_code, "returnByValue": True},
            )
            payload = result.get("result", {}).get("value")
            if isinstance(payload, dict):
                return {
                    "text": str(payload.get("text") or ""),
                    "title": str(payload.get("title") or ""),
                    "url": str(payload.get("url") or ""),
                }
        except Exception:
            pass
        return {"text": "", "title": "", "url": ""}

    async def get_broker_state_snapshot(
        self,
        instrument: str = "",
        expected_account_tokens: list[str] | tuple[str, ...] = (),
    ) -> dict[str, Any]:
        """Return a visible broker/UI snapshot suitable for reconciliation."""
        quantity_value = await self.get_order_quantity_value()
        visible_text = await self.get_visible_page_text()
        instrument_token = str(self._instrument_map.get(instrument, instrument) or "").strip().upper()
        snapshot = _extract_tradovate_broker_snapshot_from_text(
            visible_text,
            instrument_token=instrument_token,
            order_quantity_value=quantity_value,
            expected_account_tokens=expected_account_tokens,
        )
        rows = await self.get_open_positions()
        snapshot["positions_rows"] = list(rows[:10]) if isinstance(rows, list) else []
        return snapshot

    async def get_tradovate_read_only_snapshot(
        self,
        instrument: str = "",
        expected_account_tokens: list[str] | tuple[str, ...] = (),
        *,
        per_call_timeout: float = 7.0,
    ) -> dict[str, Any]:
        """Read-only Tradovate snapshot for observer/health diagnostics.

        Each sub-call is individually guarded by *per_call_timeout* seconds so a
        single stale DOM query cannot block the snapshot loop indefinitely.  When a
        sub-call times-out or errors, a safe empty value is substituted instead of
        propagating the exception – the final snapshot will simply report
        ``connected=False`` and the loop will retry on the next tick.
        """
        # Page context (title + visible text)
        try:
            page_context = await asyncio.wait_for(
                self.get_visible_page_context(), timeout=per_call_timeout
            )
        except Exception:
            page_context = {"text": "", "title": "", "url": ""}

        # Order quantity field value
        try:
            quantity_value = await asyncio.wait_for(
                self.get_order_quantity_value(), timeout=per_call_timeout
            )
        except Exception:
            quantity_value = None

        instrument_token = str(self._instrument_map.get(instrument, instrument) or "").strip().upper()
        snapshot = _extract_tradovate_broker_snapshot_from_text(
            page_context.get("text", ""),
            instrument_token=instrument_token,
            order_quantity_value=quantity_value,
            expected_account_tokens=expected_account_tokens,
        )

        # Open positions rows
        try:
            rows = await asyncio.wait_for(
                self.get_open_positions(), timeout=per_call_timeout
            )
        except Exception:
            rows = []

        snapshot["positions_rows"] = list(rows[:10]) if isinstance(rows, list) else []
        snapshot["connected"] = bool(self._connected)
        snapshot["page_title"] = str(page_context.get("title") or "")
        snapshot["page_url"] = str(page_context.get("url") or "")
        snapshot["observed_at"] = datetime.now(tz=timezone.utc).isoformat()
        snapshot["source"] = "tradovate_read_only_snapshot"
        return snapshot
    
    async def get_position(self, instrument: str) -> dict[str, Any] | None:
        """Get position for a specific instrument."""
        snapshot = await self.get_broker_state_snapshot(instrument)
        if not bool(snapshot.get("position_open")):
            return None
        if instrument and not bool(snapshot.get("instrument_visible")):
            return None
        return snapshot

    async def get_order_quantity_value(self) -> str | None:
        """Return the visible order quantity field value, if any."""
        if not self._connected or not self._selectors:
            return None

        try:
            quantity_selectors = self._selectors.order_quantity.split(", ")
            for selector in quantity_selectors:
                js_code = f"""
                (function() {{
                    const elements = Array.from(document.querySelectorAll('{selector}'));
                    for (const el of elements) {{
                        const rect = el.getBoundingClientRect();
                        const style = window.getComputedStyle(el);
                        const visible = rect.width > 0 &&
                            rect.height > 0 &&
                            style.visibility !== 'hidden' &&
                            style.display !== 'none' &&
                            style.pointerEvents !== 'none';
                        if (!visible) continue;
                        const value = ('value' in el) ? el.value : (el.innerText || el.textContent || '');
                        return String(value || '').trim();
                    }}
                    return null;
                }})();
                """
                result = await self._send_cdp_command(
                    "Runtime.evaluate",
                    {"expression": js_code, "returnByValue": True},
                )
                value = result.get("result", {}).get("value")
                if value is not None:
                    return str(value).strip()
        except Exception:
            pass

        return None

    async def _set_visible_input_value(self, selector: str, value: str) -> dict[str, Any]:
        """Set the first visible input/textarea matching selector via the native value setter."""
        desired = str(value)
        js_code = f"""
        (function() {{
            const elements = Array.from(document.querySelectorAll('{selector}'));
            for (const el of elements) {{
                const rect = el.getBoundingClientRect();
                const style = window.getComputedStyle(el);
                const visible = rect.width > 0 &&
                    rect.height > 0 &&
                    style.visibility !== 'hidden' &&
                    style.display !== 'none' &&
                    style.pointerEvents !== 'none';
                if (!visible) continue;
                if (typeof el.focus === 'function') el.focus();
                const nextValue = {json.dumps(desired)};
                const proto = el.tagName === 'TEXTAREA'
                    ? window.HTMLTextAreaElement?.prototype
                    : window.HTMLInputElement?.prototype;
                const descriptor = proto ? Object.getOwnPropertyDescriptor(proto, 'value') : null;
                if (descriptor && typeof descriptor.set === 'function') {{
                    descriptor.set.call(el, nextValue);
                }} else if ('value' in el) {{
                    el.value = nextValue;
                }}
                el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                if (typeof el.blur === 'function') el.blur();
                const applied = ('value' in el) ? el.value : (el.innerText || el.textContent || '');
                return {{ ok: String(applied || '').trim() === nextValue, value: String(applied || '').trim() }};
            }}
            return {{ ok: false, value: null }};
        }})();
        """
        result = await self._send_cdp_command(
            "Runtime.evaluate",
            {"expression": js_code, "returnByValue": True},
        )
        payload = result.get("result", {}).get("value")
        if isinstance(payload, dict):
            return payload
        return {"ok": False, "value": None}

    async def sync_order_quantity(self, quantity: float | int) -> dict[str, Any]:
        """Best-effort sync of the visible order quantity field to the expected value."""
        if not self._connected or not self._selectors:
            return {"ok": False, "expected": "", "value": None, "changed": False, "selector": None, "message": "Not connected"}

        expected = str(int(max(1, round(float(quantity)))))
        current = await self.get_order_quantity_value()
        if current == expected:
            return {
                "ok": True,
                "expected": expected,
                "value": current,
                "changed": False,
                "selector": None,
                "message": f"Quantity allerede sat til {expected}.",
            }

        quantity_selectors = self._selectors.order_quantity.split(", ")
        for selector in quantity_selectors:
            try:
                bounds = await self._get_element_bounds(selector)
                if not bounds:
                    continue
                await self._human_click_selector(selector)
                await human_pause(0.15, 0.35)
                set_result = await self._set_visible_input_value(selector, expected)
                if not bool(set_result.get("ok")):
                    continue
                await human_pause(0.1, 0.25)
                value_after = await self.get_order_quantity_value()
                if value_after == expected:
                    return {
                        "ok": True,
                        "expected": expected,
                        "value": value_after,
                        "changed": True,
                        "selector": selector,
                        "message": f"Quantity synket til {expected}.",
                    }
            except Exception:
                continue

        return {
            "ok": False,
            "expected": expected,
            "value": await self.get_order_quantity_value(),
            "changed": False,
            "selector": None,
            "message": f"Kunne ikke sætte quantity til {expected}.",
        }

    async def page_contains_tokens(self, tokens: list[str] | tuple[str, ...]) -> dict[str, Any]:
        """Check whether visible page text contains all expected tokens."""
        if not self._connected:
            return {"ok": False, "missing": list(tokens), "text_sample": "", "message": "Not connected"}

        normalized_tokens = [str(token).strip() for token in tokens if str(token).strip()]
        if not normalized_tokens:
            return {"ok": True, "missing": [], "text_sample": "", "message": "Ingen tokens krævet."}

        try:
            text = await self.get_visible_page_text()
        except Exception as exc:
            return {
                "ok": False,
                "missing": normalized_tokens,
                "text_sample": "",
                "message": f"Kunne ikke læse side-tekst: {exc}",
            }

        haystack = text.casefold()
        missing = [token for token in normalized_tokens if token.casefold() not in haystack]
        return {
            "ok": not missing,
            "missing": missing,
            "text_sample": text[:400],
            "message": (
                "Alle expected account tokens fundet."
                if not missing
                else f"Mangler expected account tokens: {', '.join(missing)}"
            ),
        }
    
    async def close_all_positions(self) -> None:
        """Emergency flatten – close all open positions.

        For Tradovate, tries the exit bundle selectors (Exit at Mkt → Flatten →
        …) in priority order via :meth:`click_element` so the same fallback chain
        that is used for normal exits is also used here.  Falls back to the
        legacy ``position_close_button`` CSS selectors for other platforms.
        """
        if not self._connected:
            return

        if str(getattr(self, "platform", "") or "").strip().lower() == "tradovate":
            # Use the registered exit bundle — click_element resolves XPath within
            # the active module with full fallback support.
            for exit_sel in _TRADOVATE_SELECTOR_BUNDLES.get("exit", ()):
                try:
                    clicked = await self.click_element(exit_sel, jitter_px=2.0)
                    if clicked:
                        return
                except Exception:
                    continue
            return

        if not self._selectors:
            return
        try:
            close_selectors = self._selectors.position_close_button.split(", ")
            for selector in close_selectors:
                js_code = f"""
                (function() {{
                    const buttons = document.querySelectorAll('{selector}');
                    buttons.forEach(btn => btn.click());
                }})();
                """
                await self._send_cdp_command("Runtime.evaluate", {"expression": js_code})
                await human_pause(0.5, 1.0)
        except Exception:
            pass
    
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order."""
        # Not directly supported via browser
        return False

    # ------------------------------------------------------------------ #
    # Public utility methods required by Custom Human UI                  #
    # ------------------------------------------------------------------ #

    async def send_raw_command(self, method: str, params: dict | None = None) -> dict:
        """Send a raw CDP command and return the result.

        Exposes the internal CDP transport as a public API so callers can
        issue arbitrary Chrome DevTools Protocol commands directly.

        Args:
            method: CDP method name, e.g. ``"Page.captureScreenshot"``.
            params: Optional parameters dict for the command.

        Returns:
            The ``result`` dict from the CDP response.
        """
        return await self._send_cdp_command(method, params)

    async def get_element_center(self, selector: str) -> tuple[float, float] | None:
        """Return the (x, y) centre of a DOM element via CDP.

        Uses ``DOM.getBoxModel`` for precision.  Falls back to a
        ``getBoundingClientRect`` JavaScript evaluation when the node
        cannot be resolved through the DOM domain.  If the element still
        cannot be found, attempts a recursive Shadow-DOM search so that
        deeply nested Tradovate components are also reachable.

        Args:
            selector: CSS selector string, e.g. ``"[data-qa='buy-button']"``.

        Returns:
            ``(x, y)`` screen coordinates of the element centre, or
            ``None`` if the element could not be located.
        """
        if not self._ws:
            raise CDPConnectionError("Not connected to Chrome")

        action = _selector_bundle_action(self.platform, selector)
        bundle_target = await self._resolve_tradovate_bundle_target(selector)
        if isinstance(bundle_target, dict):
            if not bool(bundle_target.get("interactable", True)):
                return None
            cx = float(bundle_target["x"]) + float(bundle_target["width"]) / 2.0
            cy = float(bundle_target["y"]) + float(bundle_target["height"]) / 2.0
            return cx, cy
        if action is not None:
            return None

        # --- Step 1: try DOM.getDocument + DOM.querySelector + DOM.getBoxModel ---
        try:
            doc = await self._send_cdp_command("DOM.getDocument", {"depth": 0})
            node_id_root = doc.get("root", {}).get("nodeId")
            if node_id_root:
                node_result = await self._send_cdp_command(
                    "DOM.querySelector",
                    {"nodeId": node_id_root, "selector": selector},
                )
                node_id = node_result.get("nodeId", 0)
                if node_id:
                    box = await self._send_cdp_command(
                        "DOM.getBoxModel", {"nodeId": node_id}
                    )
                    model = box.get("model")
                    if model:
                        content = model.get("content", [])
                        # content quad layout: [x1,y1, x2,y1, x2,y2, x1,y2]
                        if len(content) >= 8:
                            x1, y1, x2, _y1, x3, y3, _x1, _y3 = content[:8]
                            width = max(float(x2) - float(x1), 0.0)
                            height = max(float(y3) - float(y1), 0.0)
                            if width <= 0 or height <= 0:
                                raise ValueError("Matched element has no visible box")
                            cx = (x1 + x2 + x3 + _x1) / 4
                            cy = (y1 + _y1 + y3 + _y3) / 4
                            return cx, cy
        except Exception:
            pass

        # --- Step 2: getBoundingClientRect via Runtime.evaluate ---
        bounds = await self._get_element_bounds_basic(selector)
        if bounds:
            cx = bounds["x"] + bounds["width"] / 2
            cy = bounds["y"] + bounds["height"] / 2
            return cx, cy

        # --- Step 3: Shadow-DOM recursive search ---
        shadow_js = f"""
        (function findInShadow(root, selector) {{
            const el = root.querySelector(selector);
            if (el) {{
                const r = el.getBoundingClientRect();
                return {{ x: r.x, y: r.y, width: r.width, height: r.height }};
            }}
            const all = root.querySelectorAll('*');
            for (const node of all) {{
                if (node.shadowRoot) {{
                    const found = findInShadow(node.shadowRoot, selector);
                    if (found) return found;
                }}
            }}
            return null;
        }})(document, {json.dumps(selector)});
        """
        result = await self._send_cdp_command(
            "Runtime.evaluate",
            {"expression": shadow_js, "returnByValue": True},
        )
        shadow_bounds = result.get("result", {}).get("value")
        if shadow_bounds and shadow_bounds.get("width", 0) > 0:
            cx = shadow_bounds["x"] + shadow_bounds["width"] / 2
            cy = shadow_bounds["y"] + shadow_bounds["height"] / 2
            return cx, cy

        return None

    async def human_click_at(
        self,
        x: float,
        y: float,
        width: float = 1.0,
        height: float = 1.0,
        margin_pct: float = 0.0,
    ) -> None:
        """Perform a human-like click at explicit screen coordinates.

        Moves the visible cursor along a Bézier path to the target and
        dispatches ``mousePressed`` / ``mouseReleased`` CDP events with a
        realistic hold duration (80–180 ms).

        Args:
            x: Horizontal screen coordinate (top-left origin).
            y: Vertical screen coordinate (top-left origin).
            width: Element width used to compute a random jitter offset.
                   Pass ``1.0`` (default) for a point click.
            height: Element height used to compute a random jitter offset.
                    Pass ``1.0`` (default) for a point click.
            margin_pct: Fraction of width/height kept as a margin when
                        randomising the click offset within the element.
                        ``0.0`` means the click lands exactly at (x, y).
        """
        if not self._ws:
            raise CDPConnectionError("Not connected to Chrome")

        if margin_pct > 0.0 and width > 1.0 and height > 1.0:
            offset = random_click_offset(width=width, height=height, margin_pct=margin_pct)
            target_x = x + offset.x - width / 2
            target_y = y + offset.y - height / 2
        else:
            target_x = x + random.uniform(-2.0, 2.0)
            target_y = y + random.uniform(-2.0, 2.0)

        await self._move_cursor_human(target_x, target_y)
        await human_pause(MIN_CLICK_DELAY, MAX_CLICK_DELAY)

        await self._send_cdp_command(
            "Input.dispatchMouseEvent",
            {
                "type": "mousePressed",
                "x": target_x,
                "y": target_y,
                "button": "left",
                "clickCount": 1,
            },
        )
        await asyncio.sleep(_human_click_hold_seconds())
        await self._send_cdp_command(
            "Input.dispatchMouseEvent",
            {
                "type": "mouseReleased",
                "x": target_x,
                "y": target_y,
                "button": "left",
                "clickCount": 1,
            },
        )
        js_click = "window.%s && window.%s();" % (CURSOR_CLICK_FN, CURSOR_CLICK_FN)
        await self._send_cdp_command("Runtime.evaluate", {"expression": js_click})

    async def click_element(self, selector: str, jitter_px: float = 2.0) -> bool:
        """Find an element by CSS selector and click it with human behaviour.

        Tries standard DOM lookup first, then falls back to Shadow-DOM
        search (useful for Tradovate's complex web container).  Jitter is
        capped at ±``jitter_px`` pixels so small buttons are not missed.

        Args:
            selector: CSS selector, e.g. ``"[data-qa='buy-button']"``.
            jitter_px: Maximum pixel offset applied to the click position.

        Returns:
            ``True`` on success, ``False`` if the element was not found.
        """
        action = _selector_bundle_action(self.platform, selector)
        bundle_target = await self._resolve_tradovate_bundle_target(selector)
        if isinstance(bundle_target, dict):
            if not bool(bundle_target.get("interactable", True)):
                _LOGGER.warning(
                    "[DOM RADAR] %s target is visible but not interactable (%s).",
                    action or "selector",
                    bundle_target.get("error_code") or "disabled",
                )
                return False
            # Stealth mode: never click exactly on centre.
            # Pick a random point within the inner ±25 % of the button bounds so
            # the click is always on the button but the landing spot varies each
            # time — impossible to fingerprint as bot behaviour.
            bx = float(bundle_target["x"])
            by = float(bundle_target["y"])
            bw = float(bundle_target["width"])
            bh = float(bundle_target["height"])
            stealth_range_x = max(jitter_px, bw * 0.25)
            stealth_range_y = max(jitter_px, bh * 0.25)
            cx = bx + bw / 2.0 + random.uniform(-stealth_range_x, stealth_range_x)
            cy = by + bh / 2.0 + random.uniform(-stealth_range_y, stealth_range_y)
            action_name = action or "selector"
            if str(bundle_target.get("strategy") or "primary") != "primary" or int(bundle_target.get("selector_index", 0) or 0) > 0:
                _LOGGER.warning(
                    "[STABILITY] Fallback triggered for %s via %s (%s).",
                    action_name,
                    bundle_target.get("strategy") or "primary",
                    bundle_target.get("selector_used") or selector,
                )
        else:
            if action is not None:
                _LOGGER.warning("[CDPHumanAdapter] Tradovate target not found or not interactable: %s", selector)
                return False
            centre = await self.get_element_center(selector)
            if centre is None:
                _LOGGER.warning("[CDPHumanAdapter] Element not found: %s", selector)
                return False
            cx, cy = centre
            # Stealth: apply jitter even for coordinate-based fallback clicks.
            cx += random.uniform(-jitter_px, jitter_px)
            cy += random.uniform(-jitter_px, jitter_px)

        if cx is None or cy is None:
            _LOGGER.warning("[CDPHumanAdapter] Element not found: %s", selector)
            return False

        await self.human_click_at(cx, cy)
        return True

    async def capture_screenshot_base64(self) -> str | None:
        """Take a diagnostic screenshot and return it as a base64 string.

        Used as a fallback when an element cannot be located – the
        screenshot can be displayed in the UI for diagnosis.

        Returns:
            Base64-encoded PNG string, or ``None`` on failure.
        """
        try:
            result = await self._send_cdp_command("Page.captureScreenshot")
            return result.get("data")
        except Exception:
            return None

    async def test_connection(self) -> dict[str, Any]:
        """Verify the CDP connection is alive and return basic page info.

        Returns:
            A dict with keys ``ok`` (bool), ``url`` (str), and optionally
            ``error`` (str).
        """
        if not self._connected or not self._ws:
            return {"ok": False, "error": "Not connected"}
        try:
            result = await self._send_cdp_command("Target.getTargetInfo")
            url = result.get("targetInfo", {}).get("url", "")
            title = result.get("targetInfo", {}).get("title", "")
            return {"ok": True, "url": url, "title": title}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    async def inject_selector_spy(self) -> None:
        """Inject a one-shot click listener into the active Chrome tab.

        The listener intercepts the *next* click the user makes inside the
        browser, builds a CSS selector string from the clicked element's tag
        name, ``id``, class list and ``data-qa`` / ``data-testid`` attributes,
        and stores it in ``window.lastClickedSelector``. ``e.preventDefault()``
        and ``e.stopPropagation()`` are called so the original click action is
        suppressed while spying.

        After calling this method, poll :meth:`get_spied_selector` until it
        returns a non-``None`` value.
        """
        if not self._ws:
            raise CDPConnectionError("Not connected to Chrome")

        spy_js = """
(function() {
    window.lastClickedSelector = null;
    document.addEventListener('click', function(e) {
        e.preventDefault();
        e.stopPropagation();
        var el = e.target;
        var selector = el.tagName.toLowerCase();
        if (el.id) {
            selector += '#' + el.id;
        } else {
            if (el.className && typeof el.className === 'string') {
                selector += '.' + el.className.trim().replace(/\\s+/g, '.');
            }
            var dataTestId = el.getAttribute('data-testid');
            var dataQa = el.getAttribute('data-qa');
            if (dataTestId) { selector += '[data-testid="' + dataTestId + '"]'; }
            else if (dataQa) { selector += '[data-qa="' + dataQa + '"]'; }
        }
        window.lastClickedSelector = selector;
    }, {once: true, capture: true});
})();
"""
        await self._send_cdp_command(
            "Runtime.evaluate",
            {"expression": spy_js, "returnByValue": False},
        )
        _LOGGER.info("[CDPHumanAdapter] Selector spy injected – waiting for user click")

    async def auto_discover_trading_buttons(self) -> dict[str, dict[str, Any] | None]:
        """Discover Buy, Sell and Flatten buttons using confirmed Tradovate CSS classes.

        User-verified class mapping:
          BUY / ADD-TO-BUY  → div.btn.btn-success
          SELL / ADD-TO-SELL → div.btn.btn-danger
          EXIT (Flatten)    → button.btn.btn-default  (first non-dropdown instance)
          CONTRACT SIZE     → input.form-control

        Returns a dict with keys ``"buy"``, ``"sell"`` and optionally ``"flat"``.
        Each value is ``None`` (not found) or::

            {"selector": str, "x": float, "y": float}

        The ``selector`` is the bundle-registered CSS class so that
        :meth:`click_element` uses the full fallback chain automatically.
        """
        if not self._ws:
            raise CDPConnectionError("Not connected to Chrome")

        scan_js = """
(function() {
    function rect(el) { return el.getBoundingClientRect(); }
    function isVisible(el) {
        var r = rect(el);
        if (!r || r.width === 0 || r.height === 0) return false;
        var s = window.getComputedStyle(el);
        return s.display !== 'none' && s.visibility !== 'hidden' && parseFloat(s.opacity) > 0;
    }
    function center(el) {
        var r = rect(el);
        return {x: r.left + r.width / 2, y: r.top + r.height / 2};
    }
    function firstVisible(selector) {
        var els = Array.from(document.querySelectorAll(selector));
        for (var i = 0; i < els.length; i++) {
            if (isVisible(els[i])) return els[i];
        }
        return null;
    }

    // --- BUY: div.btn.btn-success (confirmed primary) ---
    var buyEl = firstVisible('div.btn.btn-success');
    var buyInfo = buyEl ? {selector: 'div.btn.btn-success', x: center(buyEl).x, y: center(buyEl).y} : null;

    // --- SELL: div.btn.btn-danger  (confirmed primary, skip panic button) ---
    var sellEl = null;
    var dangerEls = Array.from(document.querySelectorAll('div.btn.btn-danger'));
    for (var i = 0; i < dangerEls.length; i++) {
        var el = dangerEls[i];
        if (!isVisible(el)) continue;
        var cls = (el.className || '').toLowerCase();
        if (cls.indexOf('panic') >= 0) continue;
        sellEl = el; break;
    }
    var sellInfo = sellEl ? {selector: 'div.btn.btn-danger:not(.panic-button)', x: center(sellEl).x, y: center(sellEl).y} : null;

    // --- EXIT: button.btn.btn-default (first non-dropdown, confirmed primary) ---
    var flatEl = null;
    var defEls = Array.from(document.querySelectorAll('button.btn.btn-default'));
    for (var j = 0; j < defEls.length; j++) {
        var fe = defEls[j];
        if (!isVisible(fe)) continue;
        var fcls = (fe.className || '').toLowerCase();
        if (fcls.indexOf('dropdown') >= 0) continue;
        flatEl = fe; break;
    }
    var flatInfo = flatEl ? {selector: 'button.btn.btn-default:not(.dropdown-toggle)', x: center(flatEl).x, y: center(flatEl).y} : null;

    return {buy: buyInfo, sell: sellInfo, flat: flatInfo};
})();
"""
        result = await self._send_cdp_command(
            "Runtime.evaluate",
            {"expression": scan_js, "returnByValue": True},
        )
        value = result.get("result", {}).get("value") or {}
        buy_info: dict[str, Any] | None = (
            dict(value["buy"]) if isinstance(value.get("buy"), dict) else None
        )
        sell_info: dict[str, Any] | None = (
            dict(value["sell"]) if isinstance(value.get("sell"), dict) else None
        )
        flat_info: dict[str, Any] | None = (
            dict(value["flat"]) if isinstance(value.get("flat"), dict) else None
        )
        _LOGGER.info(
            "[CDPHumanAdapter] auto_discover_trading_buttons → buy=%s sell=%s flat=%s",
            buy_info,
            sell_info,
            flat_info,
        )
        return {"buy": buy_info, "sell": sell_info, "flat": flat_info}

    async def get_spied_selector(self) -> str | None:
        """Read and clear ``window.lastClickedSelector`` from the active tab.

        Returns the CSS selector string captured by :meth:`inject_selector_spy`
        after the user clicked an element, then resets the value to ``null``
        so subsequent calls return ``None`` until a new spy is injected.

        Returns:
            The captured selector string, or ``None`` if no click has been
            recorded yet.
        """
        if not self._ws:
            raise CDPConnectionError("Not connected to Chrome")

        read_and_clear_js = """
(function() {
    var sel = window.lastClickedSelector || null;
    window.lastClickedSelector = null;
    return sel;
})();
"""
        result = await self._send_cdp_command(
            "Runtime.evaluate",
            {"expression": read_and_clear_js, "returnByValue": True},
        )
        value = result.get("result", {}).get("value")
        return str(value) if value is not None else None

    async def disconnect(self) -> None:
        """Disconnect from Chrome."""
        if self._ws:
            await self._ws.close()
        self._mark_disconnected()
        _LOGGER.info(f"[CDPHumanAdapter] Disconnected from {self.platform}")

    @property
    def is_connected(self) -> bool:
        """Return True only if the WebSocket is open and Chrome confirmed connection.

        Previously this returned the internal ``_connected`` flag which could
        stay ``True`` after a silent WebSocket drop.  Now we also verify that
        the underlying websockets socket reports it is NOT closed, so the
        pre-flight diagnostics and auto-reconnect logic always see the real
        connection state.
        """
        if not self._connected:
            return False
        ws = self._ws
        if ws is None:
            return False
        # websockets ≥ 10: WebSocketClientProtocol exposes .closed (bool)
        # and .close_code (None = still open).  Fall back gracefully for
        # older versions that only expose state via the open property.
        try:
            if hasattr(ws, "closed") and bool(ws.closed):
                return False
            if hasattr(ws, "open") and not bool(ws.open):
                return False
        except Exception:
            pass
        return True
