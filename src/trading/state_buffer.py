"""StateVerificationBuffer – filters transient browser UI flicker.

The engine polls the Tradovate browser UI in a tight loop.  A single
"wrong" DOM reading (e.g. the order-confirmation overlay causing a
momentary "FLAT" flash while a LONG position is being entered) must
never be reported to the SQLite bridge as a genuine state change.

``StateVerificationBuffer`` solves this by requiring *N* consecutive
identical readings before accepting a value as confirmed truth.

Usage::

    from src.trading.state_buffer import StateVerificationBuffer

    buf = StateVerificationBuffer(threshold=3, name="position")
    buf.update("LONG")  # pending (1/3)
    buf.update("LONG")  # pending (2/3)
    buf.update("FLAT")  # flicker! count reset to (1/3)
    buf.update("LONG")  # back to (1/3) – previous LONG run restarted
    buf.update("LONG")  # (2/3)
    buf.update("LONG")  # confirmed → True returned
    buf.get_confirmed_state()  # → "LONG"
"""

from __future__ import annotations

import logging
from typing import Any

_LOGGER = logging.getLogger("final_fantasy.state_buffer")

_SENTINEL = object()

STATE_VERIFICATION_REQUIRED_CONFIRMATIONS: int = 3


class StateVerificationBuffer:
    """Gate state changes behind *threshold* consecutive identical readings.

    Parameters
    ----------
    threshold:
        Number of identical readings required before a value is confirmed.
        Also accepted as the keyword ``required_confirmations`` for backwards
        compatibility.
    name:
        Optional label used in log messages to identify which field this
        buffer is watching (e.g. ``"position"``, ``"account"``).
    """

    def __init__(
        self,
        threshold: int = STATE_VERIFICATION_REQUIRED_CONFIRMATIONS,
        *,
        name: str = "",
        required_confirmations: int | None = None,
    ) -> None:
        # ``required_confirmations`` is a backwards-compat alias for ``threshold``
        if required_confirmations is not None:
            threshold = required_confirmations
        self._threshold: int = max(1, int(threshold))
        self._name: str = str(name or "")
        self._pending: Any = _SENTINEL
        self._pending_count: int = 0
        self._confirmed: Any = _SENTINEL

    # ------------------------------------------------------------------
    # Primary API
    # ------------------------------------------------------------------

    def update(self, new_state: Any) -> bool:
        """Record a new reading and return ``True`` when a state is confirmed.

        A state is *confirmed* when *new_state* has been seen
        ``threshold`` consecutive times **and** it differs from the
        currently confirmed value.

        Logging
        -------
        * State candidate changes are logged at DEBUG as
          ``State change detected (OLD → NEW), waiting for confirmation (1/N)``.
        * Each confirmation step is logged as
          ``State change detected (OLD → NEW), waiting for confirmation (K/N)``.
        * Confirmation is logged at DEBUG as
          ``State confirmed: NEW (after K reads)``.
        """
        label = f"[{self._name}] " if self._name else ""

        if new_state == self._pending:
            self._pending_count += 1
            _LOGGER.debug(
                "%sState change detected (%s → %s), waiting for confirmation (%d/%d)",
                label,
                self._confirmed if self._confirmed is not _SENTINEL else "?",
                new_state,
                self._pending_count,
                self._threshold,
            )
        else:
            old_pending = self._pending
            _LOGGER.debug(
                "%sState change detected (%s → %s), waiting for confirmation (1/%d)",
                label,
                old_pending if old_pending is not _SENTINEL else "?",
                new_state,
                self._threshold,
            )
            self._pending = new_state
            self._pending_count = 1

        if self._pending_count >= self._threshold and new_state != self._confirmed:
            _LOGGER.debug(
                "%sState confirmed: %s (after %d reads)",
                label,
                new_state,
                self._pending_count,
            )
            self._confirmed = new_state
            return True

        return False

    def get_confirmed_state(self) -> Any:
        """Return the last confirmed state, or the internal sentinel if none yet."""
        return self._confirmed

    def get_pending_info(self) -> dict[str, Any]:
        """Return a snapshot of the current pending state for logging/inspection.

        Returns a dict with keys:
        ``pending`` – current candidate value (or sentinel if none),
        ``count`` – how many consecutive reads of the candidate so far,
        ``threshold`` – required reads to confirm,
        ``confirmed`` – currently confirmed value (or sentinel).
        """
        return {
            "pending": self._pending,
            "count": self._pending_count,
            "threshold": self._threshold,
            "confirmed": self._confirmed,
        }

    def reset(self) -> None:
        """Clear the buffer entirely (e.g. after an adapter reconnect)."""
        self._pending = _SENTINEL
        self._pending_count = 0
        self._confirmed = _SENTINEL
        if self._name:
            _LOGGER.debug("[%s] StateVerificationBuffer reset.", self._name)

    # ------------------------------------------------------------------
    # Backwards-compatibility shims (previous implementation used these)
    # ------------------------------------------------------------------

    def observe(self, value: Any) -> bool:
        """Alias for :meth:`update` kept for backwards compatibility."""
        return self.update(value)

    @property
    def confirmed_value(self) -> Any:
        """Alias for :meth:`get_confirmed_state` kept for backwards compatibility."""
        return self._confirmed
