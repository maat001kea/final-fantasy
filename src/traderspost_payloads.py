"""Helpers for generating TradersPost webhook payloads."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from .custom_types import CustomStrategyConfig, coerce_custom_strategy_config

TRADERSPOST_ACTIONS: tuple[str, ...] = ("buy", "sell", "exit", "cancel", "add")
TRADERSPOST_ORDER_TYPES: tuple[str, ...] = ("market", "limit", "stop", "stop_limit", "trailing_stop")
TRADERSPOST_QUANTITY_TYPES: tuple[str, ...] = (
    "fixed_quantity",
    "dollar_amount",
    "risk_dollar_amount",
    "percent_of_equity",
    "percent_of_position",
)
TRADERSPOST_PRODUCT_TYPES: tuple[str, ...] = ("Futures", "CFD")

PROP_FIRM_OPTIONS_BY_PRODUCT_TYPE: dict[str, tuple[str, ...]] = {
    "Futures": ("Lucid Trading",),
    "CFD": ("FTMO",),
}

PROP_FIRM_SOURCE_URLS: dict[str, str] = {
    "Apex Trader Funding": "https://support.apextraderfunding.com/hc/en-us/articles/31519458697243-Tradovate-Commission-Instruments",
    "Lucid Trading": "https://support.lucidtrading.com/en/articles/11508978-approved-products-and-commissions",
    "FTMO": "https://ftmo.com/en/blog/trading-updates/trading-update-16-jan-2025/",
}

PROP_FIRM_RULE_SOURCES: dict[str, tuple[str, ...]] = {
    "Apex Trader Funding": (
        "https://support.apextraderfunding.com/hc/en-us/articles/45683414022299-Intraday-Trailing-Drawdown-Evaluations",
        "https://support.apextraderfunding.com/hc/en-us/articles/46724640813083-EOD-Evaluations",
        "https://support.apextraderfunding.com/hc/en-us/articles/47257193113371-Daily-Loss-Limit-Explained",
        "https://support.apextraderfunding.com/hc/en-us/articles/40463165052955-Contract-Scaling-Rule",
    ),
    "Lucid Trading": (
        "https://help.lucidtrading.com/our-rules-and-parameters",
        "https://help.lucidtrading.com/lucidpro",
        "https://support.lucidtrading.com/en/articles/11508978-approved-products-and-commissions",
    ),
    "FTMO": (
        "https://ftmo.com/en/faq/",
        "https://ftmo.com/en/symbols/",
    ),
}

PROP_FIRM_ACCOUNT_RULES: dict[str, dict[str, dict[str, Any]]] = {
    "Apex Trader Funding": {
        "25K": {
            "account_size_usd": 25000,
            "max_loss_limit_usd": 1000.0,
            "daily_loss_limit_usd": 500.0,
            "max_contracts": 2,
            "max_contracts_eval": 4,
            "fee_per_side_usd_mym": 0.52,
            "price_monthly_usd": None,
            "notes": (
                "PA har DLL/tiers; eval og PA kan have forskellige limits.",
                "Hold altid margin under officielle ruleside (kan ændres).",
            ),
        },
        "50K": {
            "account_size_usd": 50000,
            "max_loss_limit_usd": 2000.0,
            "daily_loss_limit_usd": 1000.0,
            "max_contracts": 4,
            "max_contracts_eval": 6,
            "fee_per_side_usd_mym": 0.52,
            "price_monthly_usd": None,
            "notes": (
                "PA har DLL/tiers; eval og PA kan have forskellige limits.",
                "Max contracts i PA afhænger af scaling-tier.",
            ),
        },
        "100K": {
            "account_size_usd": 100000,
            "max_loss_limit_usd": 3000.0,
            "daily_loss_limit_usd": None,
            "max_contracts": 7,
            "max_contracts_eval": 14,
            "fee_per_side_usd_mym": 0.52,
            "price_monthly_usd": None,
            "notes": (
                "Bekræft PA DLL og scaling-tier for 100K i portal før live.",
            ),
        },
        "150K": {
            "account_size_usd": 150000,
            "max_loss_limit_usd": 5000.0,
            "daily_loss_limit_usd": None,
            "max_contracts": 10,
            "max_contracts_eval": 17,
            "fee_per_side_usd_mym": 0.52,
            "price_monthly_usd": None,
            "notes": (
                "Bekræft PA DLL og scaling-tier for 150K i portal før live.",
            ),
        },
    },
    "Lucid Trading": {
        "25K": {
            "account_size_usd": 25000,
            "max_loss_limit_usd": 1500.0,
            "daily_loss_limit_usd": None,
            "max_contracts": 3,
            "max_contracts_eval": 3,
            "fee_per_side_usd_mym": None,
            "price_monthly_usd": 83.0,
            "notes": (
                "Lucid har flere programmer (Pro/Flex/Black/Direct) med forskellige regler.",
            ),
        },
        "50K": {
            "account_size_usd": 50000,
            "max_loss_limit_usd": 2500.0,
            "daily_loss_limit_usd": None,
            "max_contracts": 6,
            "max_contracts_eval": 6,
            "fee_per_side_usd_mym": None,
            "price_monthly_usd": 138.0,
            "notes": (
                "Lucid har flere programmer (Pro/Flex/Black/Direct) med forskellige regler.",
            ),
        },
        "100K": {
            "account_size_usd": 100000,
            "max_loss_limit_usd": 3500.0,
            "daily_loss_limit_usd": None,
            "max_contracts": 12,
            "max_contracts_eval": 12,
            "fee_per_side_usd_mym": None,
            "price_monthly_usd": 219.0,
            "notes": (
                "Lucid har flere programmer (Pro/Flex/Black/Direct) med forskellige regler.",
            ),
        },
        "150K": {
            "account_size_usd": 150000,
            "max_loss_limit_usd": 4500.0,
            "daily_loss_limit_usd": None,
            "max_contracts": 15,
            "max_contracts_eval": 15,
            "fee_per_side_usd_mym": None,
            "price_monthly_usd": 303.0,
            "notes": (
                "Lucid har flere programmer (Pro/Flex/Black/Direct) med forskellige regler.",
            ),
        },
    },
}


def _round_risk_bucket(value: float) -> float:
    return float(max(5.0, 5.0 * round(float(value) / 5.0)))


def _infer_account_size_usd(account_size: str) -> int:
    token = "".join(ch for ch in str(account_size or "") if ch.isdigit())
    if not token:
        return 0
    if token.endswith("000"):
        return int(token)
    if token.endswith("00"):
        return int(token)
    return int(token) * 1000


def prop_firm_account_sizes(prop_firm: str) -> tuple[str, ...]:
    rules = PROP_FIRM_ACCOUNT_RULES.get(str(prop_firm or "").strip(), {})
    if not rules:
        return ()
    return tuple(rules.keys())


def prop_firm_account_rule(prop_firm: str, account_size: str) -> dict[str, Any]:
    firm_token = str(prop_firm or "").strip()
    size_token = str(account_size or "").strip()
    firm_rules = PROP_FIRM_ACCOUNT_RULES.get(firm_token, {})
    account_rule = firm_rules.get(size_token)
    if isinstance(account_rule, dict):
        return dict(account_rule)
    account_size_usd = _infer_account_size_usd(size_token)
    fallback_loss = float(account_size_usd) * 0.04 if account_size_usd > 0 else 1000.0
    return {
        "account_size_usd": account_size_usd,
        "max_loss_limit_usd": float(fallback_loss),
        "daily_loss_limit_usd": None,
        "max_contracts": None,
        "max_contracts_eval": None,
        "fee_per_side_usd_mym": None,
        "price_monthly_usd": None,
        "notes": ("Ingen verificeret firm-specifik account-regel fundet i app-profilen.",),
    }


def prop_firm_risk_profiles(prop_firm: str, account_size: str) -> dict[str, dict[str, Any]]:
    rule = prop_firm_account_rule(prop_firm=prop_firm, account_size=account_size)
    max_loss = _safe_positive_float(rule.get("max_loss_limit_usd")) or 1000.0
    max_contracts = int(max(1, int(rule.get("max_contracts") or 1)))
    conservative_risk = _round_risk_bucket(float(max_loss) * 0.03)
    balanced_risk = _round_risk_bucket(float(max_loss) * 0.05)
    aggressive_risk = _round_risk_bucket(float(max_loss) * 0.08)

    return {
        "Conservative": {
            "risk_usd": float(conservative_risk),
            "max_add_to_winners": 0,
            "sizing_mode": "risk_auto",
            "description": "Lav aggressivitet med fokus på drawdown-beskyttelse.",
        },
        "Balanced": {
            "risk_usd": float(balanced_risk),
            "max_add_to_winners": min(1, max_contracts - 1),
            "sizing_mode": "risk_auto",
            "description": "Mellemprofil for stabilitet og rimelig frekvens.",
        },
        "Aggressive": {
            "risk_usd": float(aggressive_risk),
            "max_add_to_winners": min(2, max_contracts - 1),
            "sizing_mode": "risk_auto",
            "description": "Højere risiko. Brug kun med stram kill-switch.",
        },
    }


def suggest_contracts_from_risk(
    *,
    ticker: str,
    risk_usd: float | int,
    stop_loss_points: float | int,
    max_contracts: int | None = None,
) -> int:
    risk_token = _safe_positive_float(risk_usd)
    stop_token = _safe_positive_float(stop_loss_points)
    if risk_token is None or stop_token is None:
        return 1
    spec = _resolve_futures_contract_spec(str(ticker or ""))
    point_value = float(spec.point_value_usd) if spec is not None else 1.0
    risk_per_contract = float(stop_token) * point_value
    if risk_per_contract <= 0:
        return 1
    qty = int(float(risk_token) // risk_per_contract)
    qty = max(1, qty)
    if max_contracts is not None:
        qty = min(qty, max(1, int(max_contracts)))
    return int(qty)


@dataclass(frozen=True)
class _InstrumentSpec:
    ticker: str
    product_name: str
    exchange: str
    notes: str = ""


@dataclass(frozen=True)
class _FuturesContractSpec:
    ticker: str
    exchange: str
    currency: str
    tick_size: float
    tick_value_usd: float
    point_value_usd: float
    min_contracts: int = 1
    contract_type: str = "micro"
    description: str = ""


APEX_FUTURES_MAP: dict[str, _InstrumentSpec] = {
    "DAX": _InstrumentSpec("FDXS", "Micro DAX Index Futures", "EUREX"),
    "EURO STOXX 50": _InstrumentSpec("FSXE", "Micro Euro Stoxx 50 Futures", "EUREX"),
    "DOW": _InstrumentSpec("MYM", "Micro E-Mini Dow Futures", "CBOT"),
    "S&P 500": _InstrumentSpec("MES", "Micro E-Mini S&P 500 Futures", "CME"),
    "NASDAQ 100": _InstrumentSpec("MNQ", "Micro E-Mini Nasdaq-100 Futures", "CME"),
    "RUSSELL 2000": _InstrumentSpec("M2K", "Micro E-Mini Russell 2000 Futures", "CME"),
    "WTI CRUDE OIL": _InstrumentSpec("MCL", "Micro Crude Oil Futures", "NYMEX"),
}

LUCID_FUTURES_MAP: dict[str, _InstrumentSpec] = {
    "DOW": _InstrumentSpec("MYM", "Micro E-mini Dow Jones Industrial Average Index Futures", "CBOT"),
    "S&P 500": _InstrumentSpec("MES", "Micro E-mini S&P 500 Index Futures", "CME"),
    "NASDAQ 100": _InstrumentSpec("MNQ", "Micro E-mini Nasdaq-100 Index Futures", "CME"),
    "RUSSELL 2000": _InstrumentSpec("M2K", "Micro E-mini Russell 2000 Index Futures", "CME"),
    "WTI CRUDE OIL": _InstrumentSpec("MCL", "Micro Crude Oil", "NYMEX"),
}

FTMO_CFD_MAP: dict[str, _InstrumentSpec] = {
    "DAX": _InstrumentSpec("GER40.cash", "Germany 40 Cash CFD", "Cash CFD"),
    "EURO STOXX 50": _InstrumentSpec("EU50.cash", "Euro Stoxx 50 Cash CFD", "Cash II CFD"),
    "CAC 40": _InstrumentSpec("FRA40.cash", "France 40 Cash CFD", "Cash II CFD"),
    "AEX": _InstrumentSpec(
        "N25.cash",
        "Netherlands 25 Cash CFD",
        "Cash II CFD",
        notes="Symbol name inferred from FTMO Cash II symbol list naming.",
    ),
    "IBEX 35": _InstrumentSpec("SPN35.cash", "Spain 35 Cash CFD", "Cash II CFD"),
    "FTSE": _InstrumentSpec("UK100.cash", "UK 100 Cash CFD", "Cash CFD"),
    "DOW": _InstrumentSpec("US30.cash", "US 30 Cash CFD", "Cash CFD"),
    "S&P 500": _InstrumentSpec("US500.cash", "US 500 Cash CFD", "Cash CFD"),
    "NASDAQ 100": _InstrumentSpec("US100.cash", "US 100 Cash CFD", "Cash CFD"),
    "RUSSELL 2000": _InstrumentSpec("US2000.cash", "US 2000 Cash CFD", "Cash II CFD"),
    "WTI CRUDE OIL": _InstrumentSpec("USOIL.cash", "US Oil Cash CFD", "Cash II CFD"),
    "BRENT CRUDE OIL": _InstrumentSpec("UKOIL.cash", "UK Oil Cash CFD", "Cash II CFD"),
}

PROP_FIRM_INSTRUMENT_MAPS: dict[tuple[str, str], dict[str, _InstrumentSpec]] = {
    ("Futures", "Apex Trader Funding"): APEX_FUTURES_MAP,
    ("Futures", "Lucid Trading"): LUCID_FUTURES_MAP,
    ("CFD", "FTMO"): FTMO_CFD_MAP,
}


MICRO_FUTURES_TICKERS: set[str] = {"FDXS", "FSXE", "MYM", "MES", "MNQ", "M2K", "MCL"}

FUTURES_CONTRACT_SPECS: dict[str, _FuturesContractSpec] = {
    "MYM": _FuturesContractSpec(
        ticker="MYM",
        exchange="CBOT",
        currency="USD",
        tick_size=1.0,
        tick_value_usd=0.5,
        point_value_usd=0.5,
        min_contracts=1,
        contract_type="micro",
        description="Micro E-mini Dow Jones futures",
    ),
    "MES": _FuturesContractSpec(
        ticker="MES",
        exchange="CME",
        currency="USD",
        tick_size=0.25,
        tick_value_usd=1.25,
        point_value_usd=5.0,
        min_contracts=1,
        contract_type="micro",
        description="Micro E-mini S&P 500 futures",
    ),
    "MNQ": _FuturesContractSpec(
        ticker="MNQ",
        exchange="CME",
        currency="USD",
        tick_size=0.25,
        tick_value_usd=0.5,
        point_value_usd=2.0,
        min_contracts=1,
        contract_type="micro",
        description="Micro E-mini Nasdaq-100 futures",
    ),
    "M2K": _FuturesContractSpec(
        ticker="M2K",
        exchange="CME",
        currency="USD",
        tick_size=0.1,
        tick_value_usd=0.5,
        point_value_usd=5.0,
        min_contracts=1,
        contract_type="micro",
        description="Micro E-mini Russell 2000 futures",
    ),
}


def default_traderspost_ticker(
    instrument: str,
    symbol_map: Mapping[str, str] | None = None,
) -> str:
    """Resolve a default TradersPost ticker from instrument name and symbol map."""
    instrument_token = str(instrument or "").strip()
    if not instrument_token:
        return ""
    if isinstance(symbol_map, Mapping):
        mapped = str(symbol_map.get(instrument_token, "")).strip()
        if mapped:
            return mapped
    return instrument_token


def _normalize_action(value: Any) -> str:
    token = str(value or "").strip().lower()
    return token if token in TRADERSPOST_ACTIONS else "buy"


def _normalize_order_type(value: Any) -> str:
    token = str(value or "").strip().lower()
    return token if token in TRADERSPOST_ORDER_TYPES else "market"


def _normalize_quantity_type(value: Any) -> str:
    token = str(value or "").strip().lower()
    return token if token in TRADERSPOST_QUANTITY_TYPES else "fixed_quantity"


def _safe_positive_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number <= 0:
        return None
    return number


def _normalize_product_type(value: Any) -> str:
    token = str(value or "").strip().lower()
    if token == "cfd":
        return "CFD"
    return "Futures"


def prop_firms_for_product_type(product_type: str) -> tuple[str, ...]:
    """Return supported prop firms for the chosen product type."""
    normalized = _normalize_product_type(product_type)
    return PROP_FIRM_OPTIONS_BY_PRODUCT_TYPE.get(normalized, ())


def _is_micro_futures_spec(spec: _InstrumentSpec) -> bool:
    ticker_token = str(spec.ticker or "").strip().upper()
    if ticker_token in MICRO_FUTURES_TICKERS:
        return True
    product_token = str(spec.product_name or "").strip().lower()
    return "micro" in product_token


def resolve_propfirm_instrument(
    *,
    instrument: str,
    product_type: str,
    prop_firm: str,
) -> dict[str, Any]:
    """Resolve instrument into firm-specific ticker/product naming."""
    normalized_product = _normalize_product_type(product_type)
    firm_token = str(prop_firm or "").strip()
    allowed_firms = PROP_FIRM_OPTIONS_BY_PRODUCT_TYPE.get(normalized_product, ())
    if not allowed_firms:
        return {
            "supported": False,
            "productType": normalized_product,
            "propFirm": firm_token,
            "message": "Ingen prop firms understottes for valgt produkttype.",
            "supportedInstruments": [],
        }

    if firm_token not in allowed_firms:
        firm_token = allowed_firms[0]

    mapping = PROP_FIRM_INSTRUMENT_MAPS.get((normalized_product, firm_token), {})
    instrument_token = str(instrument or "").strip()
    spec = mapping.get(instrument_token)
    if spec is None:
        return {
            "supported": False,
            "productType": normalized_product,
            "propFirm": firm_token,
            "instrument": instrument_token,
            "message": (
                f"{instrument_token or 'Instrument'} er ikke i den understottede {normalized_product.lower()}-liste "
                f"for {firm_token}."
            ),
            "supportedInstruments": sorted(mapping.keys()),
            "source": PROP_FIRM_SOURCE_URLS.get(firm_token, ""),
        }

    if normalized_product == "Futures" and not _is_micro_futures_spec(spec):
        return {
            "supported": False,
            "productType": normalized_product,
            "propFirm": firm_token,
            "instrument": instrument_token,
            "message": (
                f"{instrument_token or 'Instrument'} hos {firm_token} er ikke en micro futures-kontrakt. "
                "Vælg kun micro futures."
            ),
            "supportedInstruments": sorted(mapping.keys()),
            "source": PROP_FIRM_SOURCE_URLS.get(firm_token, ""),
        }

    return {
        "supported": True,
        "productType": normalized_product,
        "propFirm": firm_token,
        "instrument": instrument_token,
        "ticker": spec.ticker,
        "productName": spec.product_name,
        "exchange": spec.exchange,
        "notes": spec.notes,
        "isMicroContract": _is_micro_futures_spec(spec) if normalized_product == "Futures" else False,
        "source": PROP_FIRM_SOURCE_URLS.get(firm_token, ""),
    }


def _school_run_metadata(
    cfg: CustomStrategyConfig,
    extras: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "source": "Final Fantasy School Run",
        "strategy": "Custom School Run",
        "instrument": str(cfg.instrument),
        "timeframe": str(cfg.timeframe),
        "executionModel": str(cfg.execution_model),
        "bar1StartDK": str(cfg.bar1_start),
        "lookbackSessions": int(cfg.lookback_sessions),
        "schoolRunDefaults": {
            "directionMode": str(cfg.direction_mode),
            "entryOffsetPts": float(cfg.entry_offset_pts),
            "startFromBar": 3,
            "maxTriggerBars": int(cfg.max_trigger_bars),
            "allowLong": True,
            "allowShort": True,
            "longEntryRule": "bar2_high + entry_offset_pts",
            "longStopRule": "bar2_low",
            "shortEntryRule": "bar2_low - entry_offset_pts",
            "shortStopRule": "bar2_high",
        },
    }
    if isinstance(extras, Mapping):
        for key, value in extras.items():
            metadata[str(key)] = value
    return metadata


def _resolve_futures_contract_spec(ticker: str) -> _FuturesContractSpec | None:
    token = str(ticker or "").strip().upper()
    if not token:
        return None
    return FUTURES_CONTRACT_SPECS.get(token)


def _estimate_contract_quantity_from_risk(
    *,
    risk_usd: float,
    stop_ticks: int,
    tick_value_usd: float,
    min_contracts: int,
) -> tuple[int, float] | None:
    if risk_usd <= 0:
        return None
    if stop_ticks <= 0 or tick_value_usd <= 0:
        return None
    risk_per_contract = float(stop_ticks) * float(tick_value_usd)
    if risk_per_contract <= 0:
        return None
    contracts = int(float(risk_usd) // risk_per_contract)
    contracts = max(int(min_contracts), contracts)
    return contracts, risk_per_contract


def build_custom_traderspost_auto_payload(
    config: CustomStrategyConfig | dict[str, Any] | None,
    *,
    product_type: str = "Futures",
    prop_firm: str = "Lucid Trading",
    prop_account_size: str | None = None,
    risk_profile: str | None = None,
    max_add_to_winners: int | None = None,
    force_fixed_contract_quantity: bool = True,
    risk_usd: float | int = 100.0,
    stop_loss_amount: float | int | None = None,
    signal_id: str | None = None,
    position_key: str | None = None,
    include_sentiment: bool = False,
    force_signal_price: bool = True,
    signal_price: float | int | None = None,
) -> dict[str, Any]:
    """Build auto TradersPost JSON from active Custom: School Run config."""
    cfg = coerce_custom_strategy_config(config)
    resolved = resolve_propfirm_instrument(
        instrument=str(cfg.instrument),
        product_type=product_type,
        prop_firm=prop_firm,
    )
    if not bool(resolved.get("supported")):
        raise ValueError(str(resolved.get("message", "Instrument mapping failed.")))

    risk_token = _safe_positive_float(risk_usd)
    risk_value = float(risk_token) if risk_token is not None else 100.0
    stop_token = _safe_positive_float(stop_loss_amount)

    extras: dict[str, Any] = {
        "autoGenerated": True,
        "notes": "Built from Custom: School Run (Simple).",
        "productType": str(resolved.get("productType", "")),
        "propFirm": str(resolved.get("propFirm", "")),
        "productName": str(resolved.get("productName", "")),
        "exchange": str(resolved.get("exchange", "")),
        "microFuturesOnly": bool(resolved.get("isMicroContract", False)) if str(resolved.get("productType", "")) == "Futures" else False,
        "mappingSource": str(resolved.get("source", "")),
    }
    notes = str(resolved.get("notes", "")).strip()
    if notes:
        extras["mappingNotes"] = notes
    if prop_account_size:
        extras["propAccountSize"] = str(prop_account_size)
    if risk_profile:
        extras["riskProfile"] = str(risk_profile)
    if max_add_to_winners is not None:
        extras["maxAddToWinners"] = int(max(0, int(max_add_to_winners)))

    contract_spec = None
    if str(resolved.get("productType", "")).strip() == "Futures":
        contract_spec = _resolve_futures_contract_spec(str(resolved.get("ticker", "")))
        if contract_spec is not None:
            extras["futuresContract"] = {
                "ticker": contract_spec.ticker,
                "exchange": contract_spec.exchange,
                "currency": contract_spec.currency,
                "tickSize": float(contract_spec.tick_size),
                "tickValueUsd": float(contract_spec.tick_value_usd),
                "pointValueUsd": float(contract_spec.point_value_usd),
                "minContracts": int(contract_spec.min_contracts),
                "contractType": contract_spec.contract_type,
                "description": contract_spec.description,
            }

    signal_price_token = _safe_positive_float(signal_price)
    resolved_signal_price: float | str
    if signal_price_token is not None:
        resolved_signal_price = float(signal_price_token)
    elif force_signal_price:
        resolved_signal_price = 1.0
    else:
        resolved_signal_price = "{{close}}"

    payload: dict[str, Any] = {
        "ticker": str(resolved.get("ticker", "")).strip(),
        "action": "{{strategy.order.action}}",
        "orderType": "market",
        "signalPrice": resolved_signal_price,
        "quantityType": "risk_dollar_amount",
        "quantity": float(risk_value),
        "time": "{{timenow}}",
        "interval": "{{interval}}",
        "extras": _school_run_metadata(cfg, extras=extras),
    }
    if include_sentiment:
        payload["sentiment"] = "{{strategy.market_position}}"
    if signal_id:
        payload["extras"]["signalId"] = str(signal_id)
    if position_key:
        payload["extras"]["positionKey"] = str(position_key)
    payload["extras"]["strategyProfile"] = "aggressive"
    payload["extras"]["executionModelCanonical"] = "aggressive"
    manual_contract_override = False
    manual_contracts: int | None = None
    if str(resolved.get("productType", "")).strip() == "Futures" and bool(force_fixed_contract_quantity):
        symbol_token = str(getattr(cfg, "contract_symbol", "") or "").strip().upper()
        manual_qty_token = _safe_positive_float(getattr(cfg, "contract_quantity", None))
        if symbol_token and manual_qty_token is not None:
            manual_contract_override = True
            manual_contracts = max(1, int(round(float(manual_qty_token))))
            payload["quantityType"] = "fixed_quantity"
            payload["quantity"] = int(manual_contracts)
            payload["extras"]["orderSizing"] = {
                "mode": "fixed_contract_quantity",
                "contracts": int(manual_contracts),
                "source": "custom_futures_config",
            }

    if stop_token is not None:
        payload["stopLoss"] = {
            "type": "stop",
            "amount": float(stop_token),
        }
        if contract_spec is not None and float(contract_spec.tick_size) > 0:
            tick_size = float(contract_spec.tick_size)
            raw_stop_ticks = float(stop_token) / tick_size
            stop_ticks = max(1, int(round(raw_stop_ticks)))
            aligned_stop_amount = float(stop_ticks) * tick_size

            # For futures, align stop distance to valid contract ticks.
            payload["stopLoss"]["amount"] = float(aligned_stop_amount)
            payload["extras"]["stopLossTicks"] = int(stop_ticks)
            payload["extras"]["stopLossUnit"] = "points"
            payload["extras"]["stopLossPoints"] = float(aligned_stop_amount)
            payload["extras"]["stopLossInputPoints"] = float(stop_token)

            sizing = _estimate_contract_quantity_from_risk(
                risk_usd=float(risk_value),
                stop_ticks=stop_ticks,
                tick_value_usd=float(contract_spec.tick_value_usd),
                min_contracts=int(contract_spec.min_contracts),
            )
            if sizing is not None:
                contracts, risk_per_contract = sizing
                estimated_total_risk = float(contracts) * float(risk_per_contract)
                sizing_payload = {
                    "mode": "contracts_from_risk_and_ticks",
                    "riskUsdRequested": float(risk_value),
                    "riskPerContractUsd": float(risk_per_contract),
                    "estimatedTotalRiskUsd": float(estimated_total_risk),
                    "contracts": int(contracts),
                    "tickValueUsd": float(contract_spec.tick_value_usd),
                    "tickSize": float(contract_spec.tick_size),
                }
                if manual_contract_override:
                    payload["extras"]["orderSizingSuggested"] = sizing_payload
                    payload["extras"]["orderSizing"]["riskUsdRequested"] = float(risk_value)
                    payload["extras"]["orderSizing"]["estimatedTotalRiskUsd"] = float(manual_contracts or 1) * float(
                        risk_per_contract
                    )
                else:
                    payload["quantityType"] = "fixed_quantity"
                    payload["quantity"] = int(contracts)
                    payload["extras"]["orderSizing"] = sizing_payload
    else:
        payload["stopLoss"] = {
            "type": "stop",
            "percent": 1.0,
        }
        payload["extras"]["stopLossFallbackPercent"] = 1.0
    return payload


def build_aggressive_action_payload(
    *,
    action: Literal["buy", "sell", "add", "exit"],
    ticker: str,
    signal_price: float | int,
    quantity: int | float | None = 1,
    quantity_type: str = "fixed_quantity",
    order_type: str = "market",
    stop_loss_amount: float | int | None = None,
    signal_id: str | None = None,
    position_key: str | None = None,
    include_sentiment: bool = False,
    interval: str | int = "15",
    timestamp: str = "{{timenow}}",
    source: str = "Final Fantasy School Run",
    extras: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build strict TradersPost payload for aggressive live execution."""
    resolved_action = _normalize_action(action)
    if resolved_action not in {"buy", "sell", "add", "exit"}:
        raise ValueError(f"Unsupported aggressive action: {action}")

    ticker_token = str(ticker or "").strip().upper()
    if not ticker_token:
        raise ValueError("ticker is required")

    signal_price_token = _safe_positive_float(signal_price)
    if signal_price_token is None:
        raise ValueError("signal_price must be a positive number")

    payload: dict[str, Any] = {
        "ticker": ticker_token,
        "action": resolved_action,
        "orderType": _normalize_order_type(order_type),
        "signalPrice": float(signal_price_token),
        "time": str(timestamp),
        "interval": str(interval),
    }

    if resolved_action in {"buy", "sell", "add"}:
        qty = _safe_positive_float(quantity)
        if qty is None:
            raise ValueError("quantity must be positive for buy/sell/add")
        payload["quantityType"] = _normalize_quantity_type(quantity_type)
        payload["quantity"] = float(qty)

    if resolved_action in {"buy", "sell"} and include_sentiment:
        payload["sentiment"] = "bullish" if resolved_action == "buy" else "bearish"

    if resolved_action in {"buy", "sell"} and stop_loss_amount is not None:
        stop_token = _safe_positive_float(stop_loss_amount)
        if stop_token is None:
            raise ValueError("stop_loss_amount must be positive when provided")
        payload["stopLoss"] = {"type": "stop", "amount": float(stop_token)}

    metadata: dict[str, Any] = {
        "source": str(source),
        "strategyProfile": "aggressive",
        "executionModel": "aggressive",
    }
    if signal_id:
        metadata["signalId"] = str(signal_id)
    if position_key:
        metadata["positionKey"] = str(position_key)
    if isinstance(extras, Mapping):
        for key, value in extras.items():
            metadata[str(key)] = value
    payload["extras"] = metadata
    return payload


def build_custom_traderspost_payload(
    config: CustomStrategyConfig | dict[str, Any] | None,
    *,
    ticker: str,
    action: str = "buy",
    order_type: str = "market",
    quantity: float | int | None = 1,
    quantity_type: str = "fixed_quantity",
    signal_price: float | int | None = None,
    limit_price: float | int | None = None,
    stop_price: float | int | None = None,
    trail_amount: float | int | None = None,
    trail_percent: float | int | None = None,
    extras: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a TradersPost-compatible JSON payload from Custom strategy config."""
    cfg = coerce_custom_strategy_config(config)
    resolved_action = _normalize_action(action)
    resolved_order_type = _normalize_order_type(order_type)
    resolved_quantity_type = _normalize_quantity_type(quantity_type)

    ticker_token = str(ticker or "").strip() or str(cfg.instrument).strip() or "DAX"
    payload: dict[str, Any] = {
        "ticker": ticker_token,
        "action": resolved_action,
        "orderType": resolved_order_type,
    }

    if resolved_action in {"buy", "sell", "add"}:
        qty = _safe_positive_float(quantity)
        if qty is not None:
            payload["quantity"] = qty
        payload["quantityType"] = resolved_quantity_type

    maybe_signal_price = _safe_positive_float(signal_price)
    if maybe_signal_price is not None:
        payload["signalPrice"] = maybe_signal_price
    maybe_limit = _safe_positive_float(limit_price)
    if maybe_limit is not None:
        payload["limitPrice"] = maybe_limit
    maybe_stop = _safe_positive_float(stop_price)
    if maybe_stop is not None:
        payload["stopPrice"] = maybe_stop
    maybe_trail_amount = _safe_positive_float(trail_amount)
    if maybe_trail_amount is not None:
        payload["trailAmount"] = maybe_trail_amount
    maybe_trail_percent = _safe_positive_float(trail_percent)
    if maybe_trail_percent is not None:
        payload["trailPercent"] = maybe_trail_percent

    payload["extras"] = _school_run_metadata(cfg, extras=extras)
    return payload
