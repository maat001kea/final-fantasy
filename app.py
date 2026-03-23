from __future__ import annotations

import json
from datetime import datetime, time, timezone
from html import escape
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
import yaml

from src.custom_engine import resolve_school_run_session_clock_dk
from src.custom_types import CustomStrategyConfig, coerce_custom_strategy_config
from src.trading.cdp_adapter import CDP_PORT, resolve_cdp_port
from src.trading.runtime_control import (
    launch_cdp_chrome,
    read_cdp_chrome_status,
    read_engine_service_status,
    start_engine_service,
    stop_cdp_chrome,
    stop_engine_service,
)
from src.trading_engine_bridge import enqueue_command, fetch_status, init_bridge


REPO_ROOT = Path(__file__).resolve().parent
BRIDGE_DB_PATH = REPO_ROOT / "output" / "trading_engine_bridge.sqlite3"
UI_SETTINGS_PATH = REPO_ROOT / "output" / "final_fantasy_settings.json"
ACCOUNT_CONFIG_PATH = REPO_ROOT / "config" / "account_config.yaml"
LIVE_CONFIG_PATH = REPO_ROOT / "config" / "live_execution.yaml"

DEFAULT_SETTINGS: dict[str, Any] = {
    "debug_port": CDP_PORT,
    "expected_account_token": "",
    "selector_mode": "Auto (platform selectors)",
    "buy_selector": "",
    "sell_selector": "",
    "flatten_selector": "",
    "fixed_contracts": 1,
    "bar1_start": "14:00",
    "chrome_headless": False,
}

MANAGEMENT_ROWS = [
    {"Rule": "Strategy", "Value": "School Run"},
    {"Rule": "Execution model", "Value": "Aggressiv"},
    {"Rule": "Break-even", "Value": "+0.25R"},
    {"Rule": "Trail activation", "Value": "+0.45R"},
    {"Rule": "Trail giveback", "Value": "0.20R"},
    {"Rule": "Add #1", "Value": "+0.55R"},
    {"Rule": "Add #2", "Value": "+1.10R"},
    {"Rule": "Max adds", "Value": "2"},
    {"Rule": "Add size", "Value": "1:1 med starter size"},
]


def _set_page_style() -> None:
    st.set_page_config(
        page_title="Final Fantasy",
        layout="wide",
        page_icon="⚡",
    )
    st.markdown(
        """
        <style>
            /* === FONTS === */
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

            /* === DESIGN TOKENS === */
            :root {
                --ff-green:        #22c55e;
                --ff-green-glow:   rgba(34,197,94,0.13);
                --ff-green-border: rgba(34,197,94,0.32);
                --ff-green-text:   #86efac;
                --ff-red:          #ef4444;
                --ff-red-glow:     rgba(239,68,68,0.13);
                --ff-red-border:   rgba(239,68,68,0.32);
                --ff-red-text:     #fca5a5;
                --ff-amber:        #f59e0b;
                --ff-amber-glow:   rgba(245,158,11,0.11);
                --ff-amber-border: rgba(245,158,11,0.30);
                --ff-amber-text:   #fcd34d;
                --ff-bg:           #0c0e14;
                --ff-surface:      #12151e;
                --ff-surface-2:    #191d28;
                --ff-surface-3:    #212636;
                --ff-border:       #252b3b;
                --ff-border-2:     #2e3548;
                --ff-text:         #e4e8f0;
                --ff-muted:        #64748b;
                --ff-muted-2:      #94a3b8;
                --ff-accent:       #22c55e;
                /* Heuristic 4 – Consistency: single accent throughout */
            }

            /* === ANIMATIONS (Heuristic 1 – Visibility of system status) === */
            @keyframes pulse-green {
                0%, 100% { box-shadow: 0 0 0 0 rgba(34,197,94,0.75); }
                60%       { box-shadow: 0 0 0 7px rgba(34,197,94,0);   }
            }
            @keyframes pulse-red {
                0%, 100% { box-shadow: 0 0 0 0 rgba(239,68,68,0.75); }
                60%       { box-shadow: 0 0 0 7px rgba(239,68,68,0);   }
            }
            @keyframes pulse-amber {
                0%, 100% { box-shadow: 0 0 0 0 rgba(245,158,11,0.65); }
                60%       { box-shadow: 0 0 0 6px rgba(245,158,11,0);   }
            }
            @keyframes fadeUp {
                from { opacity: 0; transform: translateY(-5px); }
                to   { opacity: 1; transform: translateY(0);    }
            }

            /* === BASE === */
            html, body, .stApp {
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                background: var(--ff-bg);
                color: var(--ff-text);
            }
            /* Subtle ambient background gradients */
            .stApp {
                background:
                    radial-gradient(ellipse 55% 35% at 0% 0%,   rgba(34,197,94,0.055) 0%, transparent 65%),
                    radial-gradient(ellipse 45% 30% at 100% 0%,  rgba(239,68,68,0.045) 0%, transparent 60%),
                    radial-gradient(ellipse 70% 50% at 50% 110%, rgba(99,102,241,0.03) 0%, transparent 70%),
                    var(--ff-bg);
            }

            /* === SIDEBAR (Heuristic 8 – Aesthetic & Minimalist) === */
            [data-testid="stSidebar"] {
                background: var(--ff-surface);
                border-right: 1px solid var(--ff-border);
            }
            [data-testid="stSidebar"] * { color: var(--ff-text); }
            [data-testid="stSidebar"] h3 {
                font-size: 0.78rem;
                font-weight: 800;
                text-transform: uppercase;
                letter-spacing: 0.1em;
                color: var(--ff-muted-2);
                margin-bottom: 12px;
                padding-left: 10px;
                border-left: 3px solid var(--ff-accent);
            }

            /* === INPUTS (Heuristic 5 – Error Prevention: clear focus states) === */
            div[data-baseweb="input"] > div,
            div[data-baseweb="select"] > div,
            [data-testid="stTimeInput"] input,
            [data-testid="stNumberInput"] input,
            [data-testid="stTextInput"] input {
                background: var(--ff-surface-2) !important;
                color: var(--ff-text) !important;
                border: 1px solid var(--ff-border-2) !important;
                border-radius: 10px !important;
                transition: border-color 0.2s, box-shadow 0.2s;
            }
            div[data-baseweb="input"]:focus-within > div,
            [data-testid="stTextInput"] input:focus {
                border-color: var(--ff-accent) !important;
                box-shadow: 0 0 0 3px var(--ff-green-glow) !important;
            }

            /* === BUTTONS (Heuristic 7 – Flexibility & Efficiency) === */
            .stButton > button,
            .stFormSubmitButton > button {
                background: var(--ff-surface-2);
                color: var(--ff-text);
                border: 1px solid var(--ff-border-2);
                border-radius: 10px;
                font-weight: 600;
                font-size: 0.845rem;
                padding: 9px 16px;
                letter-spacing: 0.01em;
                transition: all 0.16s ease;
                position: relative;
                overflow: hidden;
            }
            .stButton > button:hover,
            .stFormSubmitButton > button:hover {
                border-color: var(--ff-accent);
                color: #fff;
                background: var(--ff-surface-3);
                box-shadow: 0 0 0 1px var(--ff-green-border), 0 4px 14px rgba(34,197,94,0.14);
                transform: translateY(-1px);
            }
            .stButton > button:active { transform: translateY(0); }
            /* Heuristic 5: disabled buttons clearly unusable */
            .stButton > button:disabled,
            .stFormSubmitButton > button:disabled {
                background: var(--ff-surface);
                color: var(--ff-muted);
                border-color: var(--ff-border);
                opacity: 0.45;
                cursor: not-allowed;
                transform: none;
                box-shadow: none;
            }

            /* === METRICS === */
            div[data-testid="stMetric"] {
                background: var(--ff-surface);
                border: 1px solid var(--ff-border);
                border-radius: 14px;
                padding: 14px 18px;
            }
            div[data-testid="stMetricLabel"] p {
                color: var(--ff-muted);
                font-size: 0.75rem;
                text-transform: uppercase;
                letter-spacing: 0.09em;
                font-weight: 600;
            }
            div[data-testid="stMetricValue"] { color: var(--ff-text); font-weight: 800; }

            /* === ALERTS (Heuristic 9 – Help users recover from errors) === */
            .stAlert {
                border-radius: 12px;
                animation: fadeUp 0.3s ease;
            }

            /* === DIVIDER === */
            hr { border-color: var(--ff-border); margin: 22px 0; }

            /* ================================================================
               HERO (Heuristic 1 – Visibility; Heuristic 8 – Aesthetic)
               ================================================================ */
            .ff-hero {
                background: linear-gradient(140deg, var(--ff-surface) 0%, #151924 100%);
                border: 1px solid var(--ff-border);
                border-radius: 20px;
                padding: 22px 26px;
                margin-bottom: 18px;
                box-shadow: 0 4px 28px rgba(0,0,0,0.32), inset 0 1px 0 rgba(255,255,255,0.035);
                position: relative;
                overflow: hidden;
                animation: fadeUp 0.4s ease;
            }
            /* Green accent bar across the top */
            .ff-hero::after {
                content: '';
                position: absolute;
                top: 0; left: 0; right: 0;
                height: 2px;
                background: linear-gradient(90deg, var(--ff-green) 0%, rgba(34,197,94,0.08) 100%);
            }
            .ff-hero h1 {
                margin: 0;
                font-size: 1.95rem;
                font-weight: 900;
                letter-spacing: -0.025em;
                color: var(--ff-text);
            }
            .ff-hero h1 .accent { color: var(--ff-green); }
            .ff-hero p {
                margin: 8px 0 0 0;
                color: var(--ff-muted-2);
                font-size: 0.9rem;
                line-height: 1.55;
            }

            /* === STATUS STRIP === */
            .ff-strip {
                display: grid;
                grid-template-columns: repeat(4, minmax(0, 1fr));
                gap: 10px;
                margin-top: 18px;
            }
            .ff-pill {
                background: rgba(255,255,255,0.04);
                border: 1px solid var(--ff-border);
                border-radius: 12px;
                padding: 10px 13px;
                transition: border-color 0.18s;
            }
            .ff-pill:hover { border-color: rgba(255,255,255,0.13); }
            .ff-pill .label {
                color: var(--ff-muted);
                font-size: 0.7rem;
                text-transform: uppercase;
                letter-spacing: 0.1em;
                font-weight: 700;
            }
            .ff-pill .value {
                display: block;
                margin-top: 3px;
                font-size: 0.95rem;
                font-weight: 700;
                color: var(--ff-text);
            }

            /* ================================================================
               CARDS (Heuristic 8 – Aesthetic & Minimalist)
               ================================================================ */
            .ff-card {
                background: var(--ff-surface);
                border: 1px solid var(--ff-border);
                border-radius: 16px;
                padding: 18px 20px;
                box-shadow: 0 2px 18px rgba(0,0,0,0.22);
            }
            .ff-section-label {
                color: var(--ff-muted);
                text-transform: uppercase;
                letter-spacing: 0.1em;
                font-size: 0.7rem;
                font-weight: 800;
                margin-bottom: 12px;
                display: flex;
                align-items: center;
                gap: 7px;
            }
            .ff-section-label::before {
                content: '';
                display: inline-block;
                width: 3px;
                height: 11px;
                background: var(--ff-accent);
                border-radius: 2px;
                flex-shrink: 0;
            }

            /* === KEY-VALUE === */
            .ff-kv {
                display: grid;
                grid-template-columns: 160px 1fr;
                gap: 8px;
                font-size: 0.875rem;
            }
            .ff-kv div:nth-child(odd)  { color: var(--ff-muted);  font-size: 0.835rem; }
            .ff-kv div:nth-child(even) { color: var(--ff-text);   font-weight: 600; }

            /* ================================================================
               BADGES (Heuristic 1 – Visibility)
               ================================================================ */
            .ff-badge-row {
                display: flex;
                flex-wrap: wrap;
                gap: 6px;
                margin: 10px 0 12px 0;
            }
            .ff-badge {
                display: inline-flex;
                align-items: center;
                gap: 5px;
                border-radius: 999px;
                padding: 5px 10px;
                font-size: 0.76rem;
                font-weight: 700;
                border: 1px solid var(--ff-border-2);
                color: var(--ff-muted-2);
                background: var(--ff-surface-2);
                letter-spacing: 0.015em;
            }
            /* Heuristic 4 – Consistency: same green = connected/good, red = error everywhere */
            .ff-badge.good  { color: var(--ff-green-text); background: var(--ff-green-glow); border-color: var(--ff-green-border); }
            .ff-badge.warn  { color: var(--ff-amber-text); background: var(--ff-amber-glow); border-color: var(--ff-amber-border); }
            .ff-badge.error { color: var(--ff-red-text);   background: var(--ff-red-glow);   border-color: var(--ff-red-border);   }

            /* === MINI GRID === */
            .ff-mini-grid { display: grid; grid-template-columns: repeat(2,minmax(0,1fr)); gap: 8px; }
            .ff-mini-card {
                background: var(--ff-surface-2);
                border: 1px solid var(--ff-border);
                border-radius: 12px;
                padding: 10px 13px;
                transition: border-color 0.15s;
            }
            .ff-mini-card:hover { border-color: rgba(255,255,255,0.1); }
            .ff-mini-card .label {
                color: var(--ff-muted);
                font-size: 0.7rem;
                text-transform: uppercase;
                letter-spacing: 0.09em;
                font-weight: 700;
            }
            .ff-mini-card .value {
                display: block;
                margin-top: 3px;
                color: var(--ff-text);
                font-size: 0.93rem;
                font-weight: 700;
                line-height: 1.25;
            }

            /* ================================================================
               NOTE BOXES (Heuristic 9 – Help recover from errors)
               ================================================================ */
            .ff-note-box {
                margin-top: 10px;
                border-radius: 12px;
                padding: 11px 14px;
                border: 1px solid var(--ff-border);
                background: var(--ff-surface-2);
                animation: fadeUp 0.25s ease;
            }
            .ff-note-box.good  { background: var(--ff-green-glow); border-color: var(--ff-green-border); }
            .ff-note-box.warn  { background: var(--ff-amber-glow); border-color: var(--ff-amber-border); }
            .ff-note-box.error { background: var(--ff-red-glow);   border-color: var(--ff-red-border);   }
            .ff-note-box .title {
                color: var(--ff-muted);
                font-size: 0.7rem;
                text-transform: uppercase;
                letter-spacing: 0.1em;
                font-weight: 800;
            }
            .ff-note-box .body {
                margin-top: 5px;
                color: var(--ff-text);
                font-size: 0.875rem;
                line-height: 1.45;
                font-weight: 500;
            }

            /* ================================================================
               OPERATOR FLOW (Heuristic 6 – Recognition over Recall)
               ================================================================ */
            .ff-flow {
                background: var(--ff-surface);
                border: 1px solid var(--ff-border);
                border-radius: 16px;
                padding: 18px 20px;
                margin: 0 0 18px 0;
            }
            .ff-flow-grid {
                display: grid;
                grid-template-columns: repeat(3, minmax(0, 1fr));
                gap: 10px;
                margin-top: 14px;
            }
            .ff-flow-step {
                background: var(--ff-surface-2);
                border: 1px solid var(--ff-border);
                border-radius: 13px;
                padding: 14px 15px;
                transition: border-color 0.18s;
            }
            .ff-flow-step:hover { border-color: rgba(255,255,255,0.11); }
            /* Numbered step circle */
            .ff-step-num {
                display: inline-flex;
                align-items: center;
                justify-content: center;
                width: 26px;
                height: 26px;
                background: var(--ff-green-glow);
                border: 1px solid var(--ff-green-border);
                border-radius: 999px;
                color: var(--ff-green-text);
                font-size: 0.76rem;
                font-weight: 800;
                margin-bottom: 9px;
            }
            .ff-flow-step .step  { display: none; }
            .ff-flow-step .title {
                display: block;
                color: var(--ff-text);
                font-weight: 700;
                font-size: 0.88rem;
                margin-bottom: 5px;
            }
            .ff-flow-step .detail {
                display: block;
                color: var(--ff-muted-2);
                font-size: 0.84rem;
                line-height: 1.4;
            }

            /* === TIMELINE === */
            .ff-timeline     { margin: 0 0 18px 0; }
            .ff-timeline-bar {
                display: grid;
                grid-template-columns: repeat(4, minmax(0, 1fr));
                gap: 10px;
            }

            /* ================================================================
               HEALTH GRID  (Heuristic 1 – Visibility; Heuristic 4 – Consistency)
               Green = connected/good | Red = broken/offline | Amber = needs attention
               ================================================================ */
            .ff-health-grid {
                display: grid;
                grid-template-columns: repeat(4, minmax(0, 1fr));
                gap: 10px;
                margin-bottom: 16px;
            }
            .ff-health-card {
                border-radius: 14px;
                padding: 13px 15px;
                border: 1px solid var(--ff-border);
                border-left: 3px solid var(--ff-border-2);
                background: var(--ff-surface-2);
                transition: all 0.2s;
            }
            /* GREEN = connected / good */
            .ff-health-card.good  {
                border-color: var(--ff-green-border);
                border-left-color: var(--ff-green);
                background: var(--ff-green-glow);
            }
            /* RED = offline / broken */
            .ff-health-card.error {
                border-color: var(--ff-red-border);
                border-left-color: var(--ff-red);
                background: var(--ff-red-glow);
            }
            /* AMBER = warning / stale */
            .ff-health-card.warn  {
                border-color: var(--ff-amber-border);
                border-left-color: var(--ff-amber);
                background: var(--ff-amber-glow);
            }
            .ff-health-card .label {
                color: var(--ff-muted);
                font-size: 0.69rem;
                text-transform: uppercase;
                letter-spacing: 0.1em;
                font-weight: 700;
            }
            .ff-health-card .value {
                display: block;
                margin-top: 5px;
                font-size: 1.05rem;
                font-weight: 800;
                color: var(--ff-text);
            }
            .ff-health-card.good  .value { color: var(--ff-green-text); }
            .ff-health-card.error .value { color: var(--ff-red-text);   }
            .ff-health-card.warn  .value { color: var(--ff-amber-text); }

            /* ================================================================
               STATUS BOARD (Heuristic 1 – Visibility of system status)
               ================================================================ */
            .ff-status-board {
                display: grid;
                grid-template-columns: minmax(260px, 0.92fr) minmax(310px, 1.08fr);
                gap: 13px;
                margin: 0 0 18px 0;
            }
            .ff-status-card {
                background: var(--ff-surface);
                border: 1px solid var(--ff-border);
                border-radius: 16px;
                padding: 20px;
                box-shadow: 0 2px 18px rgba(0,0,0,0.22);
                transition: all 0.3s;
            }
            /* All-green state – clear positive signal */
            .ff-status-card.good {
                border-color: var(--ff-green-border);
                background: linear-gradient(155deg, var(--ff-green-glow) 0%, var(--ff-surface) 55%);
                box-shadow: 0 0 0 1px var(--ff-green-border), 0 4px 22px rgba(34,197,94,0.1);
            }
            .ff-status-card.warn {
                border-color: var(--ff-amber-border);
                background: linear-gradient(155deg, var(--ff-amber-glow) 0%, var(--ff-surface) 55%);
            }
            /* Red – not ready */
            .ff-status-card.error {
                border-color: var(--ff-red-border);
                background: linear-gradient(155deg, var(--ff-red-glow) 0%, var(--ff-surface) 55%);
            }
            .ff-status-card .eyebrow {
                color: var(--ff-muted);
                font-size: 0.69rem;
                letter-spacing: 0.1em;
                text-transform: uppercase;
                font-weight: 800;
            }
            .ff-status-card .headline {
                display: block;
                margin-top: 8px;
                color: var(--ff-text);
                font-size: 1.55rem;
                font-weight: 900;
                line-height: 1.1;
                letter-spacing: -0.02em;
            }
            .ff-status-card.good  .headline { color: var(--ff-green-text); }
            .ff-status-card.error .headline { color: var(--ff-red-text);   }
            .ff-status-card .body {
                display: block;
                margin-top: 8px;
                color: var(--ff-muted-2);
                font-size: 0.875rem;
                line-height: 1.5;
            }
            .ff-status-card .meta {
                display: block;
                margin-top: 14px;
                padding-top: 13px;
                border-top: 1px solid rgba(255,255,255,0.055);
                color: var(--ff-muted);
                font-size: 0.81rem;
                line-height: 1.35;
            }

            /* ================================================================
               PREFLIGHT CHECKLIST (Heuristic 1 + Heuristic 6)
               ================================================================ */
            .ff-checklist {
                background: var(--ff-surface);
                border: 1px solid var(--ff-border);
                border-radius: 16px;
                padding: 18px 20px;
                box-shadow: 0 2px 18px rgba(0,0,0,0.22);
            }
            .ff-checklist-row {
                display: grid;
                grid-template-columns: 14px minmax(120px, 160px) 1fr;
                gap: 12px;
                align-items: center;
                padding: 9px 0;
                border-bottom: 1px solid rgba(255,255,255,0.045);
            }
            .ff-checklist-row:last-child  { border-bottom: none; padding-bottom: 0; }
            .ff-checklist-row:first-child { padding-top: 2px; }

            /* Animated status dots – pulsing = live feedback (Heuristic 1) */
            .ff-dot {
                width: 10px;
                height: 10px;
                border-radius: 999px;
                display: inline-block;
                flex-shrink: 0;
            }
            .ff-dot.good  { background: var(--ff-green); animation: pulse-green 2s ease-in-out infinite; }
            .ff-dot.warn  { background: var(--ff-amber); animation: pulse-amber 2.5s ease-in-out infinite; }
            .ff-dot.error { background: var(--ff-red);   animation: pulse-red  1.8s ease-in-out infinite; }

            .ff-checklist-row .label  { color: var(--ff-text);    font-weight: 600; font-size: 0.87rem; }
            .ff-checklist-row .detail { color: var(--ff-muted-2); font-size: 0.84rem; line-height: 1.35; }

            /* Heuristic 10 – Next Action hint */
            .ff-checklist-hint {
                margin-top: 14px;
                border-top: 1px solid rgba(255,255,255,0.065);
                padding-top: 12px;
            }
            .ff-checklist-hint .title {
                color: var(--ff-muted);
                font-size: 0.69rem;
                text-transform: uppercase;
                letter-spacing: 0.1em;
                font-weight: 800;
            }
            .ff-checklist-hint .body {
                display: block;
                margin-top: 6px;
                color: var(--ff-text);
                font-size: 0.875rem;
                font-weight: 600;
                line-height: 1.45;
            }

            /* === MISC TEXT === */
            .ff-subtle { color: var(--ff-muted-2); font-size: 0.875rem; line-height: 1.5; }
            .ff-list   { margin: 10px 0 0 0; padding-left: 18px; }
            .ff-list li { margin-bottom: 6px; line-height: 1.4; font-size: 0.875rem; color: var(--ff-muted-2); }

            /* === TABLES === */
            .ff-table-wrap table {
                width: 100%;
                border-collapse: collapse;
                background: var(--ff-surface);
                border: 1px solid var(--ff-border);
                border-radius: 12px;
                overflow: hidden;
                font-size: 0.86rem;
            }
            .ff-table-wrap th,
            .ff-table-wrap td { padding: 9px 13px; border-bottom: 1px solid rgba(255,255,255,0.045); text-align: left; }
            .ff-table-wrap th { color: var(--ff-muted); font-weight: 700; text-transform: uppercase; letter-spacing: 0.07em; background: var(--ff-surface-2); font-size: 0.69rem; }
            .ff-table-wrap td { color: var(--ff-text); }
            .ff-table-wrap tr:last-child td { border-bottom: none; }

            /* Streamlit data tables */
            .stTable table,
            [data-testid="stDataFrame"] table {
                background: var(--ff-surface) !important;
                border: 1px solid var(--ff-border) !important;
                border-radius: 12px !important;
                overflow: hidden;
                font-size: 0.86rem !important;
            }
            .stTable th,
            [data-testid="stDataFrame"] th {
                background: var(--ff-surface-2) !important;
                color: var(--ff-muted) !important;
                text-transform: uppercase;
                letter-spacing: 0.07em;
                font-size: 0.69rem !important;
                font-weight: 800 !important;
                border-color: rgba(255,255,255,0.045) !important;
            }
            .stTable td,
            [data-testid="stDataFrame"] td {
                background: var(--ff-surface) !important;
                color: var(--ff-text) !important;
                border-color: rgba(255,255,255,0.045) !important;
            }

            /* Section headers */
            h3 {
                font-size: 1rem !important;
                font-weight: 800 !important;
                color: var(--ff-text) !important;
                letter-spacing: -0.01em;
                margin-bottom: 12px !important;
            }

            /* === RESPONSIVE (Heuristic 7 – Flexibility) === */
            @media (max-width: 1200px) {
                .ff-strip,
                .ff-timeline-bar,
                .ff-health-grid,
                .ff-flow-grid,
                .ff-status-board { grid-template-columns: repeat(2, minmax(0, 1fr)); }
            }
            @media (max-width: 820px) {
                .ff-strip,
                .ff-timeline-bar,
                .ff-health-grid,
                .ff-flow-grid,
                .ff-status-board,
                .ff-mini-grid    { grid-template-columns: 1fr; }
                .ff-kv           { grid-template-columns: 1fr; gap: 4px; }
                .ff-checklist-row { grid-template-columns: 14px 1fr; }
                .ff-checklist-row .detail { grid-column: 2; }
                .ff-hero h1 { font-size: 1.5rem; }
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(ttl=60, show_spinner=False)
def _load_yaml(path_str: str) -> dict[str, Any]:
    path = Path(path_str)
    if not path.exists():
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)


def _coerce_time(value: Any, fallback: time) -> time:
    if isinstance(value, time):
        return value
    token = str(value or "").strip()
    if not token:
        return fallback
    for suffix in ("", ":00"):
        try:
            return time.fromisoformat(token + suffix if suffix and len(token) == 5 else token)
        except ValueError:
            continue
    return fallback


def _time_token(value: Any) -> str:
    value_time = _coerce_time(value, fallback=time(14, 0))
    return value_time.strftime("%H:%M")


def _bridge_path() -> Path:
    return init_bridge(BRIDGE_DB_PATH)


def _runtime_repo_root() -> Path:
    return REPO_ROOT


def _load_ui_settings() -> dict[str, Any]:
    payload = _read_json(UI_SETTINGS_PATH)
    merged = dict(DEFAULT_SETTINGS)
    merged.update({key: value for key, value in payload.items() if key in DEFAULT_SETTINGS})
    merged["debug_port"] = resolve_cdp_port(merged.get("debug_port", CDP_PORT))
    merged["fixed_contracts"] = max(1, int(merged.get("fixed_contracts", 1) or 1))
    merged["bar1_start"] = _time_token(merged.get("bar1_start", "14:00"))
    merged["selector_mode"] = (
        "CSS Selector" if str(merged.get("selector_mode", "")).strip() == "CSS Selector" else "Auto (platform selectors)"
    )
    return merged


def _init_session_state() -> None:
    defaults = _load_ui_settings()
    st.session_state.setdefault("ff_debug_port", int(defaults["debug_port"]))
    st.session_state.setdefault("ff_expected_account_token", str(defaults["expected_account_token"]))
    st.session_state.setdefault("ff_selector_mode", str(defaults["selector_mode"]))
    st.session_state.setdefault("ff_buy_selector", str(defaults["buy_selector"]))
    st.session_state.setdefault("ff_sell_selector", str(defaults["sell_selector"]))
    st.session_state.setdefault("ff_flatten_selector", str(defaults["flatten_selector"]))
    st.session_state.setdefault("ff_fixed_contracts", int(defaults["fixed_contracts"]))
    st.session_state.setdefault("ff_bar1_start", _coerce_time(defaults["bar1_start"], fallback=time(14, 0)))
    st.session_state.setdefault("ff_flash", None)


def _save_ui_settings_from_session() -> None:
    payload = {
        "debug_port": resolve_cdp_port(st.session_state.get("ff_debug_port", CDP_PORT)),
        "expected_account_token": str(st.session_state.get("ff_expected_account_token", "") or "").strip(),
        "selector_mode": str(st.session_state.get("ff_selector_mode", "Auto (platform selectors)") or "Auto (platform selectors)"),
        "buy_selector": str(st.session_state.get("ff_buy_selector", "") or "").strip(),
        "sell_selector": str(st.session_state.get("ff_sell_selector", "") or "").strip(),
        "flatten_selector": str(st.session_state.get("ff_flatten_selector", "") or "").strip(),
        "fixed_contracts": max(1, int(st.session_state.get("ff_fixed_contracts", 1) or 1)),
        "bar1_start": _time_token(st.session_state.get("ff_bar1_start", time(14, 0))),
        "chrome_headless": False,
    }
    _write_json(UI_SETTINGS_PATH, payload)


def _flash(level: str, message: str) -> None:
    st.session_state["ff_flash"] = {"level": str(level), "message": str(message)}


def _render_flash() -> None:
    payload = st.session_state.pop("ff_flash", None)
    if not isinstance(payload, dict):
        return
    message = str(payload.get("message", "") or "").strip()
    if not message:
        return
    level = str(payload.get("level", "info") or "info").strip().lower()
    renderer = {
        "success": st.success,
        "warning": st.warning,
        "error": st.error,
        "info": st.info,
    }.get(level, st.info)
    renderer(message)


def _bridge_status_age_seconds(payload: dict[str, Any]) -> float | None:
    updated_at = str(payload.get("_bridge_updated_at", "") or "").strip()
    if not updated_at:
        return None
    try:
        observed_at = datetime.fromisoformat(updated_at)
    except ValueError:
        return None
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(tz=timezone.utc) - observed_at.astimezone(timezone.utc)).total_seconds())


def _service_status() -> dict[str, Any]:
    return read_engine_service_status(_runtime_repo_root(), bridge_db_path=_bridge_path())


def _chrome_status() -> dict[str, Any]:
    port = resolve_cdp_port(st.session_state.get("ff_debug_port", CDP_PORT))
    return read_cdp_chrome_status(_runtime_repo_root(), port=port)


def _engine_status() -> dict[str, Any]:
    return fetch_status(_bridge_path())


def _account_config() -> dict[str, Any]:
    return _load_yaml(str(ACCOUNT_CONFIG_PATH))


def _live_config() -> dict[str, Any]:
    return _load_yaml(str(LIVE_CONFIG_PATH))


def _runtime_profile() -> dict[str, Any]:
    fixed_contracts = max(1, int(st.session_state.get("ff_fixed_contracts", 1) or 1))
    bar1_start = _time_token(st.session_state.get("ff_bar1_start", time(14, 0)))
    account_cfg = _account_config()
    starting_balance = account_cfg.get("starting_balance")
    return {
        "product_type": "futures",
        "strategy_name": "School Run",
        "instrument": "DOW",
        "timeframe": "15m",
        "bar1_start": bar1_start,
        "bar1_timing_mode": "manual_dk",
        "resolved_bar1_start_dk": "",
        "resolved_bar2_start_dk": "",
        "resolved_trigger_start_dk": "",
        "prop_firm": "Manual",
        "account_size": str(starting_balance or ""),
        "account_size_usd": float(starting_balance or 0.0),
        "risk_profile": "Aggressive",
        "lock_profile_risk": False,
        "risk_usd": 0.0,
        "sizing_mode": "Fixed contracts",
        "fixed_contracts": fixed_contracts,
        "configured_fixed_contracts": fixed_contracts,
        "auto_contracts": None,
        "max_add_to_winners": 2,
        "max_contracts_limit": None,
        "stop_loss_amount": None,
        "execution_model": "Aggressiv",
        "ticker": "MYM",
        "contract_symbol": "MYM",
    }


def _runtime_config() -> dict[str, Any]:
    fixed_contracts = max(1, int(st.session_state.get("ff_fixed_contracts", 1) or 1))
    bar1_start = _time_token(st.session_state.get("ff_bar1_start", time(14, 0)))
    config = coerce_custom_strategy_config(
        {
            **CustomStrategyConfig().to_dict(),
            "instrument": "DOW",
            "execution_model": "Aggressiv",
            "bar1_start": bar1_start,
            "contract_symbol": "MYM",
            "contract_quantity": float(fixed_contracts),
        }
    )
    return config.to_dict()


def _engine_payload() -> dict[str, Any]:
    fixed_contracts = max(1, int(st.session_state.get("ff_fixed_contracts", 1) or 1))
    live_cfg = _live_config()
    return {
        "platform": "tradovate",
        "debug_port": resolve_cdp_port(st.session_state.get("ff_debug_port", CDP_PORT)),
        "selector_mode": str(
            st.session_state.get("ff_selector_mode", "Auto (platform selectors)") or "Auto (platform selectors)"
        ).strip(),
        "buy_selector": str(st.session_state.get("ff_buy_selector", "") or "").strip(),
        "sell_selector": str(st.session_state.get("ff_sell_selector", "") or "").strip(),
        "flatten_selector": str(st.session_state.get("ff_flatten_selector", "") or "").strip(),
        "expected_account_token": str(st.session_state.get("ff_expected_account_token", "") or "").strip(),
        "runtime_profile": _runtime_profile(),
        "runtime_config": _runtime_config(),
        "runtime_active": True,
        "risk_gate_account_config": _account_config(),
        "timezone_name": "Europe/Copenhagen",
        "overnight_start_dk": "00:00:00",
        "overnight_end_dk": "08:00:00",
        "kill_switch": False,
        "webhook_url": str(live_cfg.get("webhook_url", "") or "").strip(),
        "quantity": fixed_contracts,
    }


def _queue_command(command: str, *, payload: dict[str, Any] | None = None) -> int:
    return enqueue_command(_bridge_path(), str(command).strip().upper(), payload=payload)


def _dispatch_command(command: str) -> None:
    command_id = _queue_command(command, payload=_engine_payload())
    _flash("success", f"{command} sendt til engine (cmd {command_id}).")
    st.rerun()


def _handle_service_action(action: str) -> None:
    if action == "start_engine":
        result = start_engine_service(_runtime_repo_root(), bridge_db_path=_bridge_path())
        if bool(result.get("ok", False)):
            _flash("success", "Trading engine service er startet." if result.get("status") == "started" else "Trading engine service kører allerede.")
        else:
            _flash("error", str(result.get("last_error", "") or "Trading engine service kunne ikke startes."))
    elif action == "stop_engine":
        result = stop_engine_service(_runtime_repo_root(), bridge_db_path=_bridge_path())
        if bool(result.get("ok", False)):
            _flash("success", "Trading engine service er stoppet.")
        else:
            _flash("error", str(result.get("last_error", "") or "Trading engine service kunne ikke stoppes rent."))
    elif action == "start_chrome":
        result = launch_cdp_chrome(
            _runtime_repo_root(),
            port=resolve_cdp_port(st.session_state.get("ff_debug_port", CDP_PORT)),
            headless=False,
        )
        if bool(result.get("ok", False)):
            status_label = "genbrugt" if result.get("status") == "reused_existing" else "startet"
            _flash("success", f"CDP Chrome er {status_label} på port {result.get('port', CDP_PORT)}.")
        else:
            _flash("error", str(result.get("last_error", "") or "CDP Chrome kunne ikke startes."))
    elif action == "stop_chrome":
        result = stop_cdp_chrome(
            _runtime_repo_root(),
            port=resolve_cdp_port(st.session_state.get("ff_debug_port", CDP_PORT)),
        )
        if bool(result.get("ok", False)):
            _flash("success", "Managed CDP Chrome er stoppet.")
        else:
            _flash("error", str(result.get("last_error", "") or "Managed CDP Chrome kunne ikke stoppes."))
    st.rerun()


def _status_chip(label: str, value: str) -> str:
    return (
        "<div class='ff-pill'>"
        f"<span class='label'>{label}</span>"
        f"<span class='value'>{value}</span>"
        "</div>"
    )


def _timeline_card(label: str, value: str) -> str:
    return (
        "<div class='ff-health-card'>"
        f"<span class='label'>{escape(label)}</span>"
        f"<span class='value'>{escape(value)}</span>"
        "</div>"
    )


def _health_card(label: str, value: str, tone: str) -> str:
    safe_tone = tone if tone in {"good", "warn", "error"} else "neutral"
    return (
        f"<div class='ff-health-card {safe_tone}'>"
        f"<span class='label'>{escape(label)}</span>"
        f"<span class='value'>{escape(value)}</span>"
        "</div>"
    )


def _flow_step(step: str, title: str, detail: str) -> str:
    step_num = step.replace("Step ", "").strip()
    return (
        "<div class='ff-flow-step'>"
        f"<div class='ff-step-num'>{escape(step_num)}</div>"
        f"<span class='title'>{escape(title)}</span>"
        f"<span class='detail'>{escape(detail)}</span>"
        "</div>"
    )


def _checklist_row(label: str, detail: str, tone: str) -> str:
    safe_tone = tone if tone in {"good", "warn", "error"} else "warn"
    return (
        "<div class='ff-checklist-row'>"
        f"<span class='ff-dot {safe_tone}'></span>"
        f"<span class='label'>{escape(label)}</span>"
        f"<span class='detail'>{escape(detail)}</span>"
        "</div>"
    )


def _status_card(headline: str, body: str, meta: str, tone: str) -> str:
    safe_tone = tone if tone in {"good", "warn", "error"} else "warn"
    return (
        f"<div class='ff-status-card {safe_tone}'>"
        "<span class='eyebrow'>Live Readiness</span>"
        f"<span class='headline'>{escape(headline)}</span>"
        f"<span class='body'>{escape(body)}</span>"
        f"<span class='meta'>{escape(meta)}</span>"
        "</div>"
    )


def _tone_for_boolean(value: bool) -> str:
    return "good" if bool(value) else "error"


def _readiness_model(
    service_status: dict[str, Any],
    chrome_status: dict[str, Any],
    engine_status: dict[str, Any],
) -> dict[str, Any]:
    snapshot = engine_status.get("tradovate_snapshot") if isinstance(engine_status.get("tradovate_snapshot"), dict) else {}
    live_state = engine_status.get("live_state") if isinstance(engine_status.get("live_state"), dict) else {}
    status_age = _bridge_status_age_seconds(engine_status)
    bridge_fresh = status_age is not None and status_age <= 5.0
    checks = [
        {
            "label": "Engine service",
            "detail": "Kører" if bool(service_status.get("running", False)) else "Stoppet",
            "ok": bool(service_status.get("running", False)),
            "tone": _tone_for_boolean(bool(service_status.get("running", False))),
        },
        {
            "label": "Chrome CDP",
            "detail": "Klar" if bool(chrome_status.get("ready", False)) else "Offline",
            "ok": bool(chrome_status.get("ready", False)),
            "tone": _tone_for_boolean(bool(chrome_status.get("ready", False))),
        },
        {
            "label": "Adapter",
            "detail": "Forbundet" if bool(engine_status.get("connected", False)) else "Ikke forbundet",
            "ok": bool(engine_status.get("connected", False)),
            "tone": _tone_for_boolean(bool(engine_status.get("connected", False))),
        },
        {
            "label": "Bridge heartbeat",
            "detail": f"Frisk ({status_age:.1f}s)" if bridge_fresh else (f"Gammel ({status_age:.1f}s)" if status_age is not None else "Ingen data"),
            "ok": bridge_fresh,
            "tone": "good" if bridge_fresh else "warn",
        },
        {
            "label": "Account match",
            "detail": "OK" if bool(snapshot.get("account_ok", False)) else ("Mismatch" if snapshot else "Kræver snapshot"),
            "ok": bool(snapshot.get("account_ok", False)),
            "tone": _tone_for_boolean(bool(snapshot.get("account_ok", False))),
        },
        {
            "label": "Instrument",
            "detail": "MYM synlig" if bool(snapshot.get("instrument_visible", False)) else ("Ikke synlig" if snapshot else "Kræver snapshot"),
            "ok": bool(snapshot.get("instrument_visible", False)),
            "tone": _tone_for_boolean(bool(snapshot.get("instrument_visible", False))),
        },
        {
            "label": "Quote feed",
            "detail": "Klar" if bool(snapshot.get("quote_ready", False)) else ("Mangler quotes" if snapshot else "Kræver snapshot"),
            "ok": bool(snapshot.get("quote_ready", False)),
            "tone": _tone_for_boolean(bool(snapshot.get("quote_ready", False))),
        },
        {
            "label": "Reconcile state",
            "detail": "Clear" if not bool(live_state.get("reconcile_required", False)) else "Manual reconcile krævet",
            "ok": not bool(live_state.get("reconcile_required", False)),
            "tone": "good" if not bool(live_state.get("reconcile_required", False)) else "error",
        },
    ]
    hard_blockers = [check for check in checks if not check["ok"] and check["label"] != "Bridge heartbeat"]
    blocker_count = len(hard_blockers)
    if not bool(service_status.get("running", False)):
        next_action = "Start Engine Service."
    elif not bool(chrome_status.get("ready", False)):
        next_action = "Start eller genbrug Chrome med CDP."
    elif not bool(engine_status.get("connected", False)):
        next_action = "Klik Connect og bekræft adapter-forbindelsen."
    elif not snapshot:
        next_action = "Klik Refresh Snapshot for at hente broker-state."
    elif not bool(snapshot.get("account_ok", False)):
        next_action = "Ret account token eller vælg korrekt konto i Tradovate."
    elif not bool(snapshot.get("instrument_visible", False)):
        next_action = "Åbn eller skift til MYM-kontrakten i Tradovate."
    elif not bool(snapshot.get("quote_ready", False)):
        next_action = "Vent på live quotes eller refresh snapshot igen."
    elif bool(live_state.get("reconcile_required", False)):
        next_action = "Stop auto og ryd reconcile før ny handel."
    else:
        next_action = "Systemet er klar. Du kan starte auto, hvis dagens vindue er korrekt."
    if blocker_count == 0:
        headline = "Ready For Auto"
        body = "Alle kritiske checks er grønne."
        tone = "good"
    elif blocker_count <= 2:
        headline = "Needs Attention"
        body = f"{blocker_count} kritiske check skal løses før auto."
        tone = "warn"
    else:
        headline = "Not Ready"
        body = f"{blocker_count} kritiske blokeringer stopper live auto."
        tone = "error"
    return {
        "checks": checks,
        "next_action": next_action,
        "headline": headline,
        "body": body,
        "tone": tone,
        "meta": f"Strategy: School Run | Model: Aggressiv | Contracts: {max(1, int(st.session_state.get('ff_fixed_contracts', 1) or 1))}",
    }


def _render_header(session_clock: Any) -> None:
    bar1 = session_clock.bar1_start_dk.strftime("%H:%M")
    bar2 = session_clock.bar2_start_dk.strftime("%H:%M")
    trigger = session_clock.trigger_start_dk.strftime("%H:%M")
    fixed_contracts = max(1, int(st.session_state.get("ff_fixed_contracts", 1) or 1))
    st.markdown(
        (
            "<div class='ff-hero'>"
            "<h1>Final <span class='accent'>Fantasy</span></h1>"
            "<p>Split-engine dashboard for School Run full auto execution. "
            "Streamlit er kun kontrolvindue — engine, broker-state og worker loops kører separat.</p>"
            "<div class='ff-strip'>"
            f"{_status_chip('Strategy', 'School Run')}"
            f"{_status_chip('Execution', 'Aggressiv')}"
            f"{_status_chip('Contracts', f'{fixed_contracts} kontrakter')}"
            f"{_status_chip('Session', f'Bar1 {bar1} | Bar2 {bar2} | Trigger {trigger}')}"
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )
    st.markdown(
        (
            "<div class='ff-timeline'>"
            "<div class='ff-timeline-bar'>"
            f"{_timeline_card('Bar 1', bar1)}"
            f"{_timeline_card('Bar 2', bar2)}"
            f"{_timeline_card('Trigger', trigger)}"
            f"{_timeline_card('Add Logic', '2 x 1:1')}"
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_operator_flow() -> None:
    st.markdown(
        (
            "<div class='ff-flow'>"
            "<div class='ff-section-label'>Operator Flow</div>"
            "<div class='ff-flow-grid'>"
            f"{_flow_step('Step 1', 'Prepare Runtime', 'Sæt contracts, Bar 1 start, port og account token i sidebaren.')}"
            f"{_flow_step('Step 2', 'Bring System Online', 'Start Engine Service, start eller genbrug Chrome, og vent på grøn status.')}"
            f"{_flow_step('Step 3', 'Verify Before Auto', 'Connect, refresh snapshot, check account, contract og qty, og start først derefter auto.')}"
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_readiness_panel(
    service_status: dict[str, Any],
    chrome_status: dict[str, Any],
    engine_status: dict[str, Any],
) -> None:
    readiness = _readiness_model(service_status, chrome_status, engine_status)
    checklist_html = "".join(
        _checklist_row(check["label"], check["detail"], check["tone"]) for check in readiness["checks"]
    )
    st.markdown("### Live Readiness")
    st.markdown(
        (
            "<div class='ff-status-board'>"
            f"{_status_card(readiness['headline'], readiness['body'], readiness['meta'], readiness['tone'])}"
            "<div class='ff-checklist'>"
            "<div class='ff-section-label'>Preflight Checklist</div>"
            f"{checklist_html}"
            "<div class='ff-checklist-hint'>"
            "<div class='title'>Next Action</div>"
            f"<span class='body'>{escape(readiness['next_action'])}</span>"
            "</div>"
            "</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_settings_panel() -> None:
    account_cfg = _account_config()
    with st.sidebar:
        st.markdown("### ⚙️ Runtime Setup")
        st.caption("Sæt kun det, der faktisk ændrer dagens live-kørsel. Resten er låst til School Run + Aggressiv.")
        with st.form("ff_runtime_form"):
            st.number_input(
                "Fixed contracts",
                min_value=1,
                max_value=100,
                step=1,
                key="ff_fixed_contracts",
            )
            st.time_input(
                "Bar 1 start (DK)",
                key="ff_bar1_start",
            )
            st.number_input(
                "CDP port",
                min_value=1024,
                max_value=65535,
                step=1,
                key="ff_debug_port",
            )
            st.text_input(
                "Tradovate account token",
                key="ff_expected_account_token",
                help="Vises read-only i snapshot-checks og bruges til account match.",
            )
            st.selectbox(
                "Selector mode",
                options=["Auto (platform selectors)", "CSS Selector"],
                key="ff_selector_mode",
            )
            if str(st.session_state.get("ff_selector_mode", "")).strip() == "CSS Selector":
                st.text_input("Buy selector", key="ff_buy_selector")
                st.text_input("Sell selector", key="ff_sell_selector")
                st.text_input("Flatten selector", key="ff_flatten_selector")
            save_clicked = st.form_submit_button("Save defaults", use_container_width=True)
        if save_clicked:
            _save_ui_settings_from_session()
            _flash("success", "Runtime defaults er gemt.")
            st.rerun()

        st.markdown("### 🔒 Locked Trade Logic")
        st.table(pd.DataFrame(MANAGEMENT_ROWS))
        st.caption("Adds bliver altid samme størrelse som starter-size. `max_add_to_winners` er låst til 2.")
        if float(account_cfg.get("starting_balance", 0.0) or 0.0) == 1000.0:
            st.warning("`config/account_config.yaml` ser stadig ud som en template. Tjek balance og loss limits før live brug.")


def _inline_status_card(label: str, value: str, ok: bool, warn: bool = False) -> str:
    if ok:
        bg, border, dot, vc = "rgba(34,197,94,0.11)", "rgba(34,197,94,0.30)", "#22c55e", "#86efac"
    elif warn:
        bg, border, dot, vc = "rgba(245,158,11,0.10)", "rgba(245,158,11,0.28)", "#f59e0b", "#fcd34d"
    else:
        bg, border, dot, vc = "rgba(239,68,68,0.11)", "rgba(239,68,68,0.30)", "#ef4444", "#fca5a5"
    return (
        f"<div style='background:{bg};border:1px solid {border};border-left:3px solid {dot};"
        f"border-radius:12px;padding:13px 15px;'>"
        f"<div style='font-size:0.69rem;font-weight:700;text-transform:uppercase;letter-spacing:0.09em;color:#64748b;margin-bottom:6px;'>{escape(label)}</div>"
        f"<div style='font-size:1.05rem;font-weight:800;color:{vc};'>{escape(value)}</div>"
        f"</div>"
    )


def _render_service_panel(service_status: dict[str, Any], chrome_status: dict[str, Any], engine_status: dict[str, Any]) -> None:
    st.markdown("### Engine Control")
    status_age = _bridge_status_age_seconds(engine_status)
    connected = bool(engine_status.get("connected", False))
    bridge_fresh = status_age is not None and status_age <= 5.0
    engine_ok = bool(service_status.get("running", False))
    chrome_ok = bool(chrome_status.get("ready", False))

    # Native 4-column layout — no CSS Grid dependency
    hc1, hc2, hc3, hc4 = st.columns(4)
    hc1.markdown(_inline_status_card("Engine Service", "Running" if engine_ok else "Stopped", engine_ok), unsafe_allow_html=True)
    hc2.markdown(_inline_status_card("Chrome CDP", "Ready" if chrome_ok else "Offline", chrome_ok), unsafe_allow_html=True)
    hc3.markdown(_inline_status_card("Adapter", "Connected" if connected else "Disconnected", connected), unsafe_allow_html=True)
    hc4.markdown(_inline_status_card("Bridge", f"{status_age:.1f}s" if status_age is not None else "N/A", bridge_fresh, warn=not bridge_fresh), unsafe_allow_html=True)

    st.caption("Grøn = klar. Rød = brudt/stoppet. Gul = kræver handling.")

    action_col1, action_col2, action_col3, action_col4 = st.columns(4)
    with action_col1:
        if st.button("Start Engine Service", use_container_width=True):
            _handle_service_action("start_engine")
    with action_col2:
        if st.button("Stop Engine Service", use_container_width=True):
            _handle_service_action("stop_engine")
    with action_col3:
        if st.button("Start / Reuse Chrome", use_container_width=True):
            _handle_service_action("start_chrome")
    with action_col4:
        if st.button("Stop Managed Chrome", use_container_width=True):
            _handle_service_action("stop_chrome")

    detail_col1, detail_col2 = st.columns([1, 1])
    with detail_col1:
        st.markdown("<div class='ff-card'>", unsafe_allow_html=True)
        st.markdown("<div class='ff-section-label'>Supervisor</div>", unsafe_allow_html=True)
        st.markdown(
            (
                "<div class='ff-kv'>"
                f"<div>State</div><div>{service_status.get('state', 'unknown')}</div>"
                f"<div>Managed</div><div>{bool(service_status.get('managed', False))}</div>"
                f"<div>Supervisor PID</div><div>{service_status.get('supervisor_pid') or 'N/A'}</div>"
                f"<div>Engine PID</div><div>{service_status.get('engine_pid') or 'N/A'}</div>"
                f"<div>Restart Count</div><div>{int(service_status.get('restart_count', 0) or 0)}</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)
    with detail_col2:
        st.markdown("<div class='ff-card'>", unsafe_allow_html=True)
        st.markdown("<div class='ff-section-label'>Chrome Runtime</div>", unsafe_allow_html=True)
        st.markdown(
            (
                "<div class='ff-kv'>"
                f"<div>Port</div><div>{chrome_status.get('port', CDP_PORT)}</div>"
                f"<div>Ready</div><div>{bool(chrome_status.get('ready', False))}</div>"
                f"<div>Managed</div><div>{bool(chrome_status.get('managed', False))}</div>"
                f"<div>PID</div><div>{chrome_status.get('pid') or 'N/A'}</div>"
                f"<div>Detail</div><div>{chrome_status.get('last_error') or 'OK'}</div>"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)


def _render_command_panel(service_status: dict[str, Any], engine_status: dict[str, Any]) -> None:
    st.markdown("### Trading Commands")
    engine_available = bool(service_status.get("running", False))
    connected = bool(engine_status.get("connected", False))
    auto_requested = bool(engine_status.get("auto_requested", False))
    live_state = engine_status.get("live_state") if isinstance(engine_status.get("live_state"), dict) else {}
    snapshot = engine_status.get("tradovate_snapshot") if isinstance(engine_status.get("tradovate_snapshot"), dict) else {}
    position_open = bool(live_state.get("position_open", False))
    worker_running = bool(engine_status.get("running", False))
    reconcile_required = bool(live_state.get("reconcile_required", False))
    ready_to_start = bool(
        engine_available
        and connected
        and snapshot
        and snapshot.get("account_ok", False)
        and snapshot.get("instrument_visible", False)
        and snapshot.get("quote_ready", False)
        and not reconcile_required
    )
    blockers: list[str] = []
    if not engine_available:
        blockers.append("Engine service er ikke startet")
    if engine_available and not connected:
        blockers.append("Adapter er ikke forbundet")
    if engine_available and connected and not snapshot:
        blockers.append("Snapshot mangler")
    if snapshot and not bool(snapshot.get("account_ok", False)):
        blockers.append("Account matcher ikke")
    if snapshot and not bool(snapshot.get("instrument_visible", False)):
        blockers.append("MYM er ikke synlig")
    if snapshot and not bool(snapshot.get("quote_ready", False)):
        blockers.append("Quote feed er ikke klar")
    if reconcile_required:
        blockers.append("Reconcile skal ryddes")

    # Row 1 – Connection & diagnostics
    st.markdown("<div style='font-size:0.69rem;font-weight:700;text-transform:uppercase;letter-spacing:0.09em;color:#64748b;margin-bottom:6px;'>Forbindelse &amp; Diagnostik</div>", unsafe_allow_html=True)
    r1c1, r1c2, r1c3, r1c4 = st.columns(4)
    with r1c1:
        if st.button("Connect", use_container_width=True, disabled=not engine_available):
            _dispatch_command("CONNECT")
    with r1c2:
        if st.button("Test Connection", use_container_width=True, disabled=not engine_available):
            _dispatch_command("TEST_CONNECTION")
    with r1c3:
        if st.button("Refresh Snapshot", use_container_width=True, disabled=not engine_available):
            _dispatch_command("REFRESH")
    with r1c4:
        if st.button("Refresh Dashboard", use_container_width=True):
            st.rerun()

    st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)

    # Row 2 – Trading actions
    st.markdown("<div style='font-size:0.69rem;font-weight:700;text-transform:uppercase;letter-spacing:0.09em;color:#64748b;margin-bottom:6px;'>Trading Handlinger</div>", unsafe_allow_html=True)
    r2c1, r2c2, r2c3, r2c4 = st.columns(4)
    with r2c1:
        if st.button("Start Auto", use_container_width=True, disabled=not ready_to_start):
            _dispatch_command("START")
    with r2c2:
        if st.button("Stop Auto", use_container_width=True, disabled=not engine_available):
            _dispatch_command("STOP")
    with r2c3:
        if st.button("Disconnect Adapter", use_container_width=True, disabled=not engine_available):
            _dispatch_command("DISCONNECT")
    with r2c4:
        if st.button("🚨 Emergency Flat", use_container_width=True, disabled=not engine_available):
            _dispatch_command("FLAT")

    sc1, sc2, sc3, sc4 = st.columns(4)
    sc1.markdown(_inline_status_card("Connected",      "Yes" if connected     else "No",  connected),                                        unsafe_allow_html=True)
    sc2.markdown(_inline_status_card("Auto Requested", "Yes" if auto_requested else "No", auto_requested, warn=not auto_requested),           unsafe_allow_html=True)
    sc3.markdown(_inline_status_card("Worker Running", "Yes" if worker_running else "No", worker_running),                                    unsafe_allow_html=True)
    sc4.markdown(_inline_status_card("Position Open",  "Yes" if position_open  else "No", True if position_open else True, warn=True),        unsafe_allow_html=True)

    if ready_to_start:
        st.success("Auto kan startes. Kritiske checks er grønne.")
    else:
        blocker_text = " | ".join(blockers[:4]) if blockers else "Manglende readiness-data."
        st.warning(f"Start Auto er låst, indtil disse checks er løst: {blocker_text}.")


def _snapshot_rows(snapshot: dict[str, Any] | None) -> pd.DataFrame:
    snap = dict(snapshot or {})
    rows = [
        {"Field": "Account", "Value": str(snap.get("account_value") or "N/A")},
        {"Field": "Account OK", "Value": bool(snap.get("account_ok", False))},
        {"Field": "Instrument", "Value": str(snap.get("instrument_match") or "N/A")},
        {"Field": "Instrument Visible", "Value": bool(snap.get("instrument_visible", False))},
        {"Field": "Position Qty", "Value": snap.get("position_qty", 0)},
        {"Field": "Order Qty", "Value": str(snap.get("order_quantity_value") or "N/A")},
        {"Field": "Last Price", "Value": str(snap.get("last_price_text") or "N/A")},
        {"Field": "Bid", "Value": str(snap.get("bid_price_text") or "N/A")},
        {"Field": "Ask", "Value": str(snap.get("ask_price_text") or "N/A")},
        {"Field": "Clock", "Value": str(snap.get("market_clock_text") or "N/A")},
    ]
    return pd.DataFrame(rows)


def _format_runtime_scalar(value: Any, *, float_decimals: int = 1) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.{float_decimals}f}"
    token = str(value).strip()
    return token or "N/A"


def _runtime_badge(label: str, value: str, tone: str = "neutral") -> str:
    safe_tone = tone if tone in {"good", "warn", "error"} else "neutral"
    return (
        f"<div class='ff-badge {safe_tone}'>"
        f"<span>{escape(label)}</span>"
        f"<span>{escape(value)}</span>"
        "</div>"
    )


def _runtime_note_box(title: str, body: str, tone: str = "neutral") -> str:
    safe_tone = tone if tone in {"good", "warn", "error"} else "neutral"
    return (
        f"<div class='ff-note-box {safe_tone}'>"
        f"<div class='title'>{escape(title)}</div>"
        f"<div class='body'>{escape(body)}</div>"
        "</div>"
    )


def _runtime_tile(label: str, value: Any, *, float_decimals: int = 1) -> str:
    return (
        "<div class='ff-mini-card'>"
        f"<span class='label'>{escape(label)}</span>"
        f"<span class='value'>{escape(_format_runtime_scalar(value, float_decimals=float_decimals))}</span>"
        "</div>"
    )


def _render_snapshot_panel(engine_status: dict[str, Any]) -> None:
    st.markdown("### Tradovate Snapshot")
    snapshot = engine_status.get("tradovate_snapshot") if isinstance(engine_status.get("tradovate_snapshot"), dict) else None
    if snapshot is None:
        st.info("Ingen snapshot-data endnu. Brug **Refresh Snapshot** efter connect.")
        return

    # --- 4 status cards via native columns so grid is Streamlit-managed ---
    c1, c2, c3, c4 = st.columns(4)
    acc_ok = bool(snapshot.get("account_ok", False))
    inst_ok = bool(snapshot.get("instrument_visible", False))
    quote_ok = bool(snapshot.get("quote_ready", False))
    pos_open = bool(snapshot.get("position_open", False))

    def _snap_card(col: Any, label: str, value: str, ok: bool, neutral: bool = False) -> None:
        tone = ("good" if ok else ("neutral" if neutral else "error"))
        dot_color = "#22c55e" if ok else ("#64748b" if neutral else "#ef4444")
        bg = "rgba(34,197,94,0.10)" if ok else ("rgba(255,255,255,0.02)" if neutral else "rgba(239,68,68,0.10)")
        border = "rgba(34,197,94,0.30)" if ok else ("rgba(255,255,255,0.07)" if neutral else "rgba(239,68,68,0.30)")
        val_color = "#86efac" if ok else ("#94a3b8" if neutral else "#fca5a5")
        col.markdown(
            f"""<div style="background:{bg};border:1px solid {border};border-radius:12px;padding:12px 14px;">
                <div style="display:flex;align-items:center;gap:7px;margin-bottom:6px;">
                    <span style="width:8px;height:8px;border-radius:50%;background:{dot_color};display:inline-block;flex-shrink:0;"></span>
                    <span style="font-size:0.7rem;font-weight:700;text-transform:uppercase;letter-spacing:0.09em;color:#64748b;">{escape(label)}</span>
                </div>
                <div style="font-size:1.05rem;font-weight:800;color:{val_color};">{escape(value)}</div>
            </div>""",
            unsafe_allow_html=True,
        )

    _snap_card(c1, "Account Match", "OK" if acc_ok else "Mismatch", acc_ok)
    _snap_card(c2, "Instrument", "Visible" if inst_ok else "Missing", inst_ok)
    _snap_card(c3, "Quote Feed", "Ready" if quote_ok else "No Quote", quote_ok)
    _snap_card(c4, "Position", "Open" if pos_open else "Flat", pos_open, neutral=not pos_open)

    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    # --- Broker Snapshot: 2-column KV table ---
    left_fields = [
        ("Account",      str(snapshot.get("account_value") or "N/A")),
        ("Instrument",   str(snapshot.get("instrument_match") or "N/A")),
        ("Order Qty",    str(snapshot.get("order_quantity_value") or "N/A")),
        ("Position Qty", str(snapshot.get("position_qty", 0))),
    ]
    right_fields = [
        ("Bid",        str(snapshot.get("bid_price_text") or "N/A")),
        ("Ask",        str(snapshot.get("ask_price_text") or "N/A")),
        ("Last Price", str(snapshot.get("last_price_text") or "N/A")),
        ("Clock",      str(snapshot.get("market_clock_text") or "N/A")),
    ]

    def _kv_rows(fields: list[tuple[str, str]]) -> str:
        rows = ""
        for k, v in fields:
            rows += (
                f"<div style='display:flex;justify-content:space-between;align-items:baseline;"
                f"padding:7px 0;border-bottom:1px solid rgba(255,255,255,0.045);'>"
                f"<span style='font-size:0.8rem;color:#64748b;font-weight:500;'>{escape(k)}</span>"
                f"<span style='font-size:0.875rem;font-weight:700;color:#e4e8f0;'>{escape(v)}</span>"
                f"</div>"
            )
        return rows

    bl, br = st.columns(2)
    bl.markdown(
        f"<div style='background:#12151e;border:1px solid #252b3b;border-radius:14px;padding:14px 16px;'>"
        f"<div style='font-size:0.69rem;font-weight:800;text-transform:uppercase;letter-spacing:0.1em;color:#64748b;margin-bottom:8px;'>Account &amp; Position</div>"
        f"{_kv_rows(left_fields)}"
        f"</div>",
        unsafe_allow_html=True,
    )
    br.markdown(
        f"<div style='background:#12151e;border:1px solid #252b3b;border-radius:14px;padding:14px 16px;'>"
        f"<div style='font-size:0.69rem;font-weight:800;text-transform:uppercase;letter-spacing:0.1em;color:#64748b;margin-bottom:8px;'>Priser &amp; Clock</div>"
        f"{_kv_rows(right_fields)}"
        f"</div>",
        unsafe_allow_html=True,
    )

    bars = engine_status.get("tradovate_15m_bars")
    if isinstance(bars, list) and bars:
        bars_frame = pd.DataFrame(bars[-5:])
        if not bars_frame.empty:
            rename_map = {
                "timestamp_dk": "Tid (DK)",
                "bar_index": "#",
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "sample_count": "Samples",
            }
            ordered = [c for c in ("timestamp_dk", "bar_index", "open", "high", "low", "close", "sample_count") if c in bars_frame.columns]
            with st.expander("Recent 15m bars", expanded=False):
                st.dataframe(bars_frame[ordered].rename(columns=rename_map), use_container_width=True, hide_index=True)
    with st.expander("Snapshot details", expanded=False):
        st.table(_snapshot_rows(snapshot))


def _status_pill_inline(label: str, value: str, ok: bool | None = None) -> str:
    """Inline pill with optional green/red/gray coloring."""
    if ok is True:
        bg, border, vc = "rgba(34,197,94,0.12)", "rgba(34,197,94,0.30)", "#86efac"
    elif ok is False:
        bg, border, vc = "rgba(239,68,68,0.12)", "rgba(239,68,68,0.30)", "#fca5a5"
    else:
        bg, border, vc = "rgba(255,255,255,0.05)", "rgba(255,255,255,0.10)", "#94a3b8"
    return (
        f"<span style='display:inline-flex;align-items:center;gap:5px;background:{bg};"
        f"border:1px solid {border};border-radius:999px;padding:4px 10px;"
        f"font-size:0.76rem;font-weight:700;margin:2px;'>"
        f"<span style='color:#64748b;'>{escape(label)}</span>"
        f"<span style='color:{vc};'>{escape(value)}</span>"
        f"</span>"
    )


def _kv_row_inline(label: str, value: str) -> str:
    return (
        f"<div style='display:flex;justify-content:space-between;align-items:baseline;"
        f"padding:8px 0;border-bottom:1px solid rgba(255,255,255,0.045);'>"
        f"<span style='font-size:0.8rem;color:#64748b;font-weight:500;white-space:nowrap;'>{escape(label)}</span>"
        f"<span style='font-size:0.875rem;font-weight:700;color:#e4e8f0;margin-left:12px;text-align:right;'>{escape(value)}</span>"
        f"</div>"
    )


def _section_header_inline(title: str) -> str:
    return (
        f"<div style='font-size:0.69rem;font-weight:800;text-transform:uppercase;"
        f"letter-spacing:0.1em;color:#64748b;margin-bottom:10px;display:flex;align-items:center;gap:6px;'>"
        f"<span style='width:3px;height:10px;background:#22c55e;border-radius:2px;display:inline-block;flex-shrink:0;'></span>"
        f"{escape(title)}"
        f"</div>"
    )


def _card_wrap(content: str, extra_style: str = "") -> str:
    return (
        f"<div style='background:#12151e;border:1px solid #252b3b;border-radius:14px;"
        f"padding:16px 18px;{extra_style}'>{content}</div>"
    )


def _note_inline(title: str, body: str, ok: bool | None = None) -> str:
    if ok is True:
        bg, border = "rgba(34,197,94,0.11)", "rgba(34,197,94,0.28)"
    elif ok is False:
        bg, border = "rgba(239,68,68,0.11)", "rgba(239,68,68,0.28)"
    else:
        bg, border = "rgba(245,158,11,0.10)", "rgba(245,158,11,0.26)"
    return (
        f"<div style='background:{bg};border:1px solid {border};border-radius:10px;"
        f"padding:10px 13px;margin-top:10px;'>"
        f"<div style='font-size:0.69rem;font-weight:800;text-transform:uppercase;letter-spacing:0.1em;color:#64748b;'>{escape(title)}</div>"
        f"<div style='font-size:0.855rem;font-weight:500;color:#e4e8f0;margin-top:4px;line-height:1.45;'>{escape(body)}</div>"
        f"</div>"
    )


def _render_runtime_status(engine_status: dict[str, Any]) -> None:
    st.markdown("### Runtime Status")

    # --- extract data ---
    diagnostics       = engine_status.get("diagnostics") if isinstance(engine_status.get("diagnostics"), dict) else {}
    live_state        = engine_status.get("live_state") if isinstance(engine_status.get("live_state"), dict) else {}
    runtime_profile   = engine_status.get("runtime_profile") if isinstance(engine_status.get("runtime_profile"), dict) else {}
    last_result       = str(engine_status.get("last_result", "") or "").strip()
    observer_status   = str(engine_status.get("live_observer_status", "") or "").strip()
    last_error        = str(engine_status.get("last_error", "") or "").strip()
    watchdog          = diagnostics.get("watchdog") if isinstance(diagnostics.get("watchdog"), dict) else {}
    post_entry_health = diagnostics.get("post_entry_health") if isinstance(diagnostics.get("post_entry_health"), dict) else {}
    preflight         = diagnostics.get("preflight") if isinstance(diagnostics.get("preflight"), dict) else {}
    auto_requested    = bool(engine_status.get("auto_requested", False))
    runtime_active    = bool(engine_status.get("runtime_active", False))
    phase             = str(live_state.get("phase", "") or "idle").replace("_", " ").title()
    position_open     = bool(live_state.get("position_open", False))
    reconcile_req     = bool(live_state.get("reconcile_required", False))
    pending_signal_id = str(live_state.get("pending_signal_id", "") or "").strip()
    pending_event     = str(live_state.get("pending_event", "") or "").strip()
    queue_depth       = diagnostics.get("queue_depth", 0)
    feed_age          = diagnostics.get("feed_age_seconds")
    snap_age          = diagnostics.get("snapshot_age_seconds")
    last_note         = str(live_state.get("last_note", "") or "").strip()
    position_qty      = live_state.get("broker_position_qty")
    entry_price       = live_state.get("entry_price")
    stop_price        = live_state.get("stop_price")
    active_stop       = live_state.get("active_stop")
    risk_pts          = live_state.get("risk_pts")
    add_count_sent    = live_state.get("add_count_sent")
    last_dispatch     = str(diagnostics.get("last_dispatch_summary", "") or "").strip()
    last_confirm      = str(diagnostics.get("last_confirmation_summary", "") or "").strip()
    pending_sig_short = (pending_signal_id[:14] + "…") if len(pending_signal_id) > 14 else (pending_signal_id or "—")
    preflight_ok      = bool(preflight.get("success", False))
    observer_ok       = "ok" in observer_status.lower() or "klar" in observer_status.lower() if observer_status else None

    # ================================================================
    # ROW 1 – Status pills (always visible, at a glance)
    # ================================================================
    phase_ok = phase.lower() not in {"idle", "manual reconcile", "waiting for setup"}
    pills_html = (
        "<div style='display:flex;flex-wrap:wrap;gap:4px;margin-bottom:14px;'>"
        + _status_pill_inline("Runtime",   "Armed"     if runtime_active  else "Inactive", runtime_active)
        + _status_pill_inline("Auto",      "Requested" if auto_requested  else "Idle",     auto_requested or None)
        + _status_pill_inline("Phase",     phase,                                          phase_ok or None)
        + _status_pill_inline("Position",  "Open"      if position_open   else "Flat",     position_open or None)
        + _status_pill_inline("Reconcile", "Required"  if reconcile_req   else "Clear",    None if not reconcile_req else False)
        + _status_pill_inline("Preflight", "OK"        if preflight_ok    else "Check",    preflight_ok)
        + _status_pill_inline("Queue",     str(int(queue_depth or 0)),                     None if int(queue_depth or 0) == 0 else False)
        + "</div>"
    )
    st.markdown(pills_html, unsafe_allow_html=True)

    # ================================================================
    # ROW 2 – Two columns: Profile | Live State
    # ================================================================
    left, right = st.columns([1, 1])

    with left:
        # Runtime Profile card
        profile_rows = (
            _kv_row_inline("Strategy",       runtime_profile.get("strategy_name", "School Run"))
            + _kv_row_inline("Execution",    runtime_profile.get("execution_model", "Aggressiv"))
            + _kv_row_inline("Bar 1 start",  runtime_profile.get("bar1_start", "14:00"))
            + _kv_row_inline("Contracts",    str(runtime_profile.get("fixed_contracts", st.session_state.get("ff_fixed_contracts", 1))))
            + _kv_row_inline("Max adds",     str(runtime_profile.get("max_add_to_winners", 2)))
            + _kv_row_inline("Contract",     runtime_profile.get("contract_symbol", "MYM"))
        )
        st.markdown(
            _card_wrap(_section_header_inline("Runtime Profile") + profile_rows),
            unsafe_allow_html=True,
        )

    with right:
        # Live State card
        if position_open:
            state_rows = (
                _kv_row_inline("Position Qty",  _format_runtime_scalar(position_qty))
                + _kv_row_inline("Entry",       _format_runtime_scalar(entry_price))
                + _kv_row_inline("Stop",        _format_runtime_scalar(stop_price))
                + _kv_row_inline("Active Stop", _format_runtime_scalar(active_stop))
                + _kv_row_inline("Risk (pts)",  _format_runtime_scalar(risk_pts))
                + _kv_row_inline("Adds Sent",   _format_runtime_scalar(add_count_sent))
            )
        else:
            state_rows = (
                _kv_row_inline("Position Qty",    _format_runtime_scalar(position_qty))
                + _kv_row_inline("Pending Event", pending_event or "—")
                + _kv_row_inline("Pending Signal",pending_sig_short)
            )
        st.markdown(
            _card_wrap(_section_header_inline("Live State") + state_rows),
            unsafe_allow_html=True,
        )

    # ================================================================
    # ROW 3 – Diagnostics & Events (collapsible)
    # ================================================================
    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    diag_rows = (
        _kv_row_inline("Feed age",     f"{_format_runtime_scalar(feed_age)} s")
        + _kv_row_inline("Snapshot age", f"{_format_runtime_scalar(snap_age)} s")
        + _kv_row_inline("Broker qty",  _format_runtime_scalar(position_qty))
        + _kv_row_inline("Account",     _format_runtime_scalar(live_state.get("broker_account_value")))
    )
    health_rows = (
        _kv_row_inline("Watchdog",   str(watchdog.get("headline", "Inaktiv") or "Inaktiv").replace("WATCHDOG ", ""))
        + _kv_row_inline("Post-entry", str(post_entry_health.get("headline", "Inaktiv") or "Inaktiv").replace("POST-ENTRY HEALTH ", ""))
    )

    dl, dr = st.columns([1, 1])
    dl.markdown(
        _card_wrap(_section_header_inline("Diagnostics") + diag_rows),
        unsafe_allow_html=True,
    )
    dr.markdown(
        _card_wrap(_section_header_inline("Health") + health_rows),
        unsafe_allow_html=True,
    )

    # ================================================================
    # ROW 4 – Notifications (only shown when they contain data)
    # ================================================================
    notes: list[tuple[str, str, bool | None]] = []
    if last_note:
        notes.append(("Last Note", last_note, None))
    if last_result:
        notes.append(("Last Result", last_result, True))
    if observer_status:
        notes.append(("Observer", observer_status, observer_ok))
    if last_error:
        notes.append(("Last Error", last_error, False))
    if last_dispatch and last_dispatch != "Ingen dispatch endnu":
        notes.append(("Last Dispatch", last_dispatch, True))
    if last_confirm and last_confirm != "Ingen confirmation endnu":
        notes.append(("Last Confirmation", last_confirm, True))

    if notes:
        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        combined = "".join(_note_inline(t, b, ok) for t, b, ok in notes)
        st.markdown(
            _card_wrap(_section_header_inline("Notifications") + combined),
            unsafe_allow_html=True,
        )

    with st.expander("Diagnostics Details", expanded=False):
        st.write(f"Watchdog: {str(watchdog.get('detail', '') or 'N/A')}")
        st.write(f"Post-entry: {str(post_entry_health.get('detail', '') or 'N/A')}")
        post_entry_checks = post_entry_health.get("checks", [])
        if isinstance(post_entry_checks, list) and post_entry_checks:
            checks_frame = pd.DataFrame(post_entry_checks)
            if not checks_frame.empty:
                st.dataframe(checks_frame, use_container_width=True, hide_index=True)
        events = diagnostics.get("events", []) if isinstance(diagnostics, dict) else []
        if isinstance(events, list) and events:
            events_frame = pd.DataFrame(events[-10:])
            if not events_frame.empty:
                st.dataframe(events_frame, use_container_width=True, hide_index=True)

    with st.expander("Live State Details", expanded=False):
        detail_rows = [
            {"Field": "Trade date",       "Value": _format_runtime_scalar(live_state.get("trade_date"))},
            {"Field": "Phase",            "Value": phase},
            {"Field": "Direction",        "Value": _format_runtime_scalar(live_state.get("direction"))},
            {"Field": "Entry price",      "Value": _format_runtime_scalar(entry_price)},
            {"Field": "Stop price",       "Value": _format_runtime_scalar(stop_price)},
            {"Field": "Active stop",      "Value": _format_runtime_scalar(active_stop)},
            {"Field": "Risk pts",         "Value": _format_runtime_scalar(risk_pts)},
            {"Field": "Break-even armed", "Value": _format_runtime_scalar(live_state.get("break_even_armed"))},
            {"Field": "Add count sent",   "Value": _format_runtime_scalar(add_count_sent)},
            {"Field": "Position open",    "Value": _format_runtime_scalar(position_open)},
            {"Field": "Reconcile required","Value": _format_runtime_scalar(reconcile_req)},
            {"Field": "Market timestamp", "Value": _format_runtime_scalar(live_state.get("market_timestamp"))},
            {"Field": "Last reconciled",  "Value": _format_runtime_scalar(live_state.get("last_reconciled_at"))},
            {"Field": "Pending event",    "Value": _format_runtime_scalar(pending_event)},
            {"Field": "Pending signal",   "Value": _format_runtime_scalar(pending_signal_id)},
            {"Field": "Last note",        "Value": _format_runtime_scalar(last_note)},
        ]
        st.table(pd.DataFrame(detail_rows))

    with st.expander("Raw engine payload", expanded=False):
        st.json(engine_status, expanded=False)


def _render_system_banner(service_status: dict[str, Any], chrome_status: dict[str, Any], engine_status: dict[str, Any]) -> None:
    """Global top-of-page status banner – Heuristic 1: Visibility of system status."""
    running = bool(service_status.get("running", False))
    chrome_ok = bool(chrome_status.get("ready", False))
    connected = bool(engine_status.get("connected", False))
    auto_on = bool(engine_status.get("auto_requested", False))

    if running and chrome_ok and connected and auto_on:
        tone, icon, label = "good",  "🟢", "AUTO KØRER — alle systemer grønne"
    elif running and chrome_ok and connected:
        tone, icon, label = "good",  "🟢", "System online — auto ikke startet endnu"
    elif running and not chrome_ok:
        tone, icon, label = "warn",  "🟡", "Engine kører — Chrome CDP offline"
    elif running and chrome_ok and not connected:
        tone, icon, label = "warn",  "🟡", "Chrome klar — adapter ikke forbundet"
    elif not running:
        tone, icon, label = "error", "🔴", "Engine service er stoppet — start den i Engine Control"
    else:
        tone, icon, label = "warn",  "🟡", "Delvist online — tjek preflight"

    color_map = {
        "good":  ("var(--ff-green-glow)", "var(--ff-green-border)", "var(--ff-green-text)"),
        "warn":  ("var(--ff-amber-glow)", "var(--ff-amber-border)", "var(--ff-amber-text)"),
        "error": ("var(--ff-red-glow)",   "var(--ff-red-border)",   "var(--ff-red-text)"),
    }
    bg, border, text = color_map[tone]
    st.markdown(
        f"""
        <div style="
            background:{bg};
            border:1px solid {border};
            border-radius:12px;
            padding:10px 16px;
            margin-bottom:14px;
            display:flex;
            align-items:center;
            gap:10px;
            font-weight:700;
            font-size:0.875rem;
            color:{text};
            animation: fadeUp 0.3s ease;
        ">
            <span style="font-size:1rem">{icon}</span>
            <span>{escape(label)}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    _set_page_style()
    _init_session_state()
    _render_flash()
    _render_settings_panel()

    service_status = _service_status()
    chrome_status = _chrome_status()
    engine_status = _engine_status()

    _render_system_banner(service_status, chrome_status, engine_status)

    session_clock = resolve_school_run_session_clock_dk(_time_token(st.session_state.get("ff_bar1_start", time(14, 0))))
    _render_header(session_clock)
    _render_operator_flow()

    _render_readiness_panel(service_status, chrome_status, engine_status)
    _render_service_panel(service_status, chrome_status, engine_status)
    st.divider()
    _render_command_panel(service_status, engine_status)
    st.divider()
    left, right = st.columns([0.9, 1.1])
    with left:
        _render_snapshot_panel(engine_status)
    with right:
        _render_runtime_status(engine_status)


if __name__ == "__main__":
    main()
