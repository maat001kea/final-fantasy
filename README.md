# Final Fantasy

Minimal split-engine app for `School Run + Aggressiv` full auto trading.

## Locked trade model

- Strategy: `School Run`
- Execution model: `Aggressiv`
- Break-even: `+0.25R`
- Trail activation: `+0.45R`
- Trail giveback: `0.20R`
- Add #1: `+0.55R`
- Add #2: `+1.10R`
- `max_add_to_winners = 2`
- Every add is `1:1` with the starter size
- `fixed_contracts` is user-selected in the dashboard
- `bar1_start` is user-selected in the dashboard, default `14:00` DK

## Architecture

- `app.py`
  - Small Streamlit dashboard only
  - Starts/stops the engine supervisor
  - Starts/reuses visible CDP Chrome
  - Sends bridge commands like `CONNECT`, `START`, `STOP`, `REFRESH`, `FLAT`
- `trading_engine_service.py`
  - Supervisor process
  - Restarts the engine if heartbeat goes stale
- `trading_engine.py`
  - Engine process
  - Owns runtime state, CDP adapter, snapshot loop and live observer
- `src/`
  - Only the modules needed for `School Run` execution, routing, risk-gate and CDP runtime

## Quick start

```bash
cd /Users/mahmoudatie/Desktop/final-fantasy
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Basic flow

1. Set `fixed_contracts`, `bar1_start`, CDP port and Tradovate account token in the sidebar.
2. Click `Start Engine Service`.
3. Click `Start / Reuse Chrome`.
4. Open Tradovate in that Chrome profile if needed.
5. Click `Connect`.
6. Click `Refresh Snapshot` and verify account, contract and quantity.
7. Click `Start Auto`.

## Notes

- Chrome is launched visible, not headless.
- Runtime files live under `/Users/mahmoudatie/Desktop/final-fantasy/output`.
- Strategy defaults are aligned to `DOW -> MYM`.
- Risk/account defaults still come from:
  - `/Users/mahmoudatie/Desktop/final-fantasy/config/account_config.yaml`
  - `/Users/mahmoudatie/Desktop/final-fantasy/config/live_execution.yaml`
  - `/Users/mahmoudatie/Desktop/final-fantasy/config/position_sizing.yaml`
