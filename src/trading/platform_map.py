"""Platform element mapping – CSS selectors and instrument maps for browser-based brokers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class PlatformSelectors:
    """CSS selectors for a trading platform."""

    # Login page
    login_username: str
    login_password: str
    login_submit: str

    # Order entry
    order_quantity: str
    order_buy_button: str
    order_sell_button: str
    order_stop_field: str
    order_confirm: str

    # Positions panel
    positions_table: str
    position_close_button: str

    # Account info
    account_equity: str
    account_balance: str

    # Price display
    price_bid: str
    price_ask: str


TD365_SELECTORS = PlatformSelectors(
    login_username="input#login_userid, input[name='userid'], input[name='username'], input[type='email'], #username",
    login_password="input#login_password, input[name='password'], input[type='password'], #password",
    login_submit="button:has-text('Log in'), button[type='submit'], input[type='submit'], .login-btn",
    order_quantity="input[name='quantity'], input[name='size'], .quantity-input",
    order_buy_button=".buy-button, button.buy, [data-action='buy']",
    order_sell_button=".sell-button, button.sell, [data-action='sell']",
    order_stop_field="input[name='stop'], input[name='stop_loss'], .stop-input",
    order_confirm=".confirm-order, button.confirm, [data-action='confirm']",
    positions_table=".positions-table, #positions, .open-positions",
    position_close_button=".close-position, button.close, [data-action='close']",
    account_equity=".equity, .account-equity, [data-field='equity']",
    account_balance=".balance, .account-balance, [data-field='balance']",
    price_bid=".bid-price, [data-field='bid'], .price-bid",
    price_ask=".ask-price, [data-field='ask'], .price-ask",
)

TRADE_NATION_SELECTORS = PlatformSelectors(
    login_username="input[name='email'], input[type='email'], #email",
    login_password="input[name='password'], input[type='password'], #password",
    login_submit="button[type='submit'], .btn-login, .login-submit",
    order_quantity="input[name='units'], input[name='quantity'], .units-input",
    order_buy_button=".btn-buy, button[data-direction='buy'], .buy-btn",
    order_sell_button=".btn-sell, button[data-direction='sell'], .sell-btn",
    order_stop_field="input[name='stop_loss'], input[name='stop'], .stop-loss-input",
    order_confirm=".btn-confirm, button.confirm-order, [data-action='confirm-trade']",
    positions_table=".positions-list, #open-positions, .positions-container",
    position_close_button=".close-btn, [data-action='close-position'], .position-close",
    account_equity=".equity-value, [data-field='equity'], .account-equity",
    account_balance=".balance-value, [data-field='balance'], .account-balance",
    price_bid=".bid, [data-side='bid'], .market-bid",
    price_ask=".ask, [data-side='ask'], .market-ask",
)

TD365_INSTRUMENT_MAP: dict[str, str] = {
    "DAX": "Germany 40",
    "EURO STOXX 50": "Europe 50",
    "CAC 40": "France 40",
    "AEX": "Netherlands 25",
    "IBEX 35": "Spain 35",
    "SMI": "Switzerland 20",
    "FTSE": "UK 100",
    "DOW": "Wall Street",
    "S&P 500": "US 500",
    "NASDAQ 100": "Tech 100",
    "RUSSELL 2000": "US Small Cap 2000",
    "ITALY 40": "Italy 40",
    "AUSTRALIA 200": "Australia 200",
    "HONG KONG 40": "Hong Kong 40",
    "JAPAN 225": "Japan 225",
    "CHINA A50": "China A50",
    "INDIA 50": "India 50",
    "US DOLLAR INDEX": "US Dollar Index",
    "VOLATILITY INDEX": "Volatility Index",
    "WTI CRUDE OIL": "US Crude Oil",
    "BRENT CRUDE OIL": "Brent Crude Oil",
}

TRADE_NATION_INSTRUMENT_MAP: dict[str, str] = {
    "DAX": "Germany 40",
    "EURO STOXX 50": "Europe 50",
    "CAC 40": "France 40",
    "AEX": "Netherlands 25",
    "IBEX 35": "Spain 35",
    "SMI": "Switzerland 20",
    "FTSE": "FTSE 100",
    "DOW": "US 30",
    "S&P 500": "S&P 500",
    "NASDAQ 100": "US Tech 100",
    "RUSSELL 2000": "US Small Cap 2000",
    "ITALY 40": "Italy 40",
    "AUSTRALIA 200": "Australia 200",
    "HONG KONG 40": "Hong Kong 40",
    "JAPAN 225": "Japan 225",
    "CHINA A50": "China A50",
    "INDIA 50": "India 50",
    "US DOLLAR INDEX": "US Dollar Index",
    "VOLATILITY INDEX": "Volatility Index",
    "WTI CRUDE OIL": "US Crude Oil",
    "BRENT CRUDE OIL": "Brent Crude Oil",
}

# =============================================================================
# TRADOVATE (Custom Human - CDP Direct)
# =============================================================================
TRADOVATE_SELECTORS = PlatformSelectors(
    # Login page - Tradovate web interface
    login_username="input[name='username'], input[name='email'], input[type='email'], #username, #email, input[autocomplete='username']",
    login_password="input[name='password'], input[type='password'], #password, input[autocomplete='current-password']",
    login_submit="button[type='submit'], button:has-text('Log in'), button:has-text('Login'), .login-btn, [data-action='login']",
    
    # Order entry - DOM order panel
    # NOTE: live Tradovate DOM: <input class="form-control" placeholder="Select value" value="10">
    # The bare input.form-control[placeholder='Select value'] is the confirmed live selector;
    # the combobox-scoped variant is kept as fallback.
    order_quantity="input.form-control[placeholder='Select value'], .select-input.combobox input.form-control, input[name='quantity'], input[name='qty'], input[name='orderQty'], .quantity-input, #quantity, [data-field='quantity']",
    order_buy_button=".buy-button, button.buy, [data-action='buy'], button:has-text('Buy'), .btn-buy, [data-side='buy']",
    order_sell_button=".sell-button, button.sell, [data-action='sell'], button:has-text('Sell'), .btn-sell, [data-side='sell']",
    order_stop_field="input[name='stop'], input[name='stop_loss'], input[name='stopPrice'], .stop-input, #stop-price, [data-field='stop']",
    order_confirm=".confirm-order, button.confirm, button:has-text('Confirm'), [data-action='confirm'], .btn-confirm, button:has-text('Submit')",
    
    # Positions panel
    positions_table=".positions-table, #positions, .open-positions, [data-table='positions'], .positions-list",
    position_close_button=".close-position, button.close, [data-action='close'], button:has-text('Close'), .btn-close-position",
    
    # Account info
    account_equity=".equity, .account-equity, [data-field='equity'], #equity, .cash-balance",
    account_balance=".balance, .account-balance, [data-field='balance'], #balance, .available-funds",
    
    # Price display
    price_bid=".bid-price, [data-field='bid'], .price-bid, #bid, .market-bid",
    price_ask=".ask-price, [data-field='ask'], .price-ask, #ask, .market-ask",
)

TRADOVATE_INSTRUMENT_MAP: dict[str, str] = {
    # Indices -> Tradovate futures symbols
    "DOW": "MYM",           # Micro E-mini Dow Jones
    "S&P 500": "MES",       # Micro E-mini S&P 500
    "NASDAQ 100": "MNQ",    # Micro E-mini Nasdaq 100
    "RUSSELL 2000": "M2K",  # Micro E-mini Russell 2000
    # Full-size futures
    "DOW_FULL": "YM",       # E-mini Dow Jones
    "S&P_500_FULL": "ES",   # E-mini S&P 500
    "NASDAQ_100_FULL": "NQ", # E-mini Nasdaq 100
    # Crypto
    "BITCOIN": "BTC",       # Bitcoin futures
    "ETHEREUM": "ETH",      # Ethereum futures
    # Energy
    "WTI_CRUDE_OIL": "CL",  # Crude Oil
    "NATURAL_GAS": "NG",    # Natural Gas
    # Metals
    "GOLD": "GC",           # Gold
    "SILVER": "SI",         # Silver
    # Forex (if available)
    "EUR_USD": "6E",        # Euro FX
    "GBP_USD": "6B",        # British Pound
    "JPY_USD": "6J",        # Japanese Yen
}

PLATFORM_REGISTRY: dict[str, dict[str, Any]] = {
    "td365": {
        "selectors": TD365_SELECTORS,
        "instrument_map": TD365_INSTRUMENT_MAP,
        "base_url": "https://traders.td365.com/login",
    },
    "trade_nation": {
        "selectors": TRADE_NATION_SELECTORS,
        "instrument_map": TRADE_NATION_INSTRUMENT_MAP,
        "base_url": "https://app.tradenation.com",
    },
    "tradovate": {
        "selectors": TRADOVATE_SELECTORS,
        "instrument_map": TRADOVATE_INSTRUMENT_MAP,
        "base_url": "https://trader.tradovate.com",
    },
}
