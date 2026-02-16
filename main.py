
import os
import json
import csv
import time
import pathlib
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
import requests
import pandas as pd
from pathlib import Path

CONFIG_PATH = Path("config.json")


def load_config(path: Path = CONFIG_PATH) -> dict:
    """
    Load JSON config if it exists, else return empty dict.
    """
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        # Bad JSON -> treat as missing
        return {}


def get_alpaca_credentials_from_config_or_prompt(config: dict) -> tuple[str, str, str]:
    """
    Returns (api_key_id, api_secret_key, base_url).

    Reads from config.json first.
    Ask if the API is paper or not, adjusts the URL.
    If missing/empty, prompts user one-time (does not save).
    """

    api_key = str(config.get("API_KEY", "") or "").strip()
    api_secret = str(config.get("API_SECRET", "") or "").strip()

    _is_live = input("Exporting from a live account? (Live Trading API) \n"
                     "Default is LIVE. Press Enter to continue with LIVE. \n"
                     "Enter 'N' if not. \n"
                     ": ")
    if (_is_live == 'N') or (_is_live == 'n'):
        base_url = str(config.get("API_KEY_PAPER", "https://paper-api.alpaca.markets") or "").strip()
    else:
        base_url = str(config.get("API_KEY_LIVE", "https://api.alpaca.markets") or "").strip()

    missing = []
    if not api_key:
        missing.append("APCA_API_KEY_ID")
    if not api_secret:
        missing.append("APCA_API_SECRET_KEY")

    if missing:
        print(
            "No or not enough data found in 'config.json' to fetch data. "
            "Enter the missing fields as a one-time run and mention they won't be saved after the program ends."
        )
        if "APCA_API_KEY_ID" in missing:
            api_key = input("Paste APCA_API_KEY_ID: ").strip()
        if "APCA_API_SECRET_KEY" in missing:
            api_secret = input("Paste APCA_API_SECRET_KEY: ").strip()

    # Final validation
    if not api_key or not api_secret:
        raise SystemExit("Missing or wrong Alpaca API credentials. Exiting.")

    return api_key, api_secret, base_url


def build_alpaca_headers(api_key_id: str, api_secret_key: str) -> dict:
    return {
        "APCA-API-KEY-ID": api_key_id,
        "APCA-API-SECRET-KEY": api_secret_key,
    }


config = load_config()
api_key_id, api_secret_key, base_url = get_alpaca_credentials_from_config_or_prompt(config)
BASE_URL = base_url
TRADING_API = f"{BASE_URL}/v2"
HEADERS = build_alpaca_headers(api_key_id, api_secret_key)


# Make a data export folder
def mkdir_export_dir() -> pathlib.Path:
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    p = pathlib.Path(f"/Data/alpaca_paper_export_{stamp}")
    p.mkdir(parents=True, exist_ok=True)
    return p


def save_json(path: pathlib.Path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def _save_csv(path: pathlib.Path, items):
    # items: list[dict] or dict -> list[dict]
    if isinstance(items, dict):
        # Normalize dict -> single row
        items = [items]
    if not items:
        # Create empty file with no rows
        pathlib.Path(path).write_text("", encoding="utf-8")
        return
    # Normalize fields
    all_keys = set()
    for it in items:
        all_keys.update(it.keys())
    fieldnames = sorted(all_keys)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for it in items:
            w.writerow({k: it.get(k, None) for k in fieldnames})


def _robust_get(url, headers, params=None):
    for attempt in range(5):
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 429:
            time.sleep(1 + attempt)
            continue
        r.raise_for_status()
        return r


def collect_with_pagination(url, headers, initial_params=None, hard_limit=None):
    items = []
    params = dict(initial_params or {})
    seen = 0
    while True:
        r = _robust_get(url, headers=headers, params=params)
        data = r.json()
        # If the endpoint returns a list directly
        if isinstance(data, list):
            items.extend(data)
        elif isinstance(data, dict):
            # Some endpoints wrap results under a key; try to find a list
            # Prefer 'orders' or 'activities' if present, else any list value
            if "orders" in data and isinstance(data["orders"], list):
                items.extend(data["orders"])
            elif "activities" in data and isinstance(data["activities"], list):
                items.extend(data["activities"])
            else:
                # Fallback: try to detect first list in dict
                list_values = [v for v in data.values() if isinstance(v, list)]
                if list_values:
                    items.extend(list_values[0])
                else:
                    # No list, assume single object
                    items.append(data)
        else:
            break

        seen += 1
        if hard_limit and len(items) >= hard_limit:
            items = items[:hard_limit]
            break

        # Try to find next page token
        next_token = None
        if isinstance(data, dict):
            next_token = data.get("next_page_token") or data.get("next_page_id")
        # Some Alpaca endpoints include paging token in headers
        if not next_token:
            next_token = r.headers.get("x-next-page-token") or r.headers.get("next_page_token")

        if next_token:
            params = dict(params)
            params["page_token"] = next_token
        else:
            break
    return items


def get_account():
    return _robust_get(f"{TRADING_API}/account", headers=HEADERS).json()


def get_clock():
    return _robust_get(f"{TRADING_API}/clock", headers=HEADERS).json()


def get_positions():
    return _robust_get(f"{TRADING_API}/positions", headers=HEADERS).json()


def get_orders(after_iso=None, until_iso=None, status="all", limit=100):
    params = {
        "status": status,
        "limit": min(limit, 500),
        "nested": "true",  # include legs for multi-leg orders
    }
    if after_iso:
        params["after"] = after_iso
    if until_iso:
        params["until"] = until_iso
    return collect_with_pagination(f"{TRADING_API}/orders", HEADERS, params)


def get_activities(activity_types=None, after_iso=None, until_iso=None, direction="desc", page_limit=100):
    """
    Activity types examples: FILL, TRANS, TRADE, DIV, MISC, etc.
    """
    params = {
        "direction": direction,
        "page_size": min(page_limit, 100),
    }
    if activity_types:
        # Can be a single type or comma-separated list
        params["activity_types"] = activity_types
    if after_iso:
        params["after"] = after_iso
    if until_iso:
        params["until"] = until_iso
    return collect_with_pagination(f"{TRADING_API}/account/activities", HEADERS, params)


def get_portfolio_history(period="1A", timeframe="1D", extended_hours="false"):
    params = {
        "period": period,           # 1D, 1W, 1M, 3M, 6M, 1A, all
        "timeframe": timeframe,     # 1Min, 5Min, 15Min, 1H, 1D
        "extended_hours": extended_hours,
    }
    return _robust_get(f"{TRADING_API}/account/portfolio/history", HEADERS, params).json()


def normalize_portfolio_history_to_rows(ph_json):
    """
    Convert portfolio history arrays into row-wise dicts (timestamp, equity, profit_loss, profit_loss_pct, base_value)
    """
    if not isinstance(ph_json, dict) or "timestamp" not in ph_json:
        return []
    ts = ph_json.get("timestamp", []) or []
    eq = ph_json.get("equity", []) or []
    pl = ph_json.get("profit_loss", []) or []
    pl_pct = ph_json.get("profit_loss_pct", []) or []
    base = ph_json.get("base_value", None)

    rows = []
    for i in range(len(ts)):
        rows.append({
            "timestamp": ts[i],
            "datetime_utc": datetime.fromtimestamp(ts[i], tz=timezone.utc).isoformat(),
            "equity": eq[i] if i < len(eq) else None,
            "profit_loss": pl[i] if i < len(pl) else None,
            "profit_loss_pct": pl_pct[i] if i < len(pl_pct) else None,
            "base_value": base,
            "timeframe": ph_json.get("timeframe"),
        })
    return rows


def to_dataframe_safe(items):
    if isinstance(items, dict):
        return pd.DataFrame([items])
    if isinstance(items, list) and items and isinstance(items[0], dict):
        return pd.DataFrame(items)
    # Fallback: wrap raw
    return pd.DataFrame({"value": [items]})


def main():
    outdir = mkdir_export_dir()
    # Time window defaults: last 90 days for orders/activities
    until_dt = datetime.now(timezone.utc)
    after_dt = until_dt - timedelta(days=180)
    after_iso = after_dt.isoformat()
    until_iso = until_dt.isoformat()

    print(f"Export directory: {outdir}")
    print("Fetching account...")
    account = get_account()
    save_json(outdir / "account.json", account)
    _save_csv(outdir / "account.csv", account)

    print("Fetching market clock...")
    clock = get_clock()
    save_json(outdir / "clock.json", clock)
    _save_csv(outdir / "clock.csv", clock)

    print("Fetching positions...")
    positions = get_positions()
    save_json(outdir / "positions.json", positions)
    _save_csv(outdir / "positions.csv", positions)

    print(f"Fetching orders (status=all, last 90 days)...")
    orders = get_orders(after_iso=after_iso, until_iso=until_iso, status="all", limit=500)
    save_json(outdir / "orders.json", orders)
    _save_csv(outdir / "orders.csv", orders)

    print(f"Fetching activities (last 90 days)...")
    activities = get_activities(after_iso=after_iso, until_iso=until_iso, direction="desc", page_limit=100)
    save_json(outdir / "activities.json", activities)
    _save_csv(outdir / "activities.csv", activities)

    print("Fetching portfolio history (1 year, 1D candles)...")
    ph = get_portfolio_history(period="1A", timeframe="1D", extended_hours="false")
    ph_rows = normalize_portfolio_history_to_rows(ph)
    save_json(outdir / "portfolio_history_raw.json", ph)
    _save_csv(outdir / "portfolio_history_rows.csv", ph_rows)

    # Summary table
    summary = {
        "export_generated_at": datetime.now().isoformat(timespec="seconds"),
        "orders_rows": len(orders) if isinstance(orders, list) else 1,
        "activities_rows": len(activities) if isinstance(activities, list) else 1,
        "positions_rows": len(positions) if isinstance(positions, list) else 1,
    }
    save_json(outdir / "summary.json", summary)
    _save_csv(outdir / "summary.csv", summary)

    # Optional: quick human-readable preview via pandas (not required)
    try:
        print("\n=== Quick preview ===")
        print("Account:")
        print(to_dataframe_safe(account).head(3).to_string(index=False))
        print("\nPositions:")
        print(to_dataframe_safe(positions).head(5).to_string(index=False))
        print("\nRecent Orders:")
        print(to_dataframe_safe(orders).head(5).to_string(index=False))
        print("\nActivities:")
        print(to_dataframe_safe(activities).head(5).to_string(index=False))
        print("\nPortfolio History (first 5 rows):")
        print(pd.DataFrame(ph_rows).head(5).to_string(index=False))
    except Exception as e:
        # Keep export resilient even if preview fails
        print(f"(Preview skipped: {e})")

    print(f"\nDone. Files saved in: {outdir.resolve()}")


if __name__ == "__main__":
    main()
