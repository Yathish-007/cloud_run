# gecko_loader.py
import os
import json
from datetime import datetime, timezone
from typing import List, Dict

import requests
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

API_KEY = os.getenv("COINGECKO_API_KEY")
if not API_KEY:
    raise RuntimeError("COINGECKO_API_KEY not set in .env")

BASE_URL = "https://api.coingecko.com/api/v3"
VS_CURRENCY = "usd"

PROJECT_ID = "tokyo-data-473514-h8"
DATASET_ID = "crypto_analytics"
TABLE_ID = "top5_markets"

bq_client = bigquery.Client(project=PROJECT_ID)

headers = {"x-cg-demo-api-key": API_KEY}

FIELDS = [
    "id", "symbol", "name", "image",
    "current_price", "market_cap", "market_cap_rank",
    "fully_diluted_valuation", "total_volume",
    "high_24h", "low_24h",
    "price_change_24h", "price_change_percentage_24h",
    "market_cap_change_24h", "market_cap_change_percentage_24h",
    "circulating_supply", "total_supply", "max_supply",
    "ath", "ath_change_percentage", "ath_date",
    "atl", "atl_change_percentage", "atl_date",
    "roi", "last_updated",
]


def fetch_top5_markets() -> List[Dict]:
    """Fetch top 5 coins by market cap with selected fields."""
    url = f"{BASE_URL}/coins/markets"
    params = {
        "vs_currency": VS_CURRENCY,
        "order": "market_cap_desc",
        "per_page": 5,
        "page": 1,
        "sparkline": "false",
    }
    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    snapshot_time = datetime.now(timezone.utc).isoformat()

    cleaned = []
    for coin in data:
        record = {"snapshot_time": snapshot_time}
        for field in FIELDS:
            value = coin.get(field)
            if field == "roi" and isinstance(value, dict):
                value = json.dumps(value)
            record[field] = value
        cleaned.append(record)

    return cleaned


def write_to_bigquery(rows: List[Dict]):
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        print(f"BigQuery insert errors (gecko): {errors}")
        raise RuntimeError(f"BigQuery insert errors (gecko): {errors}")


def main() -> List[Dict]:
    top5 = fetch_top5_markets()
    write_to_bigquery(top5)

    print("\n=== CoinGecko: inserted rows ===")
    print(json.dumps(top5, indent=2))

    return top5
