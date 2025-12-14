# cmc_loader.py
import os
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import requests
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

CMC_API_KEY = os.getenv("CMC_API_KEY")
if not CMC_API_KEY:
    raise RuntimeError("CMC_API_KEY not set in .env file")

BASE_URL_CMC = "https://pro-api.coinmarketcap.com/v1"

HEADERS_CMC = {
    "X-CMC_PRO_API_KEY": CMC_API_KEY,
    "Accept": "application/json",
}

PROJECT_ID = "tokyo-data-473514-h8"
DATASET_ID = "crypto_analytics"
LISTINGS_TABLE_ID = "cmc_listings_latest"

bq_client = bigquery.Client(project=PROJECT_ID)


def _cmc_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{BASE_URL_CMC}{path}"
    resp = requests.get(url, headers=HEADERS_CMC, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_listings_latest(limit: int = 200, convert: str = "USD") -> List[Dict[str, Any]]:
    params = {"start": 1, "limit": limit, "convert": convert}
    raw = _cmc_get("/cryptocurrency/listings/latest", params=params)
    snapshot_time = datetime.now(timezone.utc).isoformat()

    cleaned: List[Dict[str, Any]] = []
    for item in raw["data"]:
        quote = item["quote"][convert]
        record = {
            "snapshot_time": snapshot_time,
            "cmc_id": item["id"],
            "name": item["name"],
            "symbol": item["symbol"],
            "slug": item.get("slug"),
            "cmc_rank": item.get("cmc_rank"),
            "circulating_supply": item.get("circulating_supply"),
            "total_supply": item.get("total_supply"),
            "max_supply": item.get("max_supply"),
            "num_market_pairs": item.get("num_market_pairs"),
            "date_added": item.get("date_added"),
            "tags": json.dumps(item.get("tags", [])),
            "price": quote.get("price"),
            "volume_24h": quote.get("volume_24h"),
            "percent_change_1h": quote.get("percent_change_1h"),
            "percent_change_24h": quote.get("percent_change_24h"),
            "percent_change_7d": quote.get("percent_change_7d"),
            "market_cap": quote.get("market_cap"),
            "last_updated": quote.get("last_updated"),
        }
        cleaned.append(record)
    return cleaned


def write_to_bigquery(rows: List[Dict[str, Any]]):
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{LISTINGS_TABLE_ID}"
    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        print(f"BigQuery insert errors (cmc): {errors}")
        raise RuntimeError(f"BigQuery insert errors (cmc): {errors}")


def main(limit: int = 200) -> List[Dict[str, Any]]:
    listings = fetch_listings_latest(limit=limit)
    write_to_bigquery(listings)

    print(f"\n=== CMC: inserted {len(listings)} listings rows ===")
    print(json.dumps(listings[:3], indent=2))

    return listings
