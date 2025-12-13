import os
import json
import requests
from datetime import datetime, timezone

from dotenv import load_dotenv  # pip install python-dotenv
from flask import Flask, jsonify  # pip install flask
from google.cloud import bigquery  # pip install google-cloud-bigquery


# 1. Config and API key
load_dotenv()
API_KEY = os.getenv("COINGECKO_API_KEY")  # in .env: COINGECKO_API_KEY=your_demo_key
BASE_URL = "https://api.coingecko.com/api/v3"
VS_CURRENCY = "usd"

if not API_KEY:
    raise RuntimeError("COINGECKO_API_KEY not set in .env")

headers = {"x-cg-demo-api-key": API_KEY}

# GCP / BigQuery config
PROJECT_ID = "tokyo-data-473514-h8"
DATASET_ID = "crypto_analytics"
TABLE_ID = "top5_markets"

bq_client = bigquery.Client(project=PROJECT_ID)

# 2. Fields you care about from /coins/markets
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


def fetch_top5_markets():
    """Fetch top 5 coins by market cap with full market data."""
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
            record[field] = coin.get(field)
        cleaned.append(record)

    return cleaned


def write_to_bigquery(rows):
    """Insert list of dicts into BigQuery."""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        # Log errors for debugging (visible in Cloud Run logs)
        print(f"BigQuery insert errors: {errors}")
        raise RuntimeError(f"BigQuery insert errors: {errors}")


def main():
    top5 = fetch_top5_markets()
    write_to_bigquery(top5)

    # Logs for debugging (will show in Cloud Run logs)
    print("\n=== FULL JSON (cleaned fields only) ===\n")
    print(json.dumps(top5, indent=2))

    print("\n=== SUMMARY TABLE ===")
    print("id\tprice\tmcap\t24h_change%")
    for c in top5:
        print(
            f"{c['id']}\t"
            f"{c['current_price']}\t"
            f"{c['market_cap']}\t"
            f"{c['price_change_percentage_24h']}"
        )

    return top5


# ---- HTTP entrypoint for Cloud Run ----
app = Flask(__name__)


@app.route("/", methods=["GET"])
def trigger():
    top5 = main()
    # Show data in browser/preview
    return jsonify(top5), 200


if __name__ == "__main__":
    # Local dev
    app.run(host="0.0.0.0", port=8080)
