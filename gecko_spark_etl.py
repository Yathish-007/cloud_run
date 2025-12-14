# gecko_spark_etl.py
import os
import json
import requests
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

PROJECT_ID = "tokyo-data-473514-h8"
DATASET = "crypto_analytics"
TABLE = "top5_markets"
VS_CURRENCY = "usd"

API_KEY = os.getenv("COINGECKO_API_KEY")
if not API_KEY:
    raise RuntimeError("COINGECKO_API_KEY not set")

BASE_URL = "https://api.coingecko.com/api/v3"
HEADERS = {"x-cg-demo-api-key": API_KEY}

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
    url = f"{BASE_URL}/coins/markets"
    params = {
        "vs_currency": VS_CURRENCY,
        "order": "market_cap_desc",
        "per_page": 5,
        "page": 1,
        "sparkline": "false",
    }
    r = requests.get(url, headers=HEADERS, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    snapshot_time = datetime.now(timezone.utc).isoformat()

    cleaned = []
    for coin in data:
        rec = {"snapshot_time": snapshot_time}
        for f in FIELDS:
            v = coin.get(f)
            if f == "roi" and isinstance(v, dict):
                v = json.dumps(v)
            rec[f] = v
        cleaned.append(rec)
    return cleaned

def main():
    spark = (
        SparkSession.builder
        .appName("gecko_spark_to_bigquery")
        .getOrCreate()
    )

    # must exist and be writable by the service account
    spark.conf.set("temporaryGcsBucket", "spark-bq-staging-eu")

    records = fetch_top5_markets()

    df = spark.read.json(
        spark.sparkContext.parallelize([json.dumps(r) for r in records])
    )

    # example extra column
    df = df.withColumn("load_date", lit(datetime.now().date().isoformat()))

    (
        df.write
          .format("bigquery")
          .mode("append")
          .option("writeMethod", "indirect")
          .save(f"{PROJECT_ID}.{DATASET}.{TABLE}")
    )

    spark.stop()

if __name__ == "__main__":
    main()
