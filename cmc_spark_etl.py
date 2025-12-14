# cmc_spark_etl.py
import os
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

PROJECT_ID = "tokyo-data-473514-h8"
DATASET = "crypto_analytics"
TABLE = "cmc_listings_latest"

CMC_API_KEY = os.getenv("CMC_API_KEY")
if not CMC_API_KEY:
    raise RuntimeError("CMC_API_KEY not set in .env file")

BASE_URL_CMC = "https://pro-api.coinmarketcap.com/v1"

HEADERS_CMC = {
    "X-CMC_PRO_API_KEY": CMC_API_KEY,
    "Accept": "application/json",
}

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

def main():
    spark = (
        SparkSession.builder
        .appName("cmc_spark_to_bigquery")
        .getOrCreate()
    )

    spark.conf.set("temporaryGcsBucket", "spark-bq-staging-eu")

    records = fetch_listings_latest(limit=200)

    df = spark.read.json(
        spark.sparkContext.parallelize([json.dumps(r) for r in records])
    )

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
