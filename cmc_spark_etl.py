# cmc_spark_etl.py
import os
import sys
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

print("DEBUG: cmc_spark_etl.py starting")
print("DEBUG: sys.executable =", sys.executable)
print("DEBUG: PYSPARK_PYTHON =", os.getenv("PYSPARK_PYTHON"))
print("DEBUG: PYSPARK_DRIVER_PYTHON =", os.getenv("PYSPARK_DRIVER_PYTHON"))

load_dotenv()
print("DEBUG: after load_dotenv, CMC_API_KEY present =", "CMC_API_KEY" in os.environ)

PROJECT_ID = "tokyo-data-473514-h8"
DATASET = "crypto_analytics"
TABLE = "cmc_listings_latest"

CMC_API_KEY = os.getenv("CMC_API_KEY")
if not CMC_API_KEY:
    raise RuntimeError("CMC_API_KEY not set in environment")

print("DEBUG: CMC_API_KEY length =", len(CMC_API_KEY))

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
    print("DEBUG: calling CMC /cryptocurrency/listings/latest")
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
            "date_added": item.get("date_added"),           # keep as string
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
    print("DEBUG: fetched CMC records count =", len(cleaned))
    return cleaned

def main():
    print("DEBUG: creating SparkSession for CMC")
    spark = (
        SparkSession.builder
        .appName("cmc_spark_to_bigquery")
        .master("local[1]")
        .config("spark.python.worker.reuse", "false")
        .getOrCreate()
    )
    print("DEBUG: Spark version =", spark.version)

    # GCS filesystem config (same as Gecko)
    print("DEBUG: setting GCS Hadoop configuration via spark._jsc.hadoopConfiguration()")
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("google.cloud.auth.service.account.enable", "true")
    print("DEBUG: GCS Hadoop configuration set")

    spark.conf.set("temporaryGcsBucket", "spark-bq-staging-eu")
    print("DEBUG: set temporaryGcsBucket to spark-bq-staging-eu")

    records = fetch_listings_latest(limit=200)

    print("DEBUG: parallelizing CMC records, count =", len(records))
    rdd = spark.sparkContext.parallelize([json.dumps(r) for r in records])

    print("DEBUG: reading JSON into CMC DataFrame")
    df = spark.read.json(rdd)

    print("DEBUG: CMC schema after json read:")
    df.printSchema()

    # Cast types to match cmc_listings_latest BigQuery schema
    df = (
        df
        .withColumn("cmc_id",            col("cmc_id").cast("long"))     # INTEGER
        .withColumn("cmc_rank",          col("cmc_rank").cast("long"))   # INTEGER
        .withColumn("circulating_supply", col("circulating_supply").cast("double"))
        .withColumn("total_supply",       col("total_supply").cast("double"))
        .withColumn("max_supply",         col("max_supply").cast("double"))
        .withColumn("num_market_pairs",   col("num_market_pairs").cast("long"))
        .withColumn("price",              col("price").cast("double"))
        .withColumn("volume_24h",         col("volume_24h").cast("double"))
        .withColumn("percent_change_1h",  col("percent_change_1h").cast("double"))
        .withColumn("percent_change_24h", col("percent_change_24h").cast("double"))
        .withColumn("percent_change_7d",  col("percent_change_7d").cast("double"))
        .withColumn("market_cap",         col("market_cap").cast("double"))
        # date_added stays STRING to match BigQuery column type
        .withColumn("snapshot_time", to_timestamp("snapshot_time"))
        .withColumn("last_updated",  to_timestamp("last_updated"))
    )

    print("DEBUG: CMC schema after casts:")
    df.printSchema()

    row_count = df.count()
    print("DEBUG: final CMC dataframe row count =", row_count)

    target_table = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    print("DEBUG: writing CMC to BigQuery table", target_table)

    (
        df.write
          .format("bigquery")
          .mode("append")
          .option("writeMethod", "indirect")
          .save(target_table)
    )

    print("DEBUG: finished CMC BigQuery write, stopping Spark")
    spark.stop()
    print("DEBUG: Spark stopped, exiting CMC script")

if __name__ == "__main__":
    main()
