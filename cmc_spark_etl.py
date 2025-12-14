# cmc_spark_etl.py
import os
import sys
import json
import requests
from datetime import datetime, timezone

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

API_KEY = os.getenv("CMC_API_KEY")
if not API_KEY:
    raise RuntimeError("CMC_API_KEY not set in environment")
print("DEBUG: CMC_API_KEY length =", len(API_KEY))

BASE_URL = "https://pro-api.coinmarketcap.com/v1"
HEADERS = {"X-CMC_PRO_API_KEY": API_KEY}

# Fields you want from CoinMarketCap response
FIELDS = [
    "id",
    "name",
    "symbol",
    "slug",
    "cmc_rank",
    "circulating_supply",
    "total_supply",
    "max_supply",
    "num_market_pairs",
    "date_added",
    "tags",
    "last_updated",
    # price, volume, percent_change_*, market_cap are nested in quote.USD
]

def fetch_cmc_listings():
    print("DEBUG: calling CMC /cryptocurrency/listings/latest")
    url = f"{BASE_URL}/cryptocurrency/listings/latest"
    params = {
        "start": "1",
        "limit": "200",
        "convert": "USD",
        "aux": "num_market_pairs,cmc_rank,date_added,tags,max_supply"  # include extra fields
    }
    r = requests.get(url, headers=HEADERS, params=params, timeout=20)
    print("DEBUG: CMC status", r.status_code)
    print("DEBUG: CMC body snippet:", r.text[:200])
    r.raise_for_status()
    payload = r.json()
    data = payload.get("data", [])

    snapshot_time = datetime.now(timezone.utc).isoformat()

    cleaned = []
    for coin in data:
        rec = {
            "snapshot_time": snapshot_time,
            "cmc_id": coin.get("id"),
            "name": coin.get("name"),
            "symbol": coin.get("symbol"),
            "slug": coin.get("slug"),
            "cmc_rank": coin.get("cmc_rank"),
            "circulating_supply": coin.get("circulating_supply"),
            "total_supply": coin.get("total_supply"),
            "max_supply": coin.get("max_supply"),
            "num_market_pairs": coin.get("num_market_pairs"),
            "date_added": coin.get("date_added"),
            "tags": ",".join(coin.get("tags") or []),
            "last_updated": coin.get("last_updated"),
        }

        quote_usd = (coin.get("quote") or {}).get("USD") or {}
        rec["price"] = quote_usd.get("price")
        rec["volume_24h"] = quote_usd.get("volume_24h")
        rec["percent_change_1h"] = quote_usd.get("percent_change_1h")
        rec["percent_change_24h"] = quote_usd.get("percent_change_24h")
        rec["percent_change_7d"] = quote_usd.get("percent_change_7d")
        rec["market_cap"] = quote_usd.get("market_cap")

        cleaned.append(rec)

    print("DEBUG: fetched CMC records count =", len(cleaned))
    return cleaned

def main():
    print("DEBUG: creating SparkSession for CMC")
    spark = (
        SparkSession.builder
        .appName("cmc_spark_to_bigquery")
        .master("local[1]")  # single local worker
        .config("spark.python.worker.reuse", "false")
        .config("spark.python.use.daemon", "false")  # avoid daemon socket issues
        .getOrCreate()
    )
    print("DEBUG: Spark version =", spark.version)

    # GCS filesystem config
    print("DEBUG: setting GCS Hadoop configuration via spark._jsc.hadoopConfiguration()")
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("google.cloud.auth.service.account.enable", "true")
    print("DEBUG: GCS Hadoop configuration set")

    spark.conf.set("temporaryGcsBucket", "spark-bq-staging-eu")
    print("DEBUG: set temporaryGcsBucket to spark-bq-staging-eu")

    print("DEBUG: fetching records from CMC")
    records = fetch_cmc_listings()

    print("DEBUG: parallelizing CMC records, count =", len(records))
    rdd = spark.sparkContext.parallelize([json.dumps(r) for r in records])

    print("DEBUG: reading JSON into CMC DataFrame")
    df = spark.read.json(rdd)

    print("DEBUG: schema after json read:")
    df.printSchema()

    # Cast numeric fields to match BigQuery schema
    df = (
        df
        .withColumn("cmc_id", col("cmc_id").cast("long"))
        .withColumn("cmc_rank", col("cmc_rank").cast("long"))
        .withColumn("circulating_supply", col("circulating_supply").cast("double"))
        .withColumn("total_supply", col("total_supply").cast("double"))
        .withColumn("max_supply", col("max_supply").cast("double"))
        .withColumn("num_market_pairs", col("num_market_pairs").cast("long"))
        .withColumn("price", col("price").cast("double"))
        .withColumn("volume_24h", col("volume_24h").cast("double"))
        .withColumn("percent_change_1h", col("percent_change_1h").cast("double"))
        .withColumn("percent_change_24h", col("percent_change_24h").cast("double"))
        .withColumn("percent_change_7d", col("percent_change_7d").cast("double"))
        .withColumn("market_cap", col("market_cap").cast("double"))
        # snapshot_time is TIMESTAMP in BigQuery; date_added and last_updated are STRING
        .withColumn("snapshot_time", to_timestamp("snapshot_time"))
    )

    print("DEBUG: schema after casts:")
    df.printSchema()

    row_count = df.count()
    print("DEBUG: final dataframe row count =", row_count)

    target_table = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    print("DEBUG: writing to BigQuery table", target_table)

    (
        df.write
        .format("bigquery")
        .mode("append")
        .option("writeMethod", "indirect")  # uses temporaryGcsBucket
        .save(target_table)
    )

    print("DEBUG: finished BigQuery write, stopping Spark")
    spark.stop()
    print("DEBUG: Spark stopped, exiting script")

if __name__ == "__main__":
    main()
