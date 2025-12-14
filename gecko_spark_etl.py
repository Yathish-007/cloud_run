# gecko_spark_etl.py
import os
import json
import requests
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

print("DEBUG: gecko_spark_etl.py starting")

load_dotenv()
print("DEBUG: after load_dotenv, COINGECKO_API_KEY present =", "COINGECKO_API_KEY" in os.environ)

PROJECT_ID = "tokyo-data-473514-h8"
DATASET = "crypto_analytics"
TABLE = "top5_markets"
VS_CURRENCY = "usd"

API_KEY = os.getenv("COINGECKO_API_KEY")
if not API_KEY:
    raise RuntimeError("COINGECKO_API_KEY not set in environment")
print("DEBUG: COINGECKO_API_KEY length =", len(API_KEY))

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
    print("DEBUG: calling CoinGecko /coins/markets")
    url = f"{BASE_URL}/coins/markets"
    params = {
        "vs_currency": VS_CURRENCY,
        "order": "market_cap_desc",
        "per_page": 5,
        "page": 1,
        "sparkline": "false",
    }
    r = requests.get(url, headers=HEADERS, params=params, timeout=15)
    print("DEBUG: CoinGecko status", r.status_code)
    print("DEBUG: CoinGecko body snippet:", r.text[:200])
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
    print("DEBUG: fetched records count =", len(cleaned))
    return cleaned

def main():
    print("DEBUG: creating SparkSession")
    spark = (
        SparkSession.builder
        .appName("gecko_spark_to_bigquery")
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

    print("DEBUG: fetching records from CoinGecko")
    records = fetch_top5_markets()

    print("DEBUG: parallelizing records, count =", len(records))
    rdd = spark.sparkContext.parallelize([json.dumps(r) for r in records])

    print("DEBUG: reading JSON into DataFrame")
    df = spark.read.json(rdd)

    print("DEBUG: schema after json read:")
    df.printSchema()

    # Cast types to match BigQuery schema
    df = (
        df
        .withColumn("current_price",  col("current_price").cast("double"))   # FLOAT
        .withColumn("market_cap",     col("market_cap").cast("long"))        # INTEGER
        .withColumn("market_cap_rank", col("market_cap_rank").cast("long"))  # INTEGER
        .withColumn("fully_diluted_valuation", col("fully_diluted_valuation").cast("long"))
        .withColumn("total_volume",   col("total_volume").cast("long"))
        .withColumn("high_24h",       col("high_24h").cast("double"))
        .withColumn("low_24h",        col("low_24h").cast("double"))
        .withColumn("price_change_24h",           col("price_change_24h").cast("double"))
        .withColumn("price_change_percentage_24h", col("price_change_percentage_24h").cast("double"))
        .withColumn("market_cap_change_24h",           col("market_cap_change_24h").cast("double"))
        .withColumn("market_cap_change_percentage_24h", col("market_cap_change_percentage_24h").cast("double"))
        .withColumn("circulating_supply", col("circulating_supply").cast("double"))
        .withColumn("total_supply",       col("total_supply").cast("double"))
        .withColumn("max_supply",         col("max_supply").cast("double"))
        .withColumn("ath",                col("ath").cast("double"))
        .withColumn("ath_change_percentage", col("ath_change_percentage").cast("double"))
        .withColumn("atl",                col("atl").cast("double"))
        .withColumn("atl_change_percentage", col("atl_change_percentage").cast("double"))
        # Timestamps: parse ISO strings into TIMESTAMP
        .withColumn("snapshot_time", to_timestamp("snapshot_time"))
        .withColumn("ath_date",      to_timestamp("ath_date"))
        .withColumn("atl_date",      to_timestamp("atl_date"))
        .withColumn("last_updated",  to_timestamp("last_updated"))
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
          .option("writeMethod", "indirect")
          .save(target_table)
    )

    print("DEBUG: finished BigQuery write, stopping Spark")
    spark.stop()
    print("DEBUG: Spark stopped, exiting script")

if __name__ == "__main__":
    main()
