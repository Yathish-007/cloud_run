# app.py
from flask import Flask, jsonify

from gecko_loader import main as gecko_main
from cmc_loader import main as cmc_main

app = Flask(__name__)


@app.route("/gecko", methods=["GET"])
def trigger_gecko():
    """Fetch from CoinGecko and load into BigQuery."""
    rows = gecko_main()
    # don't return huge payload; cap it to first few rows
    return jsonify(rows), 200


@app.route("/cmc", methods=["GET"])
def trigger_cmc():
    """Fetch from CoinMarketCap and load into BigQuery."""
    rows = cmc_main(limit=200)
    return jsonify(rows[:20]), 200


@app.route("/", methods=["GET"])
def health():
    return {"status": "ok", "services": ["gecko", "cmc"]}, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
