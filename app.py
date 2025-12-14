# app.py
import subprocess
from flask import Flask, jsonify
from dotenv import load_dotenv

# Load variables from .env into os.environ
load_dotenv()

app = Flask(__name__)

@app.route("/gecko", methods=["GET"])
def run_gecko_spark():
    result = subprocess.run(
        ["spark-submit", "/app/gecko_spark_etl.py"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return {"status": "error", "stderr": result.stderr}, 500
    return {"status": "ok", "stdout": result.stdout}, 200

@app.route("/cmc", methods=["GET"])
def run_cmc_spark():
    result = subprocess.run(
        ["spark-submit", "/app/cmc_spark_etl.py"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return {"status": "error", "stderr": result.stderr}, 500
    return {"status": "ok", "stdout": result.stdout}, 200

@app.route("/", methods=["GET"])
def health():
    return {"status": "ok", "services": ["gecko_spark", "cmc_spark"]}, 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
