# app.py
import subprocess
from flask import Flask, jsonify
from dotenv import load_dotenv

# Load .env once at startup
load_dotenv()

app = Flask(__name__)

@app.route("/gecko", methods=["GET"])
def run_gecko_spark():
    result = subprocess.run(
        ["spark-submit", "/app/gecko_spark_etl.py"],
        capture_output=True,
        text=True,  # same as universal_newlines=True
    )
    return jsonify(
        {
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    ), (200 if result.returncode == 0 else 500)

@app.route("/cmc", methods=["GET"])
def run_cmc_spark():
    result = subprocess.run(
        ["spark-submit", "/app/cmc_spark_etl.py"],
        capture_output=True,
        text=True,
    )
    return jsonify(
        {
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    ), (200 if result.returncode == 0 else 500)

@app.route("/", methods=["GET"])
def health():
    return {"status": "ok", "services": ["gecko", "cmc"]}, 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
