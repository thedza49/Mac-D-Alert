from flask import Flask, jsonify
import subprocess
import os
from pathlib import Path

app = Flask(__name__)
BASE_DIR = Path(__file__).resolve().parent.parent

@app.route("/run/collector", methods=["POST"])
def run_collector():
    # Run fetch_prices and fetch_earnings
    try:
        subprocess.run(["python3", "scripts/fetch_prices.py"], cwd=BASE_DIR, check=True)
        subprocess.run(["python3", "scripts/fetch_earnings.py"], cwd=BASE_DIR, check=True)
        return jsonify({"status": "success", "message": "Collector finished"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/run/engine", methods=["POST"])
def run_engine():
    # Run calculate_macd and signal_detector
    try:
        subprocess.run(["python3", "scripts/calculate_macd.py"], cwd=BASE_DIR, check=True)
        subprocess.run(["python3", "scripts/signal_detector.py"], cwd=BASE_DIR, check=True)
        return jsonify({"status": "success", "message": "Engine finished"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
