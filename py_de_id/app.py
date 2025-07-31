import os
import json
from flask import Flask, jsonify, request, send_file
from pathlib import Path

# this file's parent directory
PROJECT_DIR = (
    Path(Path(__file__).parent.resolve().absolute()).parent.resolve().absolute()
)


def create_app():
    app = Flask(__name__)

    @app.route("/hello-world", methods=["GET"])
    def hello_world():
        return "hello world.", 200

    @app.route("/health", methods=["GET"])
    def health():
        # Simple health check logic; always healthy for now
        healthy = True
        return ("", 200) if healthy else ("", 400)

    @app.route("/deidentify/<id>", methods=["POST"])
    def deidentify(id):
        # Stub implementation
        data = request.get_json()
        # You can log or inspect 'data' here if needed
        if not "transaction_id" in data:
            print("Missing transaction id")
            return jsonify({"message": "missing transaction_id"}), 400

        print(f"Create /tmp/${data['transaction_id']}.json")
        filepath = f"/tmp/{data['transaction_id']}.json"
        try:
            with open(filepath, "w+", encoding="utf-8") as f:
                json_str = json.dumps(data)
                f.write(json_str)
            print(f"wrote file {data['transaction_id']}.json")
        except Exception as e:
            print("Failed to save the object to disk.")

        return jsonify({"message": "OK"}), 200

    @app.route("/favicon.ico", methods=["GET"])
    def favicon():
        return send_file(f"{PROJECT_DIR}/assets/favicon.ico")

    return app
