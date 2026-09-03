from flask import Flask
import os
# test
app = Flask(__name__)

@app.route("/")
def hello():
    return "Demo App — version 1"

@app.route("/health")
def health():
    return "OK"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
