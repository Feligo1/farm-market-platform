from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route('/api/test')
def test():
    return jsonify({"status": "ok", "message": "Server is running!"})

@app.route('/')
def home():
    return "Farm Market Platform - Backend is running!"

if __name__ == '__main__':
    print("🚀 Server starting on http://127.0.0.1:5000")
    app.run(debug=True, port=5000)