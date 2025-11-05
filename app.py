from flask import Flask, jsonify, request, send_from_directory, make_response
from flask_cors import CORS
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, Price
from forecast import simple_price_forecast
import pandas as pd
import os
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
import requests
from twilio.rest import Client
from io import BytesIO

# ---------------------------------
# Flask Setup
# ---------------------------------
app = Flask(__name__, static_folder="../frontend", static_url_path="/")
CORS(app)

# ---------------------------------
# Database Configuration
# ---------------------------------
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "farm_market")

DATABASE_URL = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    engine = create_engine(DATABASE_URL, echo=False, pool_recycle=3600)
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    print("✅ Connected to MySQL successfully.")
except Exception as e:
    print(f"❌ Database connection failed: {e}")
    raise e

# ---------------------------------
# Dummy Accounts
# ---------------------------------
USERS = {
    "admin": {"password": "admin123", "role": "admin"},
    "farmer": {"password": "farmer123", "role": "farmer"}
}

# ---------------------------------
# ROUTES
# ---------------------------------

@app.route("/api/login", methods=["POST"])
def login():
    data = request.json
    username, password = data.get("username"), data.get("password")
    user = USERS.get(username)
    if user and user["password"] == password:
        return jsonify({"status": "ok", "role": user["role"], "username": username})
    return jsonify({"status": "error", "message": "Invalid credentials"}), 401


@app.route("/api/prices", methods=["GET"])
def get_prices():
    session = Session()
    rows = session.query(Price).order_by(Price.recorded_at.desc()).limit(100).all()
    session.close()
    return jsonify({
        "status": "ok",
        "data": [{
            "id": r.id, "market": r.market, "commodity": r.commodity,
            "price": float(r.price), "volume": r.volume,
            "recorded_at": r.recorded_at.isoformat()
        } for r in rows]
    })


@app.route("/api/add-price", methods=["POST"])
def add_price():
    data = request.json
    session = Session()
    new_entry = Price(
        market=data.get("market"),
        commodity=data.get("commodity"),
        price=data.get("price"),
        volume=data.get("volume") or 0,
        recorded_at=pd.Timestamp.now().date()
    )
    session.add(new_entry)
    session.commit()
    session.close()
    return jsonify({"status": "ok", "message": "Record added successfully"})


@app.route("/api/commodity-timeseries")
def get_timeseries():
    commodity = request.args.get("commodity", "Maize")
    market = request.args.get("market")
    session = Session()
    q = session.query(Price).filter(Price.commodity == commodity)
    if market:
        q = q.filter(Price.market == market)
    q = q.order_by(Price.recorded_at.asc())
    rows = q.all()
    session.close()
    return jsonify({"status": "ok", "data": [
        {"date": r.recorded_at.isoformat(), "price": float(r.price), "market": r.market}
        for r in rows
    ]})


@app.route("/api/forecast")
def forecast():
    commodity = request.args.get("commodity", "Maize")
    market = request.args.get("market")
    days = int(request.args.get("days", 3))
    session = Session()
    q = session.query(Price).filter(Price.commodity == commodity)
    if market:
        q = q.filter(Price.market == market)
    rows = q.order_by(Price.recorded_at.asc()).all()
    session.close()

    if not rows:
        return jsonify({"status": "error", "message": "No data found"}), 404

    df = pd.DataFrame([{"recorded_at": r.recorded_at, "price": float(r.price)} for r in rows])
    preds = simple_price_forecast(df, days)
    return jsonify({"status": "ok", "commodity": commodity, "market": market, "predictions": preds})


@app.route("/api/market-comparison")
def market_comparison():
    commodity = request.args.get("commodity", "Maize")
    session = Session()
    markets = ["Lusaka Central", "Kabwe Main", "Ndola Market"]
    data = {}
    for m in markets:
        q = session.query(Price).filter(Price.market == m, Price.commodity == commodity).order_by(Price.recorded_at.asc()).all()
        data[m] = [{"date": r.recorded_at.isoformat(), "price": float(r.price)} for r in q]
    session.close()
    return jsonify({"status": "ok", "data": data})


@app.route("/api/export-excel")
def export_excel():
    session = Session()
    rows = session.query(Price).order_by(Price.recorded_at.desc()).limit(100).all()
    session.close()

    df = pd.DataFrame([{
        "Date": r.recorded_at, "Market": r.market, "Commodity": r.commodity,
        "Price (ZMW)": r.price, "Volume": r.volume
    } for r in rows])

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="MarketData")
    output.seek(0)

    response = make_response(output.read())
    response.headers["Content-Disposition"] = "attachment; filename=market_data.xlsx"
    response.headers["Content-Type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return response


@app.route("/api/report")
def report():
    session = Session()
    rows = session.query(Price).order_by(Price.recorded_at.desc()).limit(50).all()
    session.close()
    data = [[r.recorded_at, r.market, r.commodity, float(r.price)] for r in rows]

    doc = SimpleDocTemplate("market_report.pdf", pagesize=A4)
    styles = getSampleStyleSheet()
    story = [Paragraph("Farm Market Analytics Report", styles["Title"]), Spacer(1, 12)]
    table = Table([["Date", "Market", "Commodity", "Price (ZMW)"]] + data, repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.green),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
    ]))
    story.append(table)
    doc.build(story)
    return send_from_directory(".", "market_report.pdf")


@app.route("/api/weather")
def weather():
    city = request.args.get("city", "Lusaka")
    api_key = os.getenv("OPENWEATHER_KEY", "demo_key")
    try:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city},ZM&appid={api_key}&units=metric"
        res = requests.get(url).json()
        data = {
            "city": city,
            "temperature": res["main"]["temp"],
            "humidity": res["main"]["humidity"],
            "description": res["weather"][0]["description"]
        }
        return jsonify({"status": "ok", "data": data})
    except Exception:
        return jsonify({"status": "error", "message": "Weather data unavailable"})


@app.route("/api/send-sms", methods=["POST"])
def send_sms():
    data = request.json
    to = data.get("to")
    message = data.get("message")
    sid = os.getenv("TWILIO_SID")
    token = os.getenv("TWILIO_TOKEN")
    sender = os.getenv("TWILIO_PHONE")
    if not all([sid, token, sender]):
        return jsonify({"status": "error", "message": "Twilio not configured"}), 400
    try:
        client = Client(sid, token)
        client.messages.create(to=to, from_=sender, body=message)
        return jsonify({"status": "ok", "message": "SMS sent"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@app.route("/")
def frontend():
    return send_from_directory(app.static_folder, "index.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
