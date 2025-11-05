from flask import Flask, request, render_template_string
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

# ---------------------------------------------------------
# FLASK APP CONFIGURATION
# ---------------------------------------------------------
app = Flask(__name__)

# ✅ Update with your real MySQL credentials
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+mysqlconnector://root:yourpassword@localhost/farmers_db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

# ---------------------------------------------------------
# DATABASE MODEL
# ---------------------------------------------------------
class USSDSession(db.Model):
    __tablename__ = 'ussd_sessions'

    id = db.Column(db.Integer, primary_key=True)
    phone_number = db.Column(db.String(20), nullable=False)
    session_id = db.Column(db.String(100), nullable=False)
    user_input = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# ---------------------------------------------------------
# CREATE DATABASE TABLE
# ---------------------------------------------------------
with app.app_context():
    db.create_all()

# ---------------------------------------------------------
# USSD ENDPOINT
# ---------------------------------------------------------
@app.route('/ussd', methods=['POST'])
def ussd():
    data = request.form or request.get_json()

    phone_number = data.get('phoneNumber')
    session_id = data.get('sessionId')
    text = data.get('text', '')

    print("\n📨 Incoming USSD Request:")
    print(data)

    # Store interaction in DB
    new_session = USSDSession(
        phone_number=phone_number,
        session_id=session_id,
        user_input=text
    )
    db.session.add(new_session)
    db.session.commit()

    # -----------------------------------------------------
    # USSD MENU LOGIC
    # -----------------------------------------------------
    if text == "":
        response = "CON Welcome to ZedMarket\n1. View Market Prices\n2. Forecast Demand\n3. Contact Support"
    elif text == "1":
        response = "CON Select crop:\n1. Maize\n2. Soya Beans\n3. Groundnuts"
    elif text == "1*1":
        response = "END Maize price: K85 per 50kg bag"
    elif text == "1*2":
        response = "END Soya Beans price: K140 per 50kg bag"
    elif text == "1*3":
        response = "END Groundnuts price: K170 per 50kg bag"
    elif text == "2":
        response = "END Forecast: Maize demand high in Lusaka next week."
    elif text == "3":
        response = "END Contact Support:\nCall: +260977000111"
    else:
        response = "END Invalid choice. Try again."

    return response, 200, {'Content-Type': 'text/plain'}

# ---------------------------------------------------------
# DASHBOARD PAGE
# ---------------------------------------------------------
@app.route('/')
def dashboard():
    sessions = USSDSession.query.order_by(USSDSession.created_at.desc()).all()
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>ZedMarket Dashboard</title>
        <style>
            body { font-family: Arial, sans-serif; background: #f8f9fa; margin: 40px; }
            h2 { color: #007bff; }
            table { width: 100%; border-collapse: collapse; background: white; }
            th, td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
            tr:hover { background-color: #f1f1f1; }
            th { background-color: #007bff; color: white; }
        </style>
    </head>
    <body>
        <h2>ZedMarket USSD Logs</h2>
        <table>
            <tr>
                <th>ID</th>
                <th>Phone Number</th>
                <th>Session ID</th>
                <th>User Input</th>
                <th>Timestamp</th>
            </tr>
            {% for s in sessions %}
            <tr>
                <td>{{ s.id }}</td>
                <td>{{ s.phone_number }}</td>
                <td>{{ s.session_id }}</td>
                <td>{{ s.user_input }}</td>
                <td>{{ s.created_at.strftime('%Y-%m-%d %H:%M:%S') }}</td>
            </tr>
            {% endfor %}
        </table>
    </body>
    </html>
    """
    return render_template_string(html, sessions=sessions)

# ---------------------------------------------------------
# MAIN ENTRY POINT
# ---------------------------------------------------------
if __name__ == '__main__':
    print("🚀 USSD app + Dashboard running on http://127.0.0.1:5001")
    app.run(host='0.0.0.0', port=5001, debug=True)
