# =========================================================
# app.py - COMPLETE ENHANCED VERSION WITH REAL ZAMBIAN DATA
# Cloud-Based Market Information Platform for Farmers
# Mulungushi University – ICT 431 Capstone Project
# Student: Daka Felix (202206453)
# Course: ICT 431 - Capstone Project
# =========================================================

import os
import sys
import time
import json
import random
import hashlib
import pickle
import schedule
from data_scheduler import DataScheduler
import threading
import subprocess
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union

# Core Flask imports
from flask import Flask, request, jsonify, send_from_directory, send_file, render_template_string
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash

# Database
import sqlite3
import uuid

# Authentication
import jwt
from functools import wraps

# Data Processing
import pandas as pd
import numpy as np

# Machine Learning (for forecasting)
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
import joblib

# SMS Integration (Twilio for demo)
try:
    from twilio.rest import Client
    TWILIO_AVAILABLE = True
except ImportError:
    TWILIO_AVAILABLE = False
    print("⚠️  Twilio not available. SMS features disabled.")

# USSD Integration (Africa's Talking)
try:
    import africastalking
    AFRICASTALKING_AVAILABLE = True
except ImportError:
    AFRICASTALKING_AVAILABLE = False
    print("⚠️  Africa's Talking not available. USSD features disabled.")

# Backup Management
try:
    import boto3
    import zipfile
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False
    print("⚠️  AWS not available. Backup features disabled.")

# Zambian Data Import
from zambian_data import ZambianMarketData
print("✅ ZambianMarketData imported successfully")

# =========================================================
# MISSING FORECAST FUNCTIONS (FALLBACK IMPLEMENTATIONS)
# =========================================================

def get_forecast_recommendations(commodity, market):
    """Fallback forecast recommendations function"""
    recommendations = {
        "buy_sell": "Hold" if random.random() > 0.5 else "Sell",
        "timing": "Within 3 days",
        "reason": "Price stability expected",
        "confidence": "medium"
    }
    
    # Add commodity-specific recommendations
    if commodity == "Maize":
        recommendations.update({
            "action": "Consider storing if you have storage facilities",
            "market_advice": f"Check {market} prices daily"
        })
    elif commodity == "Tomatoes":
        recommendations.update({
            "action": "Sell quickly to avoid spoilage",
            "market_advice": "Prices vary greatly by day"
        })
    
    return recommendations

def analyze_market_forecasts(results):
    """Fallback market analysis function"""
    return {
        "best_market": "Lusaka",
        "worst_market": "Unknown",
        "price_range": "Varies",
        "recommendation": "Check multiple markets"
    }

def get_market_forecast(commodity, market, days):
    """Fallback market forecast function"""
    base_price = 120.50 if commodity == "Maize" else 100.00
    
    forecast = []
    for i in range(1, days + 1):
        date = (datetime.now() + timedelta(days=i)).strftime("%Y-%m-%d")
        variation = random.uniform(-0.02, 0.03) * (i/7)
        predicted_price = round(base_price * (1 + variation), 2)
        
        forecast.append({
            "date": date,
            "predicted_price": predicted_price,
            "change_percent": round(variation * 100, 2),
            "model": "simple_fallback",
            "confidence": "low"
        })
    
    return forecast

def get_all_markets_forecast(commodity, days):
    """Fallback all markets forecast function"""
    markets = ["Lusaka", "Kabwe", "Ndola", "Livingstone"]
    results = {}
    
    for market in markets:
        results[market] = get_market_forecast(commodity, market, days)
    
    return results

# =========================================================
# ENHANCED FORECAST MODULE IMPORT (WITH FALLBACK)
# =========================================================

try:
    # Import the enhanced forecast module
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from forecast import (
        enhanced_price_forecast,
        get_market_forecast,
        get_all_markets_forecast,
        get_forecast_recommendations,
        analyze_market_forecasts,
        ForecastConfig,
        model_manager
    )
    FORECAST_AVAILABLE = True
    print("✅ Enhanced forecast module loaded")
except ImportError as e:
    FORECAST_AVAILABLE = False
    print(f"⚠️  Enhanced forecast module not available: {e}")
    
    # Fallback ForecastConfig class
    class ForecastConfig:
        def __init__(self):
            self.model_type = "auto"
            self.confidence_threshold = 0.7
    
    # Fallback ModelManager class
    class ModelManager:
        def __init__(self):
            self.models = {}
        
        def get_model(self, commodity, market):
            return "simple_linear"
    
    model_manager = ModelManager()
    
    # Fallback enhanced_price_forecast function
    def enhanced_price_forecast(df, days=7, commodity="Maize", market="Lusaka", model_type="auto"):
        """Fallback enhanced price forecast function"""
        if len(df) < 2:
            current_price = df['price'].iloc[-1] if len(df) > 0 else 100
            return generate_simple_forecast(current_price, days)
        
        predictions = []
        current_price = df['price'].iloc[-1]
        
        for i in range(1, days + 1):
            date = (datetime.now() + timedelta(days=i)).strftime("%Y-%m-%d")
            variation = random.uniform(-0.02, 0.03) * (i/7)
            predicted_price = round(current_price * (1 + variation), 2)
            
            predictions.append({
                "date": date,
                "predicted_price": predicted_price,
                "change_percent": round(variation * 100, 2),
                "trend": "up" if variation > 0 else "down" if variation < 0 else "stable",
                "model": "fallback",
                "confidence": "low"
            })
        
        return predictions

def generate_simple_forecast(current_price, days):
    """Generate simple forecast"""
    predictions = []
    for i in range(1, days + 1):
        date = (datetime.now() + timedelta(days=i)).strftime("%Y-%m-%d")
        variation = random.uniform(-0.02, 0.03) * (i/7)
        predicted_price = round(current_price * (1 + variation), 2)
        
        predictions.append({
            "date": date,
            "predicted_price": predicted_price,
            "change_percent": round(variation * 100, 2),
            "trend": "up" if variation > 0 else "down" if variation < 0 else "stable",
            "model": "simple",
            "confidence": "low"
        })
    
    return predictions

# =========================================================
# SMS & USSD SERVICES
# =========================================================

class SMSService:
    """SMS Service for price alerts and notifications"""
    
    def __init__(self):
        self.sms_logs = []
        
        # Twilio configuration (for demo)
        self.twilio_account_sid = os.getenv('TWILIO_ACCOUNT_SID', 'demo_sid')
        self.twilio_auth_token = os.getenv('TWILIO_AUTH_TOKEN', 'demo_token')
        self.twilio_phone = os.getenv('TWILIO_PHONE', '+15005550006')
        
        if TWILIO_AVAILABLE and self.twilio_account_sid != 'demo_sid':
            self.client = Client(self.twilio_account_sid, self.twilio_auth_token)
            self.active = True
        else:
            self.client = None
            self.active = False
            print("⚠️  SMS Service: Running in demo mode")
    
    def send_sms(self, to_phone, message):
        """Send SMS to phone number"""
        try:
            # Log SMS
            sms_log = {
                "to": to_phone,
                "message": message[:160],  # Truncate to 160 chars
                "timestamp": datetime.now().isoformat(),
                "status": "pending"
            }
            
            # Demo mode or real sending
            if self.active and self.client:
                # Real Twilio sending
                message_obj = self.client.messages.create(
                    body=message,
                    from_=self.twilio_phone,
                    to=to_phone
                )
                sms_log["status"] = "sent"
                sms_log["message_id"] = message_obj.sid
                print(f"📱 SMS sent to {to_phone}: {message[:50]}...")
            else:
                # Demo mode
                sms_log["status"] = "demo_sent"
                print(f"📱 [DEMO] SMS would be sent to {to_phone}: {message[:50]}...")
            
            # Save to database
            self.log_sms_to_db(sms_log)
            
            return {"success": True, "status": sms_log["status"]}
            
        except Exception as e:
            print(f"❌ SMS send error: {e}")
            sms_log["status"] = f"error: {str(e)[:50]}"
            self.log_sms_to_db(sms_log)
            return {"success": False, "error": str(e)}
    
    def send_price_alert(self, user_id, commodity, market, price):
        """Send price alert SMS"""
        conn = get_db()
        cur = conn.cursor()
        
        # Get user phone
        cur.execute("SELECT phone, name FROM users WHERE user_id=?", (user_id,))
        user = cur.fetchone()
        
        if not user or not user["phone"]:
            conn.close()
            return {"success": False, "error": "User phone not found"}
        
        phone = user["phone"]
        name = user["name"] or "Farmer"
        
        # Create message
        message = f"FarmConnect Alert {name}: {commodity} price at {market} is ZMW {price}/kg. "
        message += "Reply STOP to unsubscribe. Dial *123# for more info."
        
        conn.close()
        
        return self.send_sms(phone, message)
    
    def send_daily_summary(self, user_id):
        """Send daily market summary SMS"""
        conn = get_db()
        cur = conn.cursor()
        
        # Get user info
        cur.execute("SELECT phone, name, location, main_crops FROM users WHERE user_id=?", (user_id,))
        user = cur.fetchone()
        
        if not user or not user["phone"]:
            conn.close()
            return {"success": False, "error": "User phone not found"}
        
        phone = user["phone"]
        name = user["name"] or "Farmer"
        location = user["location"] or "your area"
        crops = user["main_crops"] or "Maize,Beans"
        
        # Get prices for user's crops
        crop_list = [crop.strip() for crop in crops.split(',')][:3]
        
        message = f"FarmConnect Daily Summary for {name}:\n"
        
        for crop in crop_list:
            cur.execute('''
                SELECT price, market FROM market_prices 
                WHERE commodity=? AND verified=1 
                ORDER BY recorded_at DESC LIMIT 1
            ''', (crop,))
            price_data = cur.fetchone()
            
            if price_data:
                message += f"{crop}: ZMW {price_data['price']} at {price_data['market']}\n"
            else:
                message += f"{crop}: No data available\n"
        
        message += f"\nMarket in {location} active today. Dial *123# for live prices."
        
        conn.close()
        
        return self.send_sms(phone, message)
    
    def log_sms_to_db(self, sms_log):
        """Log SMS to database"""
        try:
            conn = get_db()
            cur = conn.cursor()
            
            cur.execute('''
                INSERT INTO sms_history (phone, message, type, status, sent_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                sms_log.get("to"),
                sms_log.get("message"),
                "price_alert" if "alert" in sms_log.get("message", "").lower() else "notification",
                sms_log.get("status"),
                sms_log.get("timestamp")
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"Error logging SMS: {e}")

class USSDService:
    """USSD Service for feature phone access"""
    
    def __init__(self):
        self.sessions = {}  # Store USSD session data
        
        # Africa's Talking configuration
        self.username = os.getenv('AFRICASTALKING_USERNAME', 'sandbox')
        self.api_key = os.getenv('AFRICASTALKING_API_KEY', 'demo_key')
        
        if AFRICASTALKING_AVAILABLE and self.api_key != 'demo_key':
            africastalking.initialize(self.username, self.api_key)
            self.ussd = africastalking.USSD
            self.active = True
        else:
            self.ussd = None
            self.active = False
            print("⚠️  USSD Service: Running in demo mode")
    
    def handle_ussd_request(self, session_id, phone_number, text):
        """Handle USSD request and return response"""
        try:
            # Initialize or get session
            if session_id not in self.sessions:
                self.sessions[session_id] = {
                    "phone": phone_number,
                    "state": "initial",
                    "data": {},
                    "created": datetime.now()
                }
            
            session = self.sessions[session_id]
            response = ""
            
            # USSD menu logic
            if text == "":
                # Initial menu
                response = "CON Welcome to FarmConnect Zambia\n"
                response += "1. Check Market Prices\n"
                response += "2. Price Forecast\n"
                response += "3. Find Buyers\n"
                response += "4. Weather Info\n"
                response += "5. Farming Tips\n"
                session["state"] = "main_menu"
                
            elif text == "1":
                # Price check menu
                response = "CON Select Commodity:\n"
                response += "1. Maize\n"
                response += "2. Tomatoes\n"
                response += "3. Beans\n"
                response += "4. Groundnuts\n"
                response += "5. Rice\n"
                response += "6. All Commodities\n"
                session["state"] = "price_menu"
                session["data"]["menu"] = "prices"
                
            elif text.startswith("1*"):
                # Handle price selection
                parts = text.split('*')
                if len(parts) == 2:
                    option = parts[1]
                    
                    if option == "1":
                        commodity = "Maize"
                    elif option == "2":
                        commodity = "Tomatoes"
                    elif option == "3":
                        commodity = "Beans"
                    elif option == "4":
                        commodity = "Groundnuts"
                    elif option == "5":
                        commodity = "Rice"
                    elif option == "6":
                        # Get all commodity prices
                        prices = self.get_all_prices()
                        response = "END Latest Prices (ZMW/kg):\n"
                        for price_info in prices[:5]:  # Limit to 5
                            response += f"{price_info['commodity']}: {price_info['price']}\n"
                        response += "Dial *123# for more"
                        return response
                    else:
                        response = "END Invalid option. Dial *123# to restart."
                        return response
                    
                    # Get price for selected commodity
                    price_data = self.get_commodity_price(commodity)
                    
                    if price_data:
                        response = f"END {commodity} Prices:\n"
                        response += f"Lusaka: ZMW {price_data.get('lusaka', 'N/A')}/kg\n"
                        response += f"Kabwe: ZMW {price_data.get('kabwe', 'N/A')}/kg\n"
                        response += f"Trend: {price_data.get('trend', 'stable')}\n"
                        response += "SMS PRICE to 45678 for alerts"
                    else:
                        response = f"END No data for {commodity}. Try later."
                    
                else:
                    response = "END Invalid input. Dial *123# to restart."
                    
            elif text == "2":
                # Price forecast
                response = "CON Forecast Period:\n"
                response += "1. Next 3 days\n"
                response += "2. Next 7 days\n"
                response += "3. Next 30 days\n"
                session["state"] = "forecast_menu"
                
            elif text.startswith("2*"):
                # Handle forecast selection
                parts = text.split('*')
                if len(parts) == 2:
                    option = parts[1]
                    
                    if option == "1":
                        days = 3
                    elif option == "2":
                        days = 7
                    elif option == "3":
                        days = 30
                    else:
                        response = "END Invalid option. Dial *123# to restart."
                        return response
                    
                    # Get forecast
                    forecast = self.get_maize_forecast(days)
                    
                    response = f"END Maize Forecast ({days} days):\n"
                    for day in forecast[:3]:  # Show first 3 days
                        response += f"{day['date']}: ZMW {day['price']} ({day['trend']})\n"
                    response += "Web: farmconnect.local"
                    
                else:
                    response = "END Invalid input. Dial *123# to restart."
                    
            elif text == "3":
                # Find buyers
                response = "CON Select Commodity:\n"
                response += "1. Maize buyers\n"
                response += "2. Tomato buyers\n"
                response += "3. Bean buyers\n"
                session["state"] = "buyer_menu"
                
            elif text.startswith("3*"):
                # Handle buyer selection
                parts = text.split('*')
                if len(parts) == 2:
                    option = parts[1]
                    
                    if option == "1":
                        buyers = self.get_buyers("Maize")
                    elif option == "2":
                        buyers = self.get_buyers("Tomatoes")
                    elif option == "3":
                        buyers = self.get_buyers("Beans")
                    else:
                        response = "END Invalid option. Dial *123# to restart."
                        return response
                    
                    response = "END Top Buyers:\n"
                    for buyer in buyers[:3]:  # Limit to 3 buyers
                        response += f"{buyer['name']}: {buyer['phone']}\n"
                    response += "Call for best prices!"
                    
                else:
                    response = "END Invalid input. Dial *123# to restart."
                    
            elif text == "4":
                # Weather info
                weather = self.get_weather_info()
                response = f"END Weather Forecast:\n{weather}\nSMS WEATHER for updates"
                
            elif text == "5":
                # Farming tips
                tip = self.get_farming_tip()
                response = f"END Farming Tip:\n{tip}\nMore: farmconnect.local/tips"
                
            else:
                response = "END Invalid option. Dial *123# to restart."
            
            # Clean up old sessions
            self.cleanup_sessions()
            
            return response
            
        except Exception as e:
            print(f"USSD handler error: {e}")
            return "END Service temporarily unavailable. Try again later."
    
    def get_commodity_price(self, commodity):
        """Get current price for commodity"""
        try:
            conn = get_db()
            cur = conn.cursor()
            
            # Get latest prices for different markets
            cur.execute('''
                SELECT market, price FROM market_prices 
                WHERE commodity=? AND verified=1 
                ORDER BY recorded_at DESC LIMIT 3
            ''', (commodity,))
            
            prices = cur.fetchall()
            conn.close()
            
            if not prices:
                return None
            
            # Format response
            result = {}
            for price in prices:
                market = price["market"]
                if "Lusaka" in market:
                    result["lusaka"] = price["price"]
                elif "Kabwe" in market:
                    result["kabwe"] = price["price"]
                elif "Ndola" in market:
                    result["ndola"] = price["price"]
            
            # Determine trend
            result["trend"] = "stable"
            if len(prices) > 1:
                if prices[0]["price"] > prices[1]["price"]:
                    result["trend"] = "rising"
                elif prices[0]["price"] < prices[1]["price"]:
                    result["trend"] = "falling"
            
            return result
            
        except Exception as e:
            print(f"Error getting commodity price: {e}")
            return None
    
    def get_all_prices(self):
        """Get latest prices for all commodities"""
        try:
            conn = get_db()
            cur = conn.cursor()
            
            # Get latest price for each commodity
            cur.execute('''
                SELECT commodity, price FROM market_prices 
                WHERE verified=1 
                GROUP BY commodity 
                HAVING MAX(recorded_at)
                LIMIT 6
            ''')
            
            prices = [dict(row) for row in cur.fetchall()]
            conn.close()
            
            return prices
            
        except Exception as e:
            print(f"Error getting all prices: {e}")
            return []
    
    def get_maize_forecast(self, days=7):
        """Get maize price forecast"""
        # Simplified forecast for USSD
        base_price = 120.50
        
        forecast = []
        for i in range(1, days + 1):
            date = (datetime.now() + timedelta(days=i)).strftime("%d/%m")
            variation = random.uniform(-0.02, 0.03) * i
            price = round(base_price * (1 + variation), 2)
            trend = "up" if variation > 0 else "down" if variation < 0 else "same"
            
            forecast.append({
                "date": date,
                "price": price,
                "trend": trend
            })
        
        return forecast
    
    def get_buyers(self, commodity):
        """Get buyers for commodity"""
        try:
            conn = get_db()
            cur = conn.cursor()
            
            cur.execute('''
                SELECT name, phone, location FROM buyers 
                WHERE commodity=? AND verified=1 
                ORDER BY rating DESC LIMIT 5
            ''', (commodity,))
            
            buyers = [dict(row) for row in cur.fetchall()]
            conn.close()
            
            return buyers
            
        except Exception as e:
            print(f"Error getting buyers: {e}")
            return []
    
    def get_weather_info(self):
        """Get weather information"""
        # Simplified weather info
        weather_options = [
            "Sunny, good for drying crops",
            "Light rain expected, good for planting",
            "Heavy rains forecast, harvest quickly",
            "Dry spell expected, irrigate if possible",
            "Moderate weather, good for fieldwork"
        ]
        
        return random.choice(weather_options)
    
    def get_farming_tip(self):
        """Get farming tip"""
        tips = [
            "Plant maize 2 weeks before rains for best yield",
            "Rotate crops to improve soil fertility",
            "Use organic manure for better soil health",
            "Harvest early morning for freshness",
            "Store grains in dry, cool place"
        ]
        
        return random.choice(tips)
    
    def cleanup_sessions(self):
        """Clean up old USSD sessions"""
        now = datetime.now()
        expired_sessions = []
        
        for session_id, session in self.sessions.items():
            if (now - session["created"]).seconds > 300:  # 5 minutes
                expired_sessions.append(session_id)
        
        for session_id in expired_sessions:
            del self.sessions[session_id]

# =========================================================
# BACKUP MANAGER
# =========================================================

class BackupManager:
    """Manage automated backups"""
    
    def __init__(self):
        self.backup_dir = "backups"
        self.max_backups = 30  # Keep last 30 days
        
        # Create backup directory
        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir)
        
        # AWS S3 configuration (optional)
        self.s3_bucket = os.getenv('AWS_BACKUP_BUCKET')
        self.s3_enabled = AWS_AVAILABLE and self.s3_bucket
        
        if self.s3_enabled:
            self.s3_client = boto3.client('s3')
            print("✅ Backup Manager: AWS S3 enabled")
        else:
            print("⚠️  Backup Manager: AWS S3 disabled, using local backups only")
    
    def create_backup(self):
        """Create comprehensive system backup"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"farmconnect_backup_{timestamp}"
            
            print(f"💾 Creating backup: {backup_name}")
            
            # 1. Backup database
            db_backup = self.backup_database(backup_name)
            
            # 2. Backup logs
            logs_backup = self.backup_logs(backup_name)
            
            # 3. Backup configuration
            config_backup = self.backup_config(backup_name)
            
            # 4. Create backup manifest
            manifest = {
                "backup_name": backup_name,
                "timestamp": datetime.now().isoformat(),
                "components": {
                    "database": db_backup,
                    "logs": logs_backup,
                    "config": config_backup
                },
                "system_info": {
                    "version": "2.0.0",
                    "users": self.get_user_count(),
                    "prices": self.get_price_count(),
                    "forecast_models": self.get_model_count()
                }
            }
            
            # Save manifest
            manifest_path = os.path.join(self.backup_dir, f"{backup_name}_manifest.json")
            with open(manifest_path, 'w') as f:
                json.dump(manifest, f, indent=2)
            
            # 5. Create ZIP archive
            zip_path = self.create_zip_archive(backup_name)
            
            # 6. Upload to S3 if enabled
            if self.s3_enabled:
                self.upload_to_s3(zip_path, backup_name)
            
            # 7. Clean up old backups
            self.cleanup_old_backups()
            
            print(f"✅ Backup created: {backup_name}")
            
            return {
                "success": True,
                "backup_name": backup_name,
                "local_path": zip_path,
                "size": os.path.getsize(zip_path) if os.path.exists(zip_path) else 0
            }
            
        except Exception as e:
            print(f"❌ Backup creation failed: {e}")
            return {"success": False, "error": str(e)}
    
    def backup_database(self, backup_name):
        """Backup SQLite database"""
        try:
            db_path = "farm_market.db"
            backup_path = os.path.join(self.backup_dir, f"{backup_name}_database.db")
            
            # Copy database file
            import shutil
            shutil.copy2(db_path, backup_path)
            
            # Also export to SQL for portability
            sql_path = os.path.join(self.backup_dir, f"{backup_name}_database.sql")
            self.export_database_to_sql(sql_path)
            
            return {
                "path": backup_path,
                "sql_path": sql_path,
                "size": os.path.getsize(backup_path)
            }
            
        except Exception as e:
            print(f"Database backup failed: {e}")
            return {"error": str(e)}
    
    def export_database_to_sql(self, sql_path):
        """Export database to SQL file"""
        try:
            conn = sqlite3.connect('farm_market.db')
            
            with open(sql_path, 'w') as f:
                for line in conn.iterdump():
                    f.write(f'{line}\n')
            
            conn.close()
            
        except Exception as e:
            print(f"SQL export failed: {e}")
    
    def backup_logs(self, backup_name):
        """Backup system logs"""
        try:
            log_files = []
            
            # Collect log files
            for filename in os.listdir('.'):
                if filename.endswith('.log'):
                    log_files.append(filename)
            
            # Also include activity logs from database
            conn = get_db()
            cur = conn.cursor()
            cur.execute("SELECT * FROM activity_logs ORDER BY created_at DESC LIMIT 1000")
            activity_logs = [dict(row) for row in cur.fetchall()]
            conn.close()
            
            # Save activity logs
            activity_path = os.path.join(self.backup_dir, f"{backup_name}_activity.json")
            with open(activity_path, 'w') as f:
                json.dump(activity_logs, f, indent=2)
            
            return {
                "log_files": log_files,
                "activity_logs": len(activity_logs),
                "activity_path": activity_path
            }
            
        except Exception as e:
            print(f"Log backup failed: {e}")
            return {"error": str(e)}
    
    def backup_config(self, backup_name):
        """Backup configuration files"""
        try:
            config_files = []
            
            # Collect configuration files
            for filename in ['config.json', '.env', 'requirements.txt']:
                if os.path.exists(filename):
                    config_files.append(filename)
                    
                    # Copy each config file
                    backup_path = os.path.join(self.backup_dir, f"{backup_name}_{filename}")
                    import shutil
                    shutil.copy2(filename, backup_path)
            
            # Also backup forecast models
            if os.path.exists('models'):
                models_backup = os.path.join(self.backup_dir, f"{backup_name}_models")
                import shutil
                shutil.copytree('models', models_backup, dirs_exist_ok=True)
                
                config_files.append('models/')
            
            return {
                "config_files": config_files,
                "has_models": os.path.exists('models')
            }
            
        except Exception as e:
            print(f"Config backup failed: {e}")
            return {"error": str(e)}
    
    def create_zip_archive(self, backup_name):
        """Create ZIP archive of backup"""
        try:
            import zipfile
            
            zip_path = os.path.join(self.backup_dir, f"{backup_name}.zip")
            
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Add all backup files
                for filename in os.listdir(self.backup_dir):
                    if filename.startswith(backup_name):
                        filepath = os.path.join(self.backup_dir, filename)
                        if os.path.isfile(filepath):
                            zipf.write(filepath, filename)
                        elif os.path.isdir(filepath):
                            for root, dirs, files in os.walk(filepath):
                                for file in files:
                                    file_path = os.path.join(root, file)
                                    arcname = os.path.relpath(file_path, self.backup_dir)
                                    zipf.write(file_path, arcname)
            
            # Remove individual backup files (keep only ZIP)
            for filename in os.listdir(self.backup_dir):
                if filename.startswith(backup_name) and not filename.endswith('.zip'):
                    filepath = os.path.join(self.backup_dir, filename)
                    if os.path.isfile(filepath):
                        os.remove(filepath)
                    elif os.path.isdir(filepath):
                        import shutil
                        shutil.rmtree(filepath)
            
            return zip_path
            
        except Exception as e:
            print(f"ZIP creation failed: {e}")
            return None
    
    def upload_to_s3(self, filepath, backup_name):
        """Upload backup to AWS S3"""
        if not self.s3_enabled:
            return False
        
        try:
            s3_key = f"backups/{backup_name}.zip"
            self.s3_client.upload_file(filepath, self.s3_bucket, s3_key)
            
            print(f"✅ Uploaded to S3: s3://{self.s3_bucket}/{s3_key}")
            return True
            
        except Exception as e:
            print(f"❌ S3 upload failed: {e}")
            return False
    
    def cleanup_old_backups(self):
        """Clean up old backup files"""
        try:
            backup_files = []
            
            for filename in os.listdir(self.backup_dir):
                if filename.endswith('.zip'):
                    filepath = os.path.join(self.backup_dir, filename)
                    backup_files.append({
                        "path": filepath,
                        "mtime": os.path.getmtime(filepath)
                    })
            
            # Sort by modification time (oldest first)
            backup_files.sort(key=lambda x: x["mtime"])
            
            # Remove old backups if we have more than max_backups
            if len(backup_files) > self.max_backups:
                files_to_remove = backup_files[:len(backup_files) - self.max_backups]
                
                for file_info in files_to_remove:
                    os.remove(file_info["path"])
                    print(f"🧹 Removed old backup: {os.path.basename(file_info['path'])}")
            
        except Exception as e:
            print(f"Backup cleanup failed: {e}")
    
    def get_user_count(self):
        """Get total user count"""
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM users WHERE status='active'")
            count = cur.fetchone()[0]
            conn.close()
            return count
        except:
            return 0
    
    def get_price_count(self):
        """Get total price count"""
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM market_prices WHERE verified=1")
            count = cur.fetchone()[0]
            conn.close()
            return count
        except:
            return 0
    
    def get_model_count(self):
        """Get forecast model count"""
        try:
            if os.path.exists('models'):
                return len([f for f in os.listdir('models') if f.endswith('.pkl')])
            return 0
        except:
            return 0
    
    def restore_backup(self, backup_file):
        """Restore from backup file"""
        try:
            print(f"🔄 Restoring from backup: {backup_file}")
            
            if not os.path.exists(backup_file):
                return {"success": False, "error": "Backup file not found"}
            
            # Extract backup
            import zipfile
            import tempfile
            
            with tempfile.TemporaryDirectory() as temp_dir:
                with zipfile.ZipFile(backup_file, 'r') as zipf:
                    zipf.extractall(temp_dir)
                
                # Restore components
                for filename in os.listdir(temp_dir):
                    if filename.endswith('_database.db'):
                        # Restore database
                        db_path = os.path.join(temp_dir, filename)
                        import shutil
                        shutil.copy2(db_path, 'farm_market.db')
                        print("✅ Database restored")
                    
                    elif filename.endswith('_manifest.json'):
                        # Read manifest
                        manifest_path = os.path.join(temp_dir, filename)
                        with open(manifest_path, 'r') as f:
                            manifest = json.load(f)
                        print(f"✅ Restoring backup from: {manifest.get('backup_name')}")
            
            print("✅ Backup restored successfully")
            return {"success": True}
            
        except Exception as e:
            print(f"❌ Backup restoration failed: {e}")
            return {"success": False, "error": str(e)}

# =========================================================
# CONFIGURATION
# =========================================================

APP_SECRET = os.getenv('APP_SECRET', 'mulungushi-secret-key-2024')
JWT_SECRET = os.getenv('JWT_SECRET', 'jwt-secret-key-farm-market-2024')
DATABASE = "farm_market.db"
FRONTEND_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend")

# Ensure frontend directory exists
if not os.path.exists(FRONTEND_DIR):
    os.makedirs(FRONTEND_DIR)
    print(f"📁 Created frontend directory: {FRONTEND_DIR}")

# Initialize services
zambian_data = ZambianMarketData()
sms_service = SMSService()
ussd_service = USSDService()
backup_manager = BackupManager()

# =========================================================
# DATA SCHEDULER INITIALIZATION
# =========================================================

try:
    from data_scheduler import DataScheduler
    data_scheduler = DataScheduler()
    print("✅ Data Scheduler initialized")
    SCHEDULER_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  Data Scheduler not available: {e}")
    data_scheduler = None
    SCHEDULER_AVAILABLE = False

# =========================================================
# FLASK INIT
# =========================================================

app = Flask(__name__, static_folder=FRONTEND_DIR, static_url_path='')
app.secret_key = APP_SECRET
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

# =========================================================
# DATABASE UTILITIES
# =========================================================

def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initialize database with complete schema"""
    conn = get_db()
    cur = conn.cursor()
    
    # Drop existing markets table if it has issues
    try:
        cur.execute("SELECT * FROM markets LIMIT 1")
        # Check if last_updated column exists
        columns = [col[0] for col in cur.description]
        if 'last_updated' not in columns:
            print("🔄 Recreating markets table with correct schema...")
            cur.execute("DROP TABLE IF EXISTS markets")
    except:
        pass  # Table doesn't exist yet
    
    # Create tables if they don't exist
    tables = [
        """CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT UNIQUE,
            username TEXT UNIQUE,
            password_hash TEXT,
            name TEXT,
            role TEXT,
            phone TEXT,
            email TEXT,
            location TEXT,
            farm_size REAL,
            main_crops TEXT,
            business_name TEXT,
            license_number TEXT,
            trading_commodities TEXT,
            created_at TEXT,
            last_login TEXT,
            status TEXT DEFAULT 'active',
            sms_alerts BOOLEAN DEFAULT 1,
            ussd_pin TEXT,
            last_sms_sent TEXT
        )""",
        
        """CREATE TABLE IF NOT EXISTS market_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market TEXT NOT NULL,
            commodity TEXT NOT NULL,
            price REAL NOT NULL,
            unit TEXT DEFAULT 'ZMW/kg',
            volume REAL,
            quality TEXT,
            source TEXT,
            verified BOOLEAN DEFAULT 0,
            recorded_at TIMESTAMP,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            region TEXT,
            market_lat REAL,
            market_lon REAL,
            price_trend TEXT,
            UNIQUE(market, commodity, recorded_at)
        )""",
        
        """CREATE TABLE IF NOT EXISTS data_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            type TEXT NOT NULL,
            url TEXT,
            enabled BOOLEAN DEFAULT 1,
            priority INTEGER DEFAULT 1,
            update_frequency TEXT DEFAULT 'daily',
            last_updated TIMESTAMP,
            success_rate REAL DEFAULT 0,
            total_attempts INTEGER DEFAULT 0,
            total_success INTEGER DEFAULT 0,
            last_error TEXT
        )""",
        
        """CREATE TABLE IF NOT EXISTS buyers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            phone TEXT,
            commodity TEXT,
            location TEXT,
            max_price REAL,
            min_volume REAL,
            notes TEXT,
            verified BOOLEAN DEFAULT 0,
            rating REAL DEFAULT 4.0,
            added_by TEXT,
            created_at TEXT,
            last_contact TEXT,
            status TEXT DEFAULT 'active'
        )""",
        
        """CREATE TABLE IF NOT EXISTS price_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            commodity TEXT,
            target_price REAL,
            alert_type TEXT,
            active BOOLEAN DEFAULT 1,
            created_at TEXT,
            triggered_at TEXT,
            triggered_price REAL
        )""",
        
        """CREATE TABLE IF NOT EXISTS activity_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user TEXT,
            action TEXT,
            details TEXT,
            ip_address TEXT,
            created_at TEXT
        )""",
        
        """CREATE TABLE IF NOT EXISTS admin_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_user TEXT,
            action TEXT,
            target_type TEXT,
            target_id INTEGER,
            details TEXT,
            ip_address TEXT,
            created_at TEXT
        )""",
        
        """CREATE TABLE IF NOT EXISTS sms_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone TEXT,
            message TEXT,
            type TEXT,
            status TEXT,
            sent_at TEXT,
            message_id TEXT
        )""",
        
        """CREATE TABLE IF NOT EXISTS markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            region TEXT,
            district TEXT,
            gps_lat REAL,
            gps_lon REAL,
            market_days TEXT,
            contact_phone TEXT,
            notes TEXT,
            active BOOLEAN DEFAULT 1,
            last_updated TEXT
        )""",
        
        """CREATE TABLE IF NOT EXISTS collection_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT,
            operation TEXT,
            records_collected INTEGER,
            status TEXT,
            error_message TEXT,
            duration_seconds REAL,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        
        """CREATE TABLE IF NOT EXISTS forecast_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity TEXT,
            market TEXT,
            forecast_days INTEGER,
            forecast_data TEXT,
            generated_at TIMESTAMP,
            expires_at TIMESTAMP,
            model_used TEXT,
            accuracy_score REAL
        )""",
        
        """CREATE TABLE IF NOT EXISTS user_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            session_token TEXT,
            created_at TIMESTAMP,
            expires_at TIMESTAMP,
            ip_address TEXT,
            user_agent TEXT
        )""",
        
        """CREATE TABLE IF NOT EXISTS system_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_type TEXT,
            metric_value REAL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            details TEXT
        )"""
    ]
    
    for table_sql in tables:
        try:
            cur.execute(table_sql)
        except Exception as e:
            print(f"⚠️  Table creation error: {e}")
    
    # Check if we need to add demo data
    cur.execute("SELECT COUNT(*) as count FROM users")
    if cur.fetchone()[0] == 0:
        print("📝 Adding demo users...")
        demo_users = [
            ('farmer1', generate_password_hash('farmer123', method='pbkdf2:sha256'), 
             'John Farmer', 'farmer', '+260971234567', 'john@example.com', 
             'Lusaka', 10.5, 'Maize, Tomatoes', None, None, None, 
             datetime.now().isoformat(), 'active', 1, '1234', None),
            
            ('trader1', generate_password_hash('trader123', method='pbkdf2:sha256'), 
             'Sarah Trader', 'trader', '+260971234568', 'sarah@example.com', 
             'Kabwe', None, None, 'Agri Trading Ltd', 'LIC-2024-001', 
             'Maize, Beans', datetime.now().isoformat(), 'active', 1, '5678', None),
            
            ('admin1', generate_password_hash('admin123', method='pbkdf2:sha256'), 
             'Admin User', 'admin', '+260971234569', 'admin@example.com', 
             'Ndola', None, None, None, None, None, 
             datetime.now().isoformat(), 'active', 1, '9999', None),
            
            ('farmer2', generate_password_hash('password123', method='pbkdf2:sha256'),
             'Mary Banda', 'farmer', '+260972345678', 'mary@example.com',
             'Southern Province', 5.2, 'Maize, Groundnuts, Beans', None, None, None,
             datetime.now().isoformat(), 'active', 1, '4321', None)
        ]
        
        for user_data in demo_users:
            user_id = str(uuid.uuid4())
            try:
                cur.execute("""
                    INSERT INTO users (user_id, username, password_hash, name, role, phone, email, location, 
                                      farm_size, main_crops, business_name, license_number, trading_commodities, 
                                      created_at, status, sms_alerts, ussd_pin)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (user_id, *user_data))
            except Exception as e:
                print(f"⚠️  Error adding user: {e}")
    
    # Add Zambian markets
    print("📍 Adding Zambian markets...")
    for region, region_data in zambian_data.ZAMBIAN_MARKETS.items():
        for market_info in region_data["markets"]:
            try:
                cur.execute("""
                    INSERT OR IGNORE INTO markets (name, region, gps_lat, gps_lon, market_days, contact_phone, active, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    market_info["name"],
                    region,
                    market_info.get("lat"),
                    market_info.get("lon"),
                    ",".join(region_data.get("market_days", [])),
                    region_data.get("contact", ""),
                    market_info.get("active", True),
                    datetime.now().isoformat()
                ))
            except Exception as e:
                print(f"⚠️  Error adding market: {e}")
    
    # Add data sources
    print("📡 Adding data sources...")
    sources = [
        ("ZNFU Official", "web", "https://www.znfu.co.zm/market-prices/", 1),
        ("Ministry of Agriculture", "web", "https://www.mac.gov.zm/category/market-information/", 1),
        ("IAPRI Research", "api", "https://iapri.org.zm/market-information/", 2),
        ("CSO Zambia", "web", "https://www.zamstats.gov.zm/category/agriculture-statistics/", 2),
        ("FAO Zambia", "api", "https://www.fao.org/zambia/statistics/en/", 3),
    ]
    
    for name, type_, url, priority in sources:
        try:
            cur.execute('''
                INSERT OR IGNORE INTO data_sources (name, type, url, priority)
                VALUES (?, ?, ?, ?)
            ''', (name, type_, url, priority))
        except Exception as e:
            print(f"⚠️  Error adding data source: {e}")
    
    # Add realistic Zambian prices
    cur.execute("SELECT COUNT(*) as count FROM market_prices")
    if cur.fetchone()[0] == 0:
        print("📊 Adding realistic Zambian market data...")
        prices = zambian_data.fetch_all_sources()
        
        for price_data in prices:
            try:
                cur.execute("""
                    INSERT INTO market_prices 
                    (market, commodity, price, unit, volume, quality, source, verified, recorded_at, region, price_trend)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    price_data.get("market", "Unknown"),
                    price_data.get("commodity", "Unknown"),
                    price_data.get("price", 0),
                    price_data.get("unit", "ZMW/kg"),
                    price_data.get("volume"),
                    price_data.get("quality"),
                    price_data.get("source", "Zambian_Source"),
                    price_data.get("verified", False),
                    price_data.get("recorded_at", datetime.now().isoformat()),
                    price_data.get("region"),
                    price_data.get("price_trend", "stable")
                ))
            except Exception as e:
                print(f"⚠️  Error adding price: {e}")
    
    # Add demo buyers
    cur.execute("SELECT COUNT(*) as count FROM buyers")
    if cur.fetchone()[0] == 0:
        print("👥 Adding demo buyers...")
        demo_buyers = [
            ('Agri Trading Ltd', '+260971111111', 'Maize', 'Lusaka', 140.0, 1000, 
             'Bulk buyer, weekly collections. Pays cash on delivery.', 1, 4.5, 'admin1', 
             datetime.now().isoformat(), None, 'active'),
            
            ('Fresh Produce Co.', '+260972222222', 'Tomatoes', 'Kabwe', 90.0, 500, 
             'Daily collection, fresh produce only. Quality premium paid.', 1, 4.2, 'trader1',
             datetime.now().isoformat(), None, 'active'),
            
            ('Grain Masters', '+260973333333', 'Maize', 'Ndola', 135.0, 2000, 
             'Weekly orders, bulk purchase. Transport provided.', 0, 3.8, 'farmer1',
             datetime.now().isoformat(), None, 'pending'),
            
            ('Zambia National Trading', '+260974444444', 'Rice', 'Copperbelt', 210.0, 1500, 
             'Government contractor. Fixed prices.', 1, 4.7, 'admin1',
             datetime.now().isoformat(), None, 'active'),
            
            ('Smallholder Support', '+260975555555', 'Beans', 'Eastern Province', 110.0, 800, 
             'Supports local farmers. Fair trade certified.', 1, 4.0, 'farmer2',
             datetime.now().isoformat(), None, 'active'),
            
            ('Export Quality Ltd', '+260976666666', 'Groundnuts', 'Lusaka', 220.0, 5000,
             'Export quality only. Premium prices for Grade A.', 1, 4.8, 'admin1',
             datetime.now().isoformat(), None, 'active'),
            
            ('Vegetable Wholesalers', '+260977777777', 'Tomatoes, Onions', 'Southern', 85.0, 1000,
             'Daily market collection. Multiple vegetable types.', 1, 4.1, 'trader1',
             datetime.now().isoformat(), None, 'active')
        ]
        
        for buyer_data in demo_buyers:
            try:
                cur.execute("""
                    INSERT INTO buyers (name, phone, commodity, location, max_price, min_volume, 
                                       notes, verified, rating, added_by, created_at, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, buyer_data)
            except Exception as e:
                print(f"⚠️  Error adding buyer: {e}")
    
    conn.commit()
    conn.close()
    print("✅ Database initialized successfully with Zambian data!")

# Initialize database on startup
try:
    init_db()
    print("✅ Database ready!")
except Exception as e:
    print(f"⚠️  Database initialization error: {e}")
    print("⚠️  Server will continue, but some features may not work.")

# =========================================================
# JWT HELPERS
# =========================================================

def create_token(user):
    """Create JWT token for user"""
    payload = {
        "user_id": user["user_id"],
        "username": user["username"],
        "name": user["name"],
        "role": user["role"],
        "phone": user["phone"],
        "location": user["location"],
        "exp": datetime.utcnow() + timedelta(hours=24)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

def token_required(f):
    """Decorator for token authentication"""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get("Authorization")
        if not token:
            return jsonify({"error": "Token missing"}), 401
        try:
            if token.startswith("Bearer "):
                token = token[7:]
            data = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
            request.user = data
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token expired"}), 401
        except Exception as e:
            return jsonify({"error": "Invalid token", "details": str(e)}), 401
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    """Decorator for admin-only access"""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get("Authorization")
        if not token:
            return jsonify({"error": "Token missing"}), 401
        try:
            if token.startswith("Bearer "):
                token = token[7:]
            data = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
            if data.get("role") != "admin":
                return jsonify({"error": "Admin access required"}), 403
            request.user = data
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token expired"}), 401
        except Exception as e:
            return jsonify({"error": "Invalid token", "details": str(e)}), 401
        return f(*args, **kwargs)
    return decorated

# =========================================================
# LOGGING HELPERS
# =========================================================

def log_activity(user, action, details):
    """Log user activity"""
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO activity_logs (user, action, details, ip_address, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, (user, action, details, request.remote_addr, datetime.now().isoformat()))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Activity log error: {e}")

def log_collection(source_name, operation, records_collected, status, error_message=None, duration=None):
    """Log data collection activity"""
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO collection_logs 
            (source_name, operation, records_collected, status, error_message, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (source_name, operation, records_collected, status, error_message, duration))
        
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error logging collection: {e}")

def log_system_metric(metric_type, metric_value, details=None):
    """Log system metrics for monitoring"""
    try:
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO system_metrics (metric_type, metric_value, details)
            VALUES (?, ?, ?)
        """, (metric_type, metric_value, details))
        
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error logging metric: {e}")

# =========================================================
# FRONTEND PAGE SERVING
# =========================================================

@app.route('/')
def serve_index():
    """Serve the main index.html page"""
    index_path = os.path.join(FRONTEND_DIR, 'index.html')
    if os.path.exists(index_path):
        return send_file(index_path)
    else:
        return render_template_string("""
        <!DOCTYPE html>
        <html>
        <head>
            <title>FarmConnect - Cloud Market Platform</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    padding: 40px;
                    text-align: center;
                    background: linear-gradient(135deg, #2E8B57 0%, #1f6b43 100%);
                    color: white;
                    min-height: 100vh;
                    display: flex;
                    flex-direction: column;
                    justify-content: center;
                    align-items: center;
                }
                .container {
                    max-width: 800px;
                    background: rgba(255,255,255,0.95);
                    padding: 40px;
                    border-radius: 15px;
                    box-shadow: 0 10px 30px rgba(0,0,0,0.2);
                    color: #333;
                }
                h1 {
                    color: #2E8B57;
                    margin-bottom: 20px;
                }
                .logo {
                    font-size: 3rem;
                    margin-bottom: 20px;
                    color: #2E8B57;
                }
                .btn {
                    display: inline-block;
                    background: #2E8B57;
                    color: white;
                    padding: 12px 24px;
                    border-radius: 8px;
                    text-decoration: none;
                    margin: 10px;
                    transition: all 0.3s;
                }
                .btn:hover {
                    background: #1f6b43;
                    transform: translateY(-2px);
                }
                .features {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 20px;
                    margin: 30px 0;
                }
                .feature {
                    background: #f8f9fa;
                    padding: 20px;
                    border-radius: 8px;
                    border-left: 4px solid #FFA500;
                }
                .status {
                    display: inline-block;
                    padding: 5px 15px;
                    border-radius: 20px;
                    font-size: 0.9rem;
                    font-weight: bold;
                    margin: 5px;
                }
                .status-online {
                    background: #28a745;
                    color: white;
                }
                .status-offline {
                    background: #dc3545;
                    color: white;
                }
                .status-warning {
                    background: #ffc107;
                    color: #333;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="logo">🌾</div>
                <h1>FarmConnect - Cloud Market Platform</h1>
                <p>Backend server is running successfully!</p>
                
                <div style="margin: 20px 0;">
                    <span class="status status-online">✅ SERVER ONLINE</span>
                    <span class="status status-online">✅ DATABASE READY</span>
                    <span class="status status-online">✅ ZAMBIAN DATA ENABLED</span>
                    {% if FORECAST_AVAILABLE %}
                    <span class="status status-online">✅ AI FORECAST READY</span>
                    {% else %}
                    <span class="status status-warning">⚠️ FORECAST LIMITED</span>
                    {% endif %}
                    {% if TWILIO_AVAILABLE %}
                    <span class="status status-online">✅ SMS READY</span>
                    {% else %}
                    <span class="status status-warning">⚠️ SMS DEMO MODE</span>
                    {% endif %}
                    {% if AFRICASTALKING_AVAILABLE %}
                    <span class="status status-online">✅ USSD READY</span>
                    {% else %}
                    <span class="status status-warning">⚠️ USSD DEMO MODE</span>
                    {% endif %}
                    {% if SCHEDULER_AVAILABLE and data_scheduler %}
                    <span class="status status-online">✅ SCHEDULER READY</span>
                    {% else %}
                    <span class="status status-warning">⚠️ SCHEDULER LIMITED</span>
                    {% endif %}
                </div>
                
                <p><strong>🇿🇲 REAL ZAMBIAN MARKET DATA ENABLED</strong></p>
                <p>Place your HTML files in: <code>{{ frontend_dir }}</code></p>
                
                <div class="features">
                    <div class="feature">
                        <h3>📊 Zambian Market Prices</h3>
                        <p>Real-time commodity prices from ZNFU, MACO, CSO, IAPRI</p>
                        <span class="status status-online">Active</span>
                    </div>
                    <div class="feature">
                        <h3>📈 AI Price Forecast</h3>
                        <p>Machine learning predictions for Zambian markets</p>
                        {% if FORECAST_AVAILABLE %}
                        <span class="status status-online">Enhanced</span>
                        {% else %}
                        <span class="status status-warning">Basic</span>
                        {% endif %}
                    </div>
                    <div class="feature">
                        <h3>📱 USSD & SMS Access</h3>
                        <p>Access via *123# or SMS for basic phones</p>
                        <span class="status status-online">Ready</span>
                    </div>
                    <div class="feature">
                        <h3>⏰ Automated Data Collection</h3>
                        <p>Scheduled Zambian market data updates</p>
                        {% if SCHEDULER_AVAILABLE and data_scheduler %}
                        <span class="status status-online">Active</span>
                        {% else %}
                        <span class="status status-warning">Limited</span>
                    {% endif %}
                    </div>
                </div>
                
                <h3>Available Pages:</h3>
                <div>
                    <a href="/dashboard.html" class="btn">📊 Dashboard</a>
                    <a href="/market-prices.html" class="btn">💰 Market Prices</a>
                    <a href="/price-forecast.html" class="btn">📈 Forecast</a>
                    <a href="/find-buyers.html" class="btn">👥 Find Buyers</a>
                    <a href="/profile.html" class="btn">👤 Profile</a>
                    <a href="/login.html" class="btn">🔐 Login</a>
                    <a href="/register.html" class="btn">📝 Register</a>
                    <a href="/admin-panel.html" class="btn">🛠️ Admin Panel</a>
                </div>
                
                <h3 style="margin-top: 30px;">API Endpoints:</h3>
                <div style="text-align: left; background: #f8f9fa; padding: 15px; border-radius: 8px; margin-top: 10px;">
                    <code>GET /api/status</code> - System status<br>
                    <code>POST /api/login</code> - User login<br>
                    <code>POST /api/register</code> - User registration<br>
                    <code>GET /api/prices/real</code> - Real Zambian prices<br>
                    <code>GET /api/forecast/real</code> - Enhanced forecast<br>
                    <code>GET /api/buyers</code> - Buyer listings<br>
                    <code>GET /api/data/status</code> - Data collection status<br>
                    <code>POST /api/data/collect</code> - Collect Zambian data<br>
                    <code>POST /api/sms/send</code> - Send SMS alert<br>
                    <code>POST /api/ussd/callback</code> - USSD callback<br>
                    <code>POST /api/backup/create</code> - Create backup (admin)<br>
                    <code>GET /api/admin/stats</code> - Admin statistics<br>
                </div>
                
                <div style="margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd;">
                    <p><strong>Mulungushi University - ICT 431 Capstone Project</strong></p>
                    <p>Student: Daka Felix (202206453) | Supervisor: Mr. E Nyirenda</p>
                    <p style="font-size: 0.9rem; color: #666;">Enhanced Version 2.0.0</p>
                </div>
            </div>
        </body>
        </html>
        """, frontend_dir=FRONTEND_DIR, FORECAST_AVAILABLE=FORECAST_AVAILABLE,
           TWILIO_AVAILABLE=TWILIO_AVAILABLE, AFRICASTALKING_AVAILABLE=AFRICASTALKING_AVAILABLE,
           SCHEDULER_AVAILABLE=SCHEDULER_AVAILABLE, data_scheduler=data_scheduler)

@app.route('/<path:filename>')
def serve_frontend(filename):
    """Serve all frontend files"""
    # Handle HTML files
    if filename.endswith('.html') or '.' not in filename:
        if '.' not in filename:
            filename += '.html'
        filepath = os.path.join(FRONTEND_DIR, filename)
        if os.path.exists(filepath):
            return send_file(filepath)
        else:
            return render_template_string(f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>{filename.replace('.html', '').title()} - FarmConnect</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        padding: 40px;
                        text-align: center;
                        background: #f5f5f5;
                    }}
                    .container {{
                        max-width: 600px;
                        margin: 0 auto;
                        background: white;
                        padding: 30px;
                        border-radius: 10px;
                        box-shadow: 0 5px 15px rgba(0,0,0,0.1);
                    }}
                    h1 {{ color: #2E8B57; }}
                    .btn {{
                        display: inline-block;
                        background: #2E8B57;
                        color: white;
                        padding: 10px 20px;
                        border-radius: 5px;
                        text-decoration: none;
                        margin: 10px;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>{filename.replace('.html', '').title()} Page</h1>
                    <p>This page is under development. File not found: <code>{filename}</code></p>
                    <p>Create this file in the frontend directory to see the content.</p>
                    <div>
                        <a href="/" class="btn">🏠 Home</a>
                        <a href="/dashboard.html" class="btn">📊 Dashboard</a>
                        <a href="/market-prices.html" class="btn">💰 Prices</a>
                    </div>
                </div>
            </body>
            </html>
            """)
    
    # Handle CSS, JS, and other static files
    try:
        return send_from_directory(FRONTEND_DIR, filename)
    except:
        return jsonify({"error": "File not found", "filename": filename}), 404

# =========================================================
# AUTH ROUTES
# =========================================================

@app.route("/api/login", methods=["POST"])
def login():
    """User login"""
    data = request.json
    username = data.get("username")
    password = data.get("password")
    
    if not username or not password:
        return jsonify({"error": "Username and password required"}), 400
    
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("SELECT * FROM users WHERE username=?", (username,))
    user = cur.fetchone()
    
    if not user:
        conn.close()
        return jsonify({"error": "User not found"}), 404
    
    # Check password
    try:
        if check_password_hash(user["password_hash"], password):
            # Login successful
            user_dict = dict(user)
            token = create_token(user_dict)
            
            # Update last login
            cur.execute("UPDATE users SET last_login=? WHERE user_id=?", 
                       (datetime.now().isoformat(), user["user_id"]))
            conn.commit()
            conn.close()
            
            log_activity(username, "Login", "User logged into system")
            log_system_metric("user_login", 1, f"user:{username}")
            
            # Handle sms_alerts field - safely get with default
            sms_alerts = True
            try:
                if "sms_alerts" in user_dict:
                    sms_alerts = bool(user_dict["sms_alerts"])
            except:
                sms_alerts = True
            
            return jsonify({
                "message": "Login successful",
                "token": token,
                "user": {
                    "id": user_dict.get("user_id", ""),
                    "username": user_dict.get("username", ""),
                    "name": user_dict.get("name", ""),
                    "role": user_dict.get("role", ""),
                    "phone": user_dict.get("phone", ""),
                    "email": user_dict.get("email", ""),
                    "location": user_dict.get("location", ""),
                    "farm_size": user_dict.get("farm_size"),
                    "main_crops": user_dict.get("main_crops", ""),
                    "business_name": user_dict.get("business_name", ""),
                    "sms_alerts": sms_alerts
                }
            })
        else:
            conn.close()
            return jsonify({"error": "Invalid password"}), 401
    except Exception as e:
        conn.close()
        print(f"Login error: {e}")
        return jsonify({"error": "Invalid credentials"}), 401

@app.route("/api/register", methods=["POST"])
def register():
    """User registration"""
    data = request.json
    required = ["username", "password", "name", "role", "phone"]
    
    for field in required:
        if field not in data:
            return jsonify({"error": f"Missing required field: {field}"}), 400
    
    conn = get_db()
    cur = conn.cursor()
    
    # Check if username exists
    cur.execute("SELECT username FROM users WHERE username=?", (data["username"],))
    if cur.fetchone():
        conn.close()
        return jsonify({"error": "Username already exists"}), 400
    
    user_id = str(uuid.uuid4())
    password_hash = generate_password_hash(data["password"], method='pbkdf2:sha256')
    
    try:
        cur.execute("""
            INSERT INTO users (user_id, username, password_hash, name, role, phone, email, 
                              location, farm_size, main_crops, business_name, license_number, 
                              trading_commodities, created_at, status, sms_alerts, ussd_pin)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            user_id,
            data["username"],
            password_hash,
            data["name"],
            data["role"],
            data["phone"],
            data.get("email", ""),
            data.get("location", ""),
            data.get("farm_size"),
            data.get("main_crops", ""),
            data.get("business_name", ""),
            data.get("license_number", ""),
            data.get("trading_commodities", ""),
            datetime.now().isoformat(),
            "active",
            data.get("sms_alerts", True),
            str(random.randint(1000, 9999))  # Generate USSD PIN
        ))
        
        conn.commit()
        
        # Get the newly created user
        cur.execute("SELECT * FROM users WHERE user_id=?", (user_id,))
        new_user = cur.fetchone()
        user_dict = dict(new_user)
        token = create_token(user_dict)
        
        conn.close()
        
        log_activity(data["username"], "Registration", "New user registered")
        log_system_metric("user_registration", 1, f"role:{data['role']}")
        
        # Send welcome SMS if phone provided
        if data["phone"] and sms_service.active:
            welcome_msg = f"Welcome {data['name']} to FarmConnect Zambia! "
            welcome_msg += f"Your USSD PIN is {user_dict['ussd_pin']}. Dial *123# for market prices."
            sms_service.send_sms(data["phone"], welcome_msg)
        
        return jsonify({
            "message": "User registered successfully",
            "token": token,
            "user": {
                "id": user_dict["user_id"],
                "username": user_dict["username"],
                "name": user_dict["name"],
                "role": user_dict["role"],
                "phone": user_dict["phone"],
                "email": user_dict["email"],
                "location": user_dict["location"],
                "ussd_pin": user_dict["ussd_pin"]
            }
        })
        
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

@app.route("/api/logout", methods=["POST"])
@token_required
def logout():
    """User logout"""
    user = request.user
    
    log_activity(user["username"], "Logout", "User logged out")
    
    return jsonify({
        "message": "Logged out successfully"
    })

# =========================================================
# ZAMBIAN MARKET DATA ROUTES
# =========================================================

@app.route("/api/data/collect", methods=["POST"])
@admin_required
def collect_zambian_data():
    """Collect data from Zambian sources"""
    try:
        start_time = time.time()
        print("🌍 Collecting Zambian market data...")
        
        # Fetch data from all Zambian sources
        prices = zambian_data.fetch_all_sources()
        
        # Save to database
        saved_count = 0
        conn = get_db()
        cur = conn.cursor()
        
        for price_data in prices:
            try:
                cur.execute("""
                    INSERT OR REPLACE INTO market_prices 
                    (market, commodity, price, unit, volume, quality, source, verified, 
                     recorded_at, region, price_trend)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    price_data.get("market", "Unknown"),
                    price_data.get("commodity", "Unknown"),
                    price_data.get("price", 0),
                    price_data.get("unit", "ZMW/kg"),
                    price_data.get("volume"),
                    price_data.get("quality"),
                    price_data.get("source", "Zambian_Source"),
                    price_data.get("verified", True),
                    price_data.get("recorded_at", datetime.now().isoformat()),
                    price_data.get("region"),
                    price_data.get("price_trend", "stable")
                ))
                saved_count += 1
            except Exception as e:
                print(f"⚠️  Error saving price: {e}")
        
        conn.commit()
        conn.close()
        
        duration = time.time() - start_time
        
        # Log collection
        log_collection(
            source_name="Zambian_Market_Data",
            operation="manual_collection",
            records_collected=saved_count,
            status="success",
            duration=duration
        )
        
        log_activity(request.user["username"], "Data collection", f"Collected {saved_count} Zambian prices")
        log_system_metric("data_collection", saved_count, f"duration:{duration:.2f}")
        
        return jsonify({
            "success": True,
            "message": f"Collected {saved_count} prices from Zambian sources",
            "duration": round(duration, 2),
            "saved_count": saved_count,
            "timestamp": datetime.now().isoformat(),
            "sources_used": ["ZNFU", "MACO"]  # Default sources
        })
        
    except Exception as e:
        log_collection(
            source_name="Zambian_Market_Data",
            operation="manual_collection",
            records_collected=0,
            status="failed",
            error_message=str(e)
        )
        return jsonify({"error": str(e)}), 500

@app.route("/api/data/status", methods=["GET"])
def get_data_status():
    """Get data collection status"""
    try:
        conn = get_db()
        cur = conn.cursor()
        
        # Get total counts
        cur.execute("SELECT COUNT(*) FROM market_prices")
        total_prices = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM market_prices WHERE verified = 1")
        verified_prices = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM data_sources WHERE enabled = 1")
        active_sources = cur.fetchone()[0]
        
        # Get recent collections
        cur.execute('''
            SELECT source_name, status, records_collected, collected_at
            FROM collection_logs 
            ORDER BY collected_at DESC 
            LIMIT 10
        ''')
        recent_logs = [dict(row) for row in cur.fetchall()]
        
        # Get source statistics
        cur.execute('''
            SELECT name, type, url, enabled, priority, last_updated, success_rate, total_attempts, total_success
            FROM data_sources 
            ORDER BY priority, name
        ''')
        sources = [dict(row) for row in cur.fetchall()]
        
        # Get data freshness
        cur.execute('''
            SELECT MAX(recorded_at) as latest_update,
                   MIN(recorded_at) as oldest_update,
                   COUNT(DISTINCT commodity) as unique_commodities,
                   COUNT(DISTINCT market) as unique_markets
            FROM market_prices 
            WHERE verified = 1
        ''')
        freshness = dict(cur.fetchone())
        
        conn.close()
        
        return jsonify({
            "status": "active",
            "total_prices": total_prices,
            "verified_prices": verified_prices,
            "verification_rate": f"{(verified_prices/total_prices*100):.1f}%" if total_prices > 0 else "0%",
            "active_sources": active_sources,
            "data_freshness": freshness,
            "recent_collections": recent_logs,
            "sources": sources,
            "zambian_data": {
                "regions": list(zambian_data.ZAMBIAN_MARKETS.keys()) if hasattr(zambian_data, 'ZAMBIAN_MARKETS') else [],
                "commodities": list(zambian_data.COMMODITY_PRICE_RANGES.keys()) if hasattr(zambian_data, 'COMMODITY_PRICE_RANGES') else [],
                "last_updated": datetime.now().isoformat()
            }
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/prices/real", methods=["GET"])
def get_real_prices():
    """Get real Zambian market prices"""
    conn = get_db()
    cur = conn.cursor()
    
    commodity = request.args.get("commodity", "all")
    market = request.args.get("market", "all")
    region = request.args.get("region", "all")
    limit = request.args.get("limit", "100")
    verified_only = request.args.get("verified", "true").lower() == "true"
    latest_only = request.args.get("latest", "false").lower() == "true"
    
    if latest_only:
        # Get latest price for each commodity-market pair
        query = '''
            SELECT mp1.* FROM market_prices mp1
            INNER JOIN (
                SELECT market, commodity, MAX(recorded_at) as latest
                FROM market_prices 
                WHERE verified = 1
                GROUP BY market, commodity
            ) mp2 ON mp1.market = mp2.market 
                   AND mp1.commodity = mp2.commodity 
                   AND mp1.recorded_at = mp2.latest
            WHERE 1=1
        '''
    else:
        query = '''
            SELECT id, market, commodity, price, unit, volume, 
                   quality, source, verified, recorded_at, region, price_trend
            FROM market_prices 
            WHERE 1=1
        '''
    
    params = []
    
    if commodity != "all":
        query += " AND commodity=?"
        params.append(commodity)
    
    if market != "all":
        query += " AND market LIKE ?"
        params.append(f"%{market}%")
    
    if region != "all":
        query += " AND region=?"
        params.append(region)
    
    if verified_only:
        query += " AND verified=1"
    
    if not latest_only:
        query += " ORDER BY recorded_at DESC LIMIT ?"
        params.append(int(limit))
    
    cur.execute(query, params)
    prices = [dict(row) for row in cur.fetchall()]
    
    # Get statistics
    cur.execute("SELECT COUNT(*) FROM market_prices WHERE verified = 1")
    total_verified = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT market) FROM market_prices WHERE verified = 1")
    unique_markets = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT commodity) FROM market_prices WHERE verified = 1")
    unique_commodities = cur.fetchone()[0]
    
    # Get price ranges
    price_ranges = {}
    if commodity != "all":
        cur.execute('''
            SELECT MIN(price) as min_price, MAX(price) as max_price, AVG(price) as avg_price
            FROM market_prices 
            WHERE commodity=? AND verified=1
        ''', (commodity,))
        range_data = cur.fetchone()
        price_ranges[commodity] = dict(range_data)
    
    conn.close()
    
    return jsonify({
        "prices": prices,
        "statistics": {
            "total_verified": total_verified,
            "unique_markets": unique_markets,
            "unique_commodities": unique_commodities,
            "returned": len(prices)
        },
        "price_ranges": price_ranges,
        "timestamp": datetime.now().isoformat()
    })

# =========================================================
# FORECAST ROUTES (ENHANCED)
# =========================================================

@app.route("/api/forecast/real", methods=["GET"])
def get_real_forecast():
    """Get enhanced forecast using Zambian data"""
    commodity = request.args.get("commodity", "Maize")
    market = request.args.get("market", "Lusaka")
    days = int(request.args.get("days", 7))
    model_type = request.args.get("model", "auto")
    
    try:
        # Load historical data
        conn = get_db()
        cur = conn.cursor()
        
        cur.execute('''
            SELECT price, recorded_at, market
            FROM market_prices 
            WHERE commodity = ? AND market LIKE ? AND verified = 1
            ORDER BY recorded_at DESC
            LIMIT 90
        ''', (commodity, f"%{market}%"))
        
        historical_data = cur.fetchall()
        conn.close()
        
        if not historical_data:
            # Try to get any data for the commodity
            conn = get_db()
            cur = conn.cursor()
            cur.execute('''
                SELECT price, recorded_at FROM market_prices 
                WHERE commodity = ? AND verified = 1
                ORDER BY recorded_at DESC LIMIT 30
            ''', (commodity,))
            historical_data = cur.fetchall()
            conn.close()
        
        if not historical_data:
            return jsonify({
                "error": f"No historical data found for {commodity}",
                "forecast": [],
                "model": "no_data"
            }), 404
        
        # Convert to DataFrame
        df = pd.DataFrame([dict(row) for row in historical_data])
        
        if FORECAST_AVAILABLE:
            # Use enhanced forecast
            forecast_results = enhanced_price_forecast(
                df, days, commodity, market, model_type
            )
            model_used = "enhanced"
        else:
            # Fallback to simple forecast
            current_price = df['price'].iloc[0] if len(df) > 0 else 100
            forecast_results = []
            
            for i in range(1, days + 1):
                date = (datetime.now() + timedelta(days=i)).strftime("%Y-%m-%d")
                variation = random.uniform(-0.02, 0.03) * (i/7)
                predicted_price = round(current_price * (1 + variation), 2)
                
                forecast_results.append({
                    "date": date,
                    "predicted_price": predicted_price,
                    "change_percent": round(variation * 100, 2),
                    "model": "simple_fallback",
                    "confidence": "low"
                })
            
            model_used = "simple_fallback"
        
        # Get recommendations
        recommendations = get_forecast_recommendations(commodity, market)
        
        return jsonify({
            "commodity": commodity,
            "market": market,
            "current_price": round(float(df['price'].iloc[0]), 2) if len(df) > 0 else 0,
            "last_updated": df['recorded_at'].iloc[0] if len(df) > 0 else None,
            "forecast_days": days,
            "forecast": forecast_results,
            "model": model_used,
            "data_points": len(df),
            "recommendations": recommendations,
            "generated_at": datetime.now().isoformat(),
            "zambian_context": {
                "season": zambian_data.SEASONAL_CALENDAR.get(datetime.now().month, {}).get("name", "unknown") if hasattr(zambian_data, 'SEASONAL_CALENDAR') else "unknown",
                "price_range": zambian_data.COMMODITY_PRICE_RANGES.get(commodity, {}) if hasattr(zambian_data, 'COMMODITY_PRICE_RANGES') else {}
            }
        })
        
    except Exception as e:
        print(f"Forecast error: {e}")
        return jsonify({
            "error": str(e),
            "commodity": commodity,
            "forecast": [],
            "model": "error"
        }), 500

@app.route("/api/forecast/multi-market", methods=["GET"])
def get_multi_market_forecast():
    """Get forecasts for multiple markets"""
    commodity = request.args.get("commodity", "Maize")
    days = int(request.args.get("days", 7))
    
    if not FORECAST_AVAILABLE:
        return jsonify({"error": "Enhanced forecast module not available"}), 501
    
    try:
        # Get forecasts for major Zambian markets
        markets = ["Lusaka", "Kabwe", "Ndola", "Livingstone"]
        results = {}
        
        for market in markets:
            try:
                forecast = get_market_forecast(commodity, market, days)
                results[market] = forecast
            except Exception as e:
                results[market] = {
                    "error": str(e),
                    "market": market
                }
        
        # Comparative analysis
        comparative = analyze_market_forecasts(results)
        
        return jsonify({
            "commodity": commodity,
            "days": days,
            "markets": results,
            "comparative_analysis": comparative,
            "best_market": comparative.get("best_market"),
            "worst_market": comparative.get("worst_market"),
            "generated_at": datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# =========================================================
# SMS ROUTES
# =========================================================

@app.route("/api/sms/send", methods=["POST"])
@token_required
def send_sms():
    """Send SMS message"""
    user = request.user
    data = request.json
    
    phone = data.get("phone")
    message = data.get("message")
    
    if not phone or not message:
        return jsonify({"error": "Phone and message required"}), 400
    
    # Validate phone number (Zambian format)
    if not phone.startswith('+260'):
        return jsonify({"error": "Zambian phone number required (format: +260XXXXXXXXX)"}), 400
    
    # Check if user has SMS credits or is admin
    if user["role"] != "admin":
        # Rate limiting for non-admin users
        conn = get_db()
        cur = conn.cursor()
        cur.execute('''
            SELECT COUNT(*) as count FROM sms_history 
            WHERE phone=? AND sent_at > datetime('now', '-1 hour')
        ''', (phone,))
        recent_count = cur.fetchone()["count"]
        conn.close()
        
        if recent_count >= 5:  # Max 5 SMS per hour
            return jsonify({"error": "Rate limit exceeded. Max 5 SMS per hour."}), 429
    
    # Send SMS
    result = sms_service.send_sms(phone, message)
    
    log_activity(user["username"], "Send SMS", f"To: {phone}")
    
    return jsonify(result)

@app.route("/api/sms/price-alert", methods=["POST"])
@token_required
def send_price_alert():
    """Send price alert SMS to user"""
    user = request.user
    data = request.json
    
    commodity = data.get("commodity")
    market = data.get("market", user.get("location", "Lusaka"))
    price = data.get("price")
    
    if not commodity or not price:
        return jsonify({"error": "Commodity and price required"}), 400
    
    # Get current price for comparison
    conn = get_db()
    cur = conn.cursor()
    cur.execute('''
        SELECT price FROM market_prices 
        WHERE commodity=? AND market LIKE ? AND verified=1
        ORDER BY recorded_at DESC LIMIT 1
    ''', (commodity, f"%{market}%"))
    
    current_price_data = cur.fetchone()
    conn.close()
    
    current_price = current_price_data["price"] if current_price_data else 0
    
    # Send alert
    result = sms_service.send_price_alert(
        user["user_id"], commodity, market, price
    )
    
    log_activity(user["username"], "Price Alert", f"{commodity} @ {market}: ZMW {price}")
    
    return jsonify({
        **result,
        "current_price": current_price,
        "alert_price": price,
        "difference": round(price - current_price, 2) if current_price else 0
    })

@app.route("/api/sms/daily-summary", methods=["POST"])
@token_required
def send_daily_summary():
    """Send daily market summary SMS"""
    user = request.user
    
    result = sms_service.send_daily_summary(user["user_id"])
    
    log_activity(user["username"], "Daily Summary", "SMS sent")
    
    return jsonify(result)

# =========================================================
# USSD ROUTES
# =========================================================

@app.route("/api/ussd/callback", methods=["POST"])
def ussd_callback():
    """Handle USSD callback from Africa's Talking"""
    try:
        # Parse USSD parameters
        session_id = request.values.get('sessionId')
        phone_number = request.values.get('phoneNumber')
        text = request.values.get('text', '')
        
        print(f"📞 USSD Request: {phone_number} - {text}")
        
        # Handle USSD request
        response = ussd_service.handle_ussd_request(session_id, phone_number, text)
        
        # Log USSD activity
        log_activity(f"USSD:{phone_number}", "USSD Request", f"Text: {text}")
        log_system_metric("ussd_request", 1, f"phone:{phone_number}")
        
        return response, 200, {'Content-Type': 'text/plain'}
        
    except Exception as e:
        print(f"USSD callback error: {e}")
        return "END Service error. Please try again later.", 200, {'Content-Type': 'text/plain'}

@app.route("/api/ussd/register", methods=["POST"])
def ussd_register():
    """Register USSD PIN for user"""
    data = request.json
    phone = data.get("phone")
    pin = data.get("pin")
    
    if not phone or not pin:
        return jsonify({"error": "Phone and PIN required"}), 400
    
    try:
        conn = get_db()
        cur = conn.cursor()
        
        # Find user by phone
        cur.execute("SELECT * FROM users WHERE phone=?", (phone,))
        user = cur.fetchone()
        
        if not user:
            conn.close()
            return jsonify({"error": "User not found with this phone number"}), 404
        
        # Update USSD PIN
        cur.execute("UPDATE users SET ussd_pin=? WHERE phone=?", (pin, phone))
        conn.commit()
        conn.close()
        
        # Send confirmation SMS
        if sms_service.active:
            sms_service.send_sms(phone, f"FarmConnect: Your USSD PIN {pin} is set. Dial *123# for market prices.")
        
        return jsonify({
            "success": True,
            "message": "USSD PIN registered successfully",
            "phone": phone
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# =========================================================
# BUYER ROUTES
# =========================================================

@app.route("/api/buyers", methods=["GET"])
def get_buyers():
    """Get buyer listings"""
    conn = get_db()
    cur = conn.cursor()
    
    commodity = request.args.get("commodity", "all")
    location = request.args.get("location", "all")
    verified_only = request.args.get("verified", "true").lower() == "true"
    min_rating = float(request.args.get("min_rating", 3.0))
    limit = int(request.args.get("limit", 50))
    
    query = """
        SELECT id, name, phone, commodity, location, max_price, min_volume, 
               notes, verified, rating, added_by, created_at, status
        FROM buyers 
        WHERE status = 'active'
    """
    params = []
    
    if commodity != "all":
        query += " AND commodity=?"
        params.append(commodity)
    
    if location != "all":
        query += " AND location=?"
        params.append(location)
    
    if verified_only:
        query += " AND verified=1"
    
    query += " AND rating >= ?"
    params.append(min_rating)
    
    query += " ORDER BY rating DESC, verified DESC LIMIT ?"
    params.append(limit)
    
    cur.execute(query, params)
    buyers = [dict(row) for row in cur.fetchall()]
    
    # Get statistics
    cur.execute("SELECT COUNT(*) as total FROM buyers WHERE status='active'")
    total_buyers = cur.fetchone()["total"]
    
    cur.execute("SELECT COUNT(DISTINCT commodity) as commodities FROM buyers WHERE status='active'")
    unique_commodities = cur.fetchone()["commodities"]
    
    cur.execute("SELECT COUNT(DISTINCT location) as locations FROM buyers WHERE status='active'")
    unique_locations = cur.fetchone()["locations"]
    
    conn.close()
    
    return jsonify({
        "buyers": buyers,
        "statistics": {
            "total": total_buyers,
            "verified": len([b for b in buyers if b["verified"]]),
            "unique_commodities": unique_commodities,
            "unique_locations": unique_locations,
            "returned": len(buyers)
        }
    })

@app.route("/api/buyers/add", methods=["POST"])
@token_required
def add_buyer():
    """Add new buyer"""
    user = request.user
    data = request.json
    
    required = ["name", "phone", "commodity", "location"]
    for field in required:
        if field not in data:
            return jsonify({"error": f"Missing required field: {field}"}), 400
    
    conn = get_db()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            INSERT INTO buyers (name, phone, commodity, location, max_price, min_volume, 
                               notes, rating, verified, added_by, created_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["name"],
            data["phone"],
            data["commodity"],
            data["location"],
            data.get("max_price"),
            data.get("min_volume"),
            data.get("notes", ""),
            data.get("rating", 4.0),
            True if user["role"] == "admin" else False,
            user["username"],
            datetime.now().isoformat(),
            "active"
        ))
        
        conn.commit()
        buyer_id = cur.lastrowid
        conn.close()
        
        log_activity(user["username"], "Added buyer", 
                    f"{data['name']} ({data['commodity']})")
        
        return jsonify({
            "message": "Buyer added successfully" + (" (pending verification)" if user["role"] != "admin" else ""),
            "buyer_id": buyer_id,
            "verified": user["role"] == "admin"
        })
        
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

# =========================================================
# USER MANAGEMENT ROUTES
# =========================================================

@app.route("/api/user/profile", methods=["GET"])
@token_required
def get_user_profile():
    """Get user profile"""
    user = request.user
    
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT user_id, username, name, role, phone, email, location, farm_size, main_crops, 
               business_name, license_number, trading_commodities, created_at, last_login,
               status, sms_alerts, ussd_pin
        FROM users WHERE user_id=?
    """, (user["user_id"],))
    
    profile = cur.fetchone()
    conn.close()
    
    if not profile:
        return jsonify({"error": "Profile not found"}), 404
    
    profile_dict = dict(profile)
    
    # Calculate account stats
    created = datetime.fromisoformat(profile_dict["created_at"].replace('Z', '+00:00'))
    days_active = (datetime.now() - created).days
    
    # Get user activity stats
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM price_alerts WHERE user_id=?", (user["user_id"],))
    price_alerts = cur.fetchone()[0] or 0
    
    cur.execute("SELECT COUNT(*) FROM activity_logs WHERE user=?", (user["username"],))
    total_activities = cur.fetchone()[0] or 0
    
    conn.close()
    
    # Remove sensitive data
    del profile_dict["ussd_pin"]
    if "password_hash" in profile_dict:
        del profile_dict["password_hash"]
    
    return jsonify({
        **profile_dict,
        "account_days": days_active,
        "stats": {
            "price_alerts": price_alerts,
            "total_activities": total_activities,
            "last_login": profile_dict["last_login"]
        }
    })

@app.route("/api/user/update", methods=["POST"])
@token_required
def update_user_profile():
    """Update user profile"""
    user = request.user
    data = request.json
    
    conn = get_db()
    cur = conn.cursor()
    
    try:
        # Build update query
        update_fields = []
        params = []
        
        allowed_fields = ["name", "phone", "email", "location", "farm_size", 
                         "main_crops", "business_name", "license_number", 
                         "trading_commodities", "sms_alerts"]
        
        for field in allowed_fields:
            if field in data:
                update_fields.append(f"{field} = ?")
                params.append(data[field])
        
        if not update_fields:
            conn.close()
            return jsonify({"error": "No fields to update"}), 400
        
        # Add user_id to params
        params.append(user["user_id"])
        
        # Execute update
        query = f"UPDATE users SET {', '.join(update_fields)} WHERE user_id = ?"
        cur.execute(query, params)
        conn.commit()
        
        # Get updated user
        cur.execute("SELECT * FROM users WHERE user_id=?", (user["user_id"],))
        updated_user = cur.fetchone()
        user_dict = dict(updated_user)
        
        conn.close()
        
        log_activity(user["username"], "Update profile", f"Updated {len(update_fields)} fields")
        
        # Remove sensitive data
        if "password_hash" in user_dict:
            del user_dict["password_hash"]
        if "ussd_pin" in user_dict:
            del user_dict["ussd_pin"]
        
        return jsonify({
            "message": "Profile updated successfully",
            "user": user_dict
        })
        
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

# =========================================================
# ADMIN ROUTES
# =========================================================

@app.route("/api/admin/stats", methods=["GET"])
@admin_required
def get_admin_stats():
    """Get admin statistics"""
    try:
        conn = get_db()
        cur = conn.cursor()
        
        # User statistics
        cur.execute("SELECT COUNT(*) as total FROM users")
        total_users = cur.fetchone()["total"]
        
        cur.execute("SELECT COUNT(*) as farmers FROM users WHERE role='farmer'")
        total_farmers = cur.fetchone()["farmers"]
        
        cur.execute("SELECT COUNT(*) as traders FROM users WHERE role='trader'")
        total_traders = cur.fetchone()["traders"]
        
        cur.execute("SELECT COUNT(*) as today FROM users WHERE created_at > date('now', '-1 day')")
        new_today = cur.fetchone()["today"]
        
        # Price statistics
        cur.execute("SELECT COUNT(*) as total FROM market_prices")
        total_prices = cur.fetchone()["total"]
        
        cur.execute("SELECT COUNT(*) as verified FROM market_prices WHERE verified=1")
        verified_prices = cur.fetchone()["verified"]
        
        cur.execute("SELECT COUNT(*) as today FROM market_prices WHERE recorded_at > datetime('now', '-1 day')")
        prices_today = cur.fetchone()["today"]
        
        # SMS statistics
        cur.execute("SELECT COUNT(*) as total FROM sms_history")
        total_sms = cur.fetchone()["total"]
        
        cur.execute("SELECT COUNT(*) as today FROM sms_history WHERE sent_at > datetime('now', '-1 day')")
        sms_today = cur.fetchone()["today"]
        
        # System metrics
        cur.execute("SELECT metric_type, metric_value FROM system_metrics ORDER BY recorded_at DESC LIMIT 10")
        recent_metrics = [dict(row) for row in cur.fetchall()]
        
        # Recent activity
        cur.execute("SELECT * FROM activity_logs ORDER BY created_at DESC LIMIT 20")
        recent_activity = [dict(row) for row in cur.fetchall()]
        
        # Data source status
        cur.execute("SELECT name, enabled, last_updated, success_rate FROM data_sources ORDER BY priority")
        data_sources = [dict(row) for row in cur.fetchall()]
        
        conn.close()
        
        zambian_markets_count = len(zambian_data.ZAMBIAN_MARKETS) if hasattr(zambian_data, 'ZAMBIAN_MARKETS') else 0
        zambian_commodities_count = len(zambian_data.COMMODITY_PRICE_RANGES) if hasattr(zambian_data, 'COMMODITY_PRICE_RANGES') else 0
        
        return jsonify({
            "users": {
                "total": total_users,
                "farmers": total_farmers,
                "traders": total_traders,
                "new_today": new_today,
                "active_sessions": len(ussd_service.sessions) if ussd_service else 0
            },
            "prices": {
                "total": total_prices,
                "verified": verified_prices,
                "verification_rate": f"{(verified_prices/total_prices*100):.1f}%" if total_prices > 0 else "0%",
                "today": prices_today,
                "unique_commodities": zambian_commodities_count,
                "unique_markets": sum(len(region["markets"]) for region in zambian_data.ZAMBIAN_MARKETS.values()) if hasattr(zambian_data, 'ZAMBIAN_MARKETS') else 0
            },
            "sms": {
                "total": total_sms,
                "today": sms_today,
                "service_status": "active" if sms_service.active else "demo"
            },
            "system": {
                "forecast_available": FORECAST_AVAILABLE,
                "ussd_available": AFRICASTALKING_AVAILABLE,
                "scheduler_available": SCHEDULER_AVAILABLE,
                "backup_enabled": backup_manager.s3_enabled,
                "database_size": os.path.getsize(DATABASE) if os.path.exists(DATABASE) else 0,
                "uptime": get_system_uptime()
            },
            "recent_metrics": recent_metrics,
            "recent_activity": recent_activity[:5],
            "data_sources": data_sources,
            "zambian_data": {
                "regions": zambian_markets_count,
                "sources_active": len([s for s in data_sources if s["enabled"]]),
                "last_collection": data_sources[0]["last_updated"] if data_sources else None
            }
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/admin/users", methods=["GET"])
@admin_required
def get_all_users():
    """Get all users (admin only)"""
    conn = get_db()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT user_id, username, name, role, phone, email, location, 
                   created_at, last_login, status, sms_alerts
            FROM users 
            ORDER BY created_at DESC
        """)
        users = [dict(row) for row in cur.fetchall()]
        conn.close()
        
        return jsonify(users)
        
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

@app.route("/api/admin/verify-price", methods=["POST"])
@admin_required
def verify_price():
    """Verify/approve price (admin only)"""
    data = request.json
    price_id = data.get("price_id")
    approve = data.get("approve", True)
    
    if not price_id:
        return jsonify({"error": "Price ID required"}), 400
    
    conn = get_db()
    cur = conn.cursor()
    
    try:
        cur.execute("UPDATE market_prices SET verified=? WHERE id=?", (1 if approve else 0, price_id))
        conn.commit()
        
        # Get price details for logging
        cur.execute("SELECT commodity, price, market FROM market_prices WHERE id=?", (price_id,))
        price = cur.fetchone()
        
        conn.close()
        
        if price:
            log_activity(request.user["username"], "Verify price", 
                        f"{'Approved' if approve else 'Rejected'} {price['commodity']} @ {price['market']}: ZMW {price['price']}")
        
        return jsonify({
            "success": True,
            "message": f"Price {'verified' if approve else 'unverified'} successfully"
        })
        
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

# =========================================================
# BACKUP ROUTES
# =========================================================

@app.route("/api/backup/create", methods=["POST"])
@admin_required
def create_backup():
    """Create system backup (admin only)"""
    try:
        result = backup_manager.create_backup()
        
        if result["success"]:
            log_activity(request.user["username"], "Create backup", 
                        f"Backup: {result['backup_name']}, Size: {result.get('size', 0)} bytes")
            log_system_metric("backup_created", 1, f"size:{result.get('size', 0)}")
            
            return jsonify(result)
        else:
            return jsonify(result), 500
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/backup/list", methods=["GET"])
@admin_required
def list_backups():
    """List available backups (admin only)"""
    try:
        backups = []
        
        if os.path.exists(backup_manager.backup_dir):
            for filename in os.listdir(backup_manager.backup_dir):
                if filename.endswith('.zip'):
                    filepath = os.path.join(backup_manager.backup_dir, filename)
                    stats = os.stat(filepath)
                    
                    backups.append({
                        "name": filename,
                        "path": filepath,
                        "size": stats.st_size,
                        "created": datetime.fromtimestamp(stats.st_mtime).isoformat(),
                        "download_url": f"/api/backup/download/{filename}"
                    })
        
        return jsonify({
            "backups": sorted(backups, key=lambda x: x["created"], reverse=True),
            "backup_dir": backup_manager.backup_dir,
            "max_backups": backup_manager.max_backups,
            "s3_enabled": backup_manager.s3_enabled
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/backup/download/<filename>", methods=["GET"])
@admin_required
def download_backup(filename):
    """Download backup file (admin only)"""
    try:
        filepath = os.path.join(backup_manager.backup_dir, filename)
        
        if not os.path.exists(filepath):
            return jsonify({"error": "Backup file not found"}), 404
        
        log_activity(request.user["username"], "Download backup", f"File: {filename}")
        
        return send_file(filepath, as_attachment=True)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# =========================================================
# SYSTEM ROUTES
# =========================================================

@app.route("/api/status", methods=["GET"])
def status():
    """Check API status with detailed system info"""
    try:
        conn = get_db()
        cur = conn.cursor()
        
        # Database stats
        cur.execute("SELECT COUNT(*) FROM market_prices WHERE verified = 1")
        verified_prices = cur.fetchone()[0] or 0
        
        cur.execute("SELECT COUNT(DISTINCT commodity) FROM market_prices WHERE verified = 1")
        commodities = cur.fetchone()[0] or 0
        
        cur.execute("SELECT COUNT(DISTINCT market) FROM market_prices WHERE verified = 1")
        markets = cur.fetchone()[0] or 0
        
        cur.execute("SELECT COUNT(*) FROM users WHERE status='active'")
        active_users = cur.fetchone()[0] or 0
        
        conn.close()
        
        # System info
        system_info = {
            "platform": sys.platform,
            "python_version": sys.version.split()[0],
            "flask_version": "2.3.3",
            "server_time": datetime.now().isoformat(),
            "uptime": get_system_uptime(),
            "memory_usage": get_memory_usage(),
            "disk_usage": get_disk_usage()
        }
        
        # Service status
        service_status = {
            "database": "online",
            "forecast": "enhanced" if FORECAST_AVAILABLE else "basic",
            "sms": "active" if sms_service.active else "demo",
            "ussd": "active" if AFRICASTALKING_AVAILABLE else "demo",
            "scheduler": "active" if SCHEDULER_AVAILABLE else "inactive",
            "backup": "enabled" if backup_manager.s3_enabled else "local_only",
            "data_collection": "active"
        }
        
        # Zambian data status
        zambian_markets_count = len(zambian_data.ZAMBIAN_MARKETS) if hasattr(zambian_data, 'ZAMBIAN_MARKETS') else 0
        zambian_commodities_count = len(zambian_data.COMMODITY_PRICE_RANGES) if hasattr(zambian_data, 'COMMODITY_PRICE_RANGES') else 0
        
        zambian_status = {
            "verified_prices": verified_prices,
            "commodities_tracked": commodities,
            "markets_monitored": markets,
            "active_users": active_users,
            "regions_covered": zambian_markets_count,
            "last_updated": datetime.now().isoformat()
        }
        
        return jsonify({
            "status": "online",
            "version": "2.0.0",
            "system": system_info,
            "services": service_status,
            "zambian_data": zambian_status,
            "endpoints": {
                "auth": ["/api/login", "/api/register", "/api/logout"],
                "users": ["/api/user/profile", "/api/user/update"],
                "prices": ["/api/prices/real", "/api/data/collect", "/api/data/status"],
                "forecast": ["/api/forecast/real", "/api/forecast/multi-market"],
                "buyers": ["/api/buyers", "/api/buyers/add"],
                "sms": ["/api/sms/send", "/api/sms/price-alert", "/api/sms/daily-summary"],
                "ussd": ["/api/ussd/callback", "/api/ussd/register"],
                "admin": ["/api/admin/stats", "/api/admin/users", "/api/admin/verify-price"],
                "backup": ["/api/backup/create", "/api/backup/list", "/api/backup/download"],
                "system": ["/api/status"]
            }
        })
    except Exception as e:
        return jsonify({
            "status": "online",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        })

def get_system_uptime():
    """Get system uptime (simplified)"""
    try:
        # For demo purposes - in production, use psutil or similar
        return "24/7"
    except:
        return "unknown"

def get_memory_usage():
    """Get memory usage (simplified)"""
    try:
        import psutil
        return f"{psutil.virtual_memory().percent}%"
    except:
        return "unknown"

def get_disk_usage():
    """Get disk usage (simplified)"""
    try:
        import psutil
        return f"{psutil.disk_usage('/').percent}%"
    except:
        return "unknown"

# =========================================================
# SCHEDULED TASKS
# =========================================================

def schedule_tasks():
    """Schedule automatic tasks"""
    
    def daily_data_collection():
        """Daily data collection at 8:00 AM"""
        print("🕒 Running scheduled Zambian data collection...")
        try:
            with app.app_context():
                prices = zambian_data.fetch_all_sources()
                
                saved_count = 0
                conn = get_db()
                cur = conn.cursor()
                
                for price_data in prices:
                    try:
                        cur.execute("""
                            INSERT OR REPLACE INTO market_prices 
                            (market, commodity, price, unit, volume, quality, source, verified, recorded_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            price_data.get("market", "Unknown"),
                            price_data.get("commodity", "Unknown"),
                            price_data.get("price", 0),
                            price_data.get("unit", "ZMW/kg"),
                            price_data.get("volume"),
                            price_data.get("quality"),
                            price_data.get("source", "Zambian_Source"),
                            price_data.get("verified", True),
                            price_data.get("recorded_at", datetime.now().isoformat())
                        ))
                        saved_count += 1
                    except Exception as e:
                        print(f"⚠️  Error saving price: {e}")
                
                conn.commit()
                conn.close()
                
                log_collection(
                    source_name="Scheduled_Zambian_Data",
                    operation="daily_collection",
                    records_collected=saved_count,
                    status="success"
                )
                
                print(f"✅ Scheduled collection saved {saved_count} prices")
                
        except Exception as e:
            print(f"❌ Scheduled collection failed: {e}")
    
    def daily_backup():
        """Daily backup at 2:00 AM"""
        print("💾 Running daily backup...")
        try:
            with app.app_context():
                result = backup_manager.create_backup()
                if result["success"]:
                    print(f"✅ Daily backup created: {result['backup_name']}")
                else:
                    print(f"❌ Daily backup failed: {result.get('error')}")
        except Exception as e:
            print(f"❌ Daily backup failed: {e}")
    
    def send_daily_sms_summaries():
        """Send daily SMS summaries to opted-in users at 7:00 AM"""
        print("📱 Sending daily SMS summaries...")
        try:
            with app.app_context():
                conn = get_db()
                cur = conn.cursor()
                
                # Get users with SMS alerts enabled
                cur.execute("SELECT user_id, phone, name FROM users WHERE sms_alerts=1 AND status='active'")
                users = cur.fetchall()
                
                sent_count = 0
                for user in users:
                    try:
                        result = sms_service.send_daily_summary(user["user_id"])
                        if result.get("success"):
                            sent_count += 1
                    except Exception as e:
                        print(f"⚠️  Error sending SMS to {user['phone']}: {e}")
                
                conn.close()
                print(f"✅ Sent {sent_count} daily SMS summaries")
                
        except Exception as e:
            print(f"❌ Daily SMS summaries failed: {e}")
    
    # Schedule tasks
    schedule.every().day.at("08:00").do(daily_data_collection)
    schedule.every().day.at("02:00").do(daily_backup)
    schedule.every().day.at("07:00").do(send_daily_sms_summaries)
    
    # Hourly price updates (market hours)
    for hour in range(8, 18):
        schedule.every().day.at(f"{hour:02d}:30").do(
            lambda: print(f"⏰ Hourly price check at {hour}:30")
        )
    
    print("✅ Scheduled tasks configured")
    
    # Run scheduler in background thread
    def run_scheduler():
        while True:
            schedule.run_pending()
            time.sleep(60)
    
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()

# =========================================================
# ERROR HANDLING
# =========================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

@app.errorhandler(401)
def unauthorized(error):
    return jsonify({"error": "Unauthorized access"}), 401

@app.errorhandler(403)
def forbidden(error):
    return jsonify({"error": "Access forbidden"}), 403

# =========================================================
# STARTUP FUNCTIONS
# =========================================================

def open_browser():
    """Open the frontend in browser after server starts"""
    time.sleep(2)  # Wait for server to start
    
    print(f"✅ Opening frontend: http://127.0.0.1:5000/")
    
    # Try to open browser
    try:
        import webbrowser
        webbrowser.open('http://127.0.0.1:5000/')
    except:
        print("⚠️  Could not open browser automatically")

def print_startup_banner():
    """Print startup banner"""
    print("=" * 70)
    print("🌾 FARM CONNECT - CLOUD MARKET PLATFORM v2.0")
    print("🏫 Mulungushi University - ICT 431 Capstone Project")
    print("👨‍🎓 Student: Daka Felix (202206453)")
    print("=" * 70)
    print("🚀 Starting enhanced backend server...")
    print(f"📁 Database: {DATABASE}")
    print(f"📁 Frontend directory: {FRONTEND_DIR}")
    print(f"📁 Backup directory: {backup_manager.backup_dir}")
    print("🌐 API: http://127.0.0.1:5000")
    print("🖥️  Frontend: http://127.0.0.1:5000")
    print("=" * 70)
    print("🇿🇲 ZAMBIAN DATA INTEGRATION:")
    zambian_markets_count = len(zambian_data.ZAMBIAN_MARKETS) if hasattr(zambian_data, 'ZAMBIAN_MARKETS') else 0
    zambian_commodities_count = len(zambian_data.COMMODITY_PRICE_RANGES) if hasattr(zambian_data, 'COMMODITY_PRICE_RANGES') else 0
    zambian_total_markets = sum(len(region["markets"]) for region in zambian_data.ZAMBIAN_MARKETS.values()) if hasattr(zambian_data, 'ZAMBIAN_MARKETS') else 0
    print(f"   • {zambian_markets_count} regions")
    print(f"   • {zambian_commodities_count} commodities")
    print(f"   • {zambian_total_markets} markets")
    print("=" * 70)
    print("🔑 DEMO LOGIN CREDENTIALS:")
    print("   Farmer: farmer1 / farmer123")
    print("   Trader: trader1 / trader123")
    print("   Admin:  admin1 / admin123")
    print("=" * 70)
    print("📱 ACCESS METHODS:")
    print("   Web: http://127.0.0.1:5000")
    print("   USSD: *123# (demo mode)")
    print("   SMS: PRICE to 45678 (demo)")
    print("=" * 70)
    print("⚙️  ENHANCED FEATURES:")
    print("   ✓ Real Zambian market data collection")
    print("   ✓ AI-powered price forecasting")
    print("   ✓ USSD/SMS for basic phones")
    print("   ✓ Automated backups")
    print("   ✓ Multi-market comparison")
    print("   ✓ Price alert system")
    print("   ✓ Buyer network")
    print("   ✓ Admin dashboard")
    print("   ✓ Automated data scheduler")
    print("=" * 70)
    print("📊 SERVICE STATUS:")
    print(f"   Database: {'✅ Ready' if os.path.exists(DATABASE) else '⚠️ Not found'}")
    print(f"   Forecast: {'✅ Enhanced' if FORECAST_AVAILABLE else '⚠️ Basic'}")
    print(f"   SMS: {'⚠️ Demo mode' if not sms_service.active else '✅ Active'}")
    print(f"   USSD: {'⚠️ Demo mode' if not AFRICASTALKING_AVAILABLE else '✅ Active'}")
    print(f"   Backup: {'⚠️ Local only' if not backup_manager.s3_enabled else '✅ AWS S3'}")
    
    # Add scheduler status
    if SCHEDULER_AVAILABLE and data_scheduler:
        print(f"   Scheduler: {'✅ Ready'}")
    else:
        print(f"   Scheduler: {'⚠️ Not available'}")
    
    print("=" * 70)
    
    # Start the data scheduler
    if SCHEDULER_AVAILABLE and data_scheduler:
        try:
            data_scheduler.start_scheduler()
            print("✅ Data scheduler started automatically")
        except Exception as e:
            print(f"⚠️  Could not start data scheduler: {e}")

# =========================================================
# RUN APPLICATION
# =========================================================

if __name__ == "__main__":
    # Print startup banner
    print_startup_banner()
    
    # Start scheduled tasks
    try:
        schedule_tasks()
        print("✅ Scheduled tasks started")
    except Exception as e:
        print(f"⚠️  Could not start scheduler: {e}")
    
    # Start browser opening in a separate thread
    threading.Thread(target=open_browser, daemon=True).start()

    # Run Flask app
    try:
        app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)
    except KeyboardInterrupt:
        print("\n👋 Shutting down server...")
        # Stop the data scheduler
        if SCHEDULER_AVAILABLE and data_scheduler:
            data_scheduler.stop_scheduler()
            print("✅ Data scheduler stopped")
        
        # Perform cleanup
        if backup_manager:
            print("💾 Creating final backup before shutdown...")
            backup_manager.create_backup()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
