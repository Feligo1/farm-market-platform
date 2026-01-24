# =========================================================
# forecast.py - ENHANCED PRICE FORECASTING MODULE
# Compatible with app.py and project requirements
# =========================================================

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json
import sqlite3
import pickle
import os
import warnings
import hashlib
import joblib
from typing import Dict, List, Optional, Tuple, Union

warnings.filterwarnings('ignore')

# Machine Learning Imports
try:
    from sklearn.linear_model import LinearRegression, Ridge, Lasso
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import cross_val_score
    from sklearn.metrics import mean_absolute_error, mean_squared_error
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    print("⚠️  scikit-learn not available. Using simplified models.")

# =========================================================
# CONFIGURATION CLASSES - Compatible with app.py
# =========================================================

class ForecastConfig:
    """Configuration for forecasting - compatible with app.py"""
    
    def __init__(self):
        self.model_type = "ensemble"
        self.confidence_threshold = 0.7
    
    @staticmethod
    def get_commodity_config(commodity: str) -> dict:
        """Get configuration for specific commodity"""
        commodity_settings = {
            "Maize": {
                "model": "ensemble",
                "seasonality": 7,
                "volatility": 0.08,
                "default_price": 120.50,
                "harvest_effect": -0.15,
                "lean_effect": 0.20,
            },
            "Tomatoes": {
                "model": "random_forest",
                "seasonality": 7,
                "volatility": 0.25,
                "default_price": 85.25,
                "rainy_effect": 0.30,
                "dry_effect": -0.15,
            },
            "Beans": {
                "model": "linear",
                "seasonality": 30,
                "volatility": 0.10,
                "default_price": 175.30,
                "export_effect": 0.25,
            },
            "Rice": {
                "model": "ensemble",
                "seasonality": 30,
                "volatility": 0.05,
                "default_price": 200.00,
                "import_effect": 0.10,
            },
            "Groundnuts": {
                "model": "ensemble",
                "seasonality": 14,
                "volatility": 0.12,
                "default_price": 190.00,
                "export_premium": 0.30,
            },
        }
        return commodity_settings.get(commodity, {
            "model": "ensemble",
            "seasonality": 7,
            "volatility": 0.10,
            "default_price": 100.00,
        })
    
    @staticmethod
    def get_market_factor(market: str) -> float:
        """Get market location adjustment factor"""
        market_factors = {
            "Lusaka": 1.05,
            "Kabwe": 1.02,
            "Ndola": 1.03,
            "Livingstone": 1.00,
            "Copperbelt": 1.03,
            "Southern": 1.00,
            "Eastern": 0.98,
            "Central": 0.96,
            "Northern": 0.97,
        }
        return market_factors.get(market, 1.0)
    
    @staticmethod
    def get_seasonal_factor(month: int, commodity: str) -> float:
        """Get seasonal adjustment factor"""
        if commodity == "Maize":
            if month in [4, 5, 6]:  # Harvest season
                return 0.92
            elif month in [8, 9, 10]:  # Lean season
                return 1.15
        elif commodity == "Tomatoes":
            if month in [10, 11, 12, 1]:  # Rainy season
                return 1.20
            else:
                return 0.90
        return 1.0

# =========================================================
# MODEL MANAGER CLASS
# =========================================================

class ModelManager:
    """Manage forecasting models"""
    
    def __init__(self):
        self.models = {}
        self.model_dir = "models"
        
        # Create model directory if it doesn't exist
        if not os.path.exists(self.model_dir):
            os.makedirs(self.model_dir)
    
    def get_model(self, commodity, market):
        """Get or create model for commodity-market pair"""
        model_key = f"{commodity}_{market}"
        
        if model_key not in self.models:
            # Try to load from disk
            model_path = os.path.join(self.model_dir, f"{model_key}.pkl")
            if os.path.exists(model_path):
                try:
                    self.models[model_key] = joblib.load(model_path)
                except:
                    self.models[model_key] = self._create_new_model()
            else:
                self.models[model_key] = self._create_new_model()
        
        return self.models[model_key]
    
    def _create_new_model(self):
        """Create a new model instance"""
        return LinearRegression()
    
    def save_models(self):
        """Save all models to disk"""
        for model_key, model in self.models.items():
            model_path = os.path.join(self.model_dir, f"{model_key}.pkl")
            joblib.dump(model, model_path)

# =========================================================
# CORE FORECASTING FUNCTIONS
# =========================================================

def enhanced_price_forecast(df, days=7, commodity="Maize", market="Lusaka", model_type="auto"):
    """
    Enhanced price forecasting with Zambian market context
    Compatible with app.py requirements
    """
    if df.empty or len(df) < 2:
        current_price = 100.00
        return fallback_forecast_with_context(current_price, commodity, market, days)
    
    try:
        # Ensure we have price data
        if 'price' not in df.columns:
            raise ValueError("DataFrame must contain 'price' column")
        
        # Get current price
        current_price = float(df['price'].iloc[-1])
        
        # Get commodity configuration
        config = ForecastConfig.get_commodity_config(commodity)
        
        if model_type == "auto":
            model_type = config["model"]
        
        # Prepare features
        features = prepare_features(df, days)
        
        # Generate forecast based on model type
        if model_type == "ensemble" and len(df) >= 10:
            forecast_results = ensemble_forecast(df, current_price, days, commodity, market)
        elif model_type == "random_forest" and len(df) >= 10:
            forecast_results = random_forest_forecast(df, current_price, days, commodity, market)
        elif model_type == "linear" and len(df) >= 5:
            forecast_results = linear_regression_forecast(df, current_price, days, commodity, market)
        else:
            forecast_results = fallback_forecast_with_context(current_price, commodity, market, days)
        
        # Add metadata
        for i, result in enumerate(forecast_results):
            result.update({
                "commodity": commodity,
                "market": market,
                "model": model_type,
                "confidence": "medium" if len(df) >= 20 else "low",
                "data_points": len(df)
            })
        
        return forecast_results
        
    except Exception as e:
        print(f"Enhanced forecast error: {e}")
        current_price = float(df['price'].iloc[-1]) if not df.empty and 'price' in df.columns else 100.00
        return fallback_forecast_with_context(current_price, commodity, market, days)

def prepare_features(df, forecast_days):
    """Prepare features for forecasting"""
    features = {
        "current_price": float(df['price'].iloc[-1]) if not df.empty and 'price' in df.columns else 100.00,
        "trend": 0.0,
        "volatility": 0.1,
        "seasonal_factor": 1.0,
        "market_factor": 1.0,
        "days_to_forecast": forecast_days
    }
    
    # Calculate trend if we have enough data
    if len(df) >= 3 and 'price' in df.columns:
        recent_prices = df['price'].tail(3).astype(float).values
        if len(recent_prices) >= 2:
            features["trend"] = (recent_prices[-1] - recent_prices[0]) / recent_prices[0]
    
    # Calculate volatility
    if len(df) >= 7 and 'price' in df.columns:
        prices = df['price'].tail(7).astype(float).values
        features["volatility"] = np.std(prices) / np.mean(prices) if np.mean(prices) > 0 else 0.1
    
    return features

def ensemble_forecast(df, current_price, days, commodity, market):
    """Ensemble forecasting combining multiple approaches"""
    forecasts = []
    
    # Linear trend forecast
    linear_predictions = linear_trend_forecast(df, current_price, days, commodity, market)
    if linear_predictions:
        forecasts.append([p["predicted_price"] for p in linear_predictions])
    
    # Seasonal forecast
    seasonal_predictions = seasonal_forecast(df, current_price, days, commodity, market)
    if seasonal_predictions:
        forecasts.append([p["predicted_price"] for p in seasonal_predictions])
    
    # Market-aware forecast
    market_predictions = market_aware_forecast(df, current_price, days, commodity, market)
    if market_predictions:
        forecasts.append([p["predicted_price"] for p in market_predictions])
    
    if not forecasts:
        return fallback_forecast_with_context(current_price, commodity, market, days)
    
    # Average the forecasts
    results = []
    for i in range(days):
        day_predictions = []
        for forecast in forecasts:
            if i < len(forecast):
                day_predictions.append(forecast[i])
        
        if day_predictions:
            avg_price = np.mean(day_predictions)
        else:
            avg_price = current_price
        
        # Apply Zambian market factors
        config = ForecastConfig.get_commodity_config(commodity)
        future_date = datetime.now() + timedelta(days=i+1)
        seasonal_factor = ForecastConfig.get_seasonal_factor(future_date.month, commodity)
        market_factor = ForecastConfig.get_market_factor(market)
        
        # Add small random variation
        variation = random.uniform(-config["volatility"]/2, config["volatility"]/2)
        final_price = avg_price * seasonal_factor * market_factor * (1 + variation)
        
        results.append({
            "date": future_date.strftime("%Y-%m-%d"),
            "predicted_price": round(final_price, 2),
            "change_percent": round(((final_price - current_price) / current_price * 100), 2) if current_price > 0 else 0,
            "trend": "up" if final_price > current_price else "down" if final_price < current_price else "stable"
        })
    
    return results

def linear_trend_forecast(df, current_price, days, commodity, market):
    """Linear trend-based forecast"""
    if len(df) < 3:
        return None
    
    try:
        # Simple linear extrapolation
        prices = df['price'].astype(float).values
        x = np.arange(len(prices)).reshape(-1, 1)
        y = prices
        
        model = LinearRegression()
        model.fit(x, y)
        
        results = []
        for i in range(days):
            future_date = datetime.now() + timedelta(days=i+1)
            future_x = len(prices) + i
            predicted_price = model.predict([[future_x]])[0]
            
            # Apply market factors
            seasonal_factor = ForecastConfig.get_seasonal_factor(future_date.month, commodity)
            market_factor = ForecastConfig.get_market_factor(market)
            predicted_price = predicted_price * seasonal_factor * market_factor
            
            results.append({
                "date": future_date.strftime("%Y-%m-%d"),
                "predicted_price": round(float(predicted_price), 2),
                "change_percent": round(((predicted_price - current_price) / current_price * 100), 2) if current_price > 0 else 0,
                "trend": "up" if predicted_price > current_price else "down" if predicted_price < current_price else "stable"
            })
        
        return results
    except Exception as e:
        print(f"Linear trend forecast error: {e}")
        return None

def seasonal_forecast(df, current_price, days, commodity, market):
    """Seasonal-aware forecast"""
    config = ForecastConfig.get_commodity_config(commodity)
    
    results = []
    for i in range(days):
        future_date = datetime.now() + timedelta(days=i+1)
        
        # Base seasonal pattern
        seasonal_factor = ForecastConfig.get_seasonal_factor(future_date.month, commodity)
        
        # Day of week effect (market days typically have higher prices)
        day_of_week = future_date.weekday()
        day_factor = 1.02 if day_of_week in [2, 5] else 1.0  # Wednesday, Saturday
        
        # Market factor
        market_factor = ForecastConfig.get_market_factor(market)
        
        # Calculate predicted price
        predicted_price = current_price * seasonal_factor * day_factor * market_factor
        
        # Add small trend based on historical data
        if len(df) >= 7:
            recent_trend = calculate_recent_trend(df)
            trend_factor = 1 + (0.001 * (i+1) * recent_trend)
            predicted_price = predicted_price * trend_factor
        
        results.append({
            "date": future_date.strftime("%Y-%m-%d"),
            "predicted_price": round(predicted_price, 2),
            "change_percent": round(((predicted_price - current_price) / current_price * 100), 2) if current_price > 0 else 0,
            "trend": "up" if predicted_price > current_price else "down" if predicted_price < current_price else "stable"
        })
    
    return results

def market_aware_forecast(df, current_price, days, commodity, market):
    """Market-aware forecast considering Zambian market dynamics"""
    config = ForecastConfig.get_commodity_config(commodity)
    
    results = []
    base_price = config["default_price"]
    market_factor = ForecastConfig.get_market_factor(market)
    
    for i in range(days):
        future_date = datetime.now() + timedelta(days=i+1)
        
        # Start from commodity's default price adjusted for market
        predicted_price = base_price * market_factor
        
        # Apply seasonal adjustment
        seasonal_factor = ForecastConfig.get_seasonal_factor(future_date.month, commodity)
        predicted_price = predicted_price * seasonal_factor
        
        # Blend with current price (weighted average)
        if not df.empty:
            # Give more weight to current price (70%) than default price (30%)
            predicted_price = (0.7 * current_price) + (0.3 * predicted_price)
        
        # Add small random variation
        variation = random.uniform(-config["volatility"]/2, config["volatility"]/2)
        predicted_price = predicted_price * (1 + variation)
        
        # Add small upward trend over forecast period
        trend_factor = 1 + (0.001 * (i+1))
        predicted_price = predicted_price * trend_factor
        
        results.append({
            "date": future_date.strftime("%Y-%m-%d"),
            "predicted_price": round(predicted_price, 2),
            "change_percent": round(((predicted_price - current_price) / current_price * 100), 2) if current_price > 0 else 0,
            "trend": "up" if predicted_price > current_price else "down" if predicted_price < current_price else "stable"
        })
    
    return results

def random_forest_forecast(df, current_price, days, commodity, market):
    """Random Forest forecast (simplified for compatibility)"""
    # For compatibility with app.py, we'll use the ensemble approach
    return ensemble_forecast(df, current_price, days, commodity, market)

def linear_regression_forecast(df, current_price, days, commodity, market):
    """Linear regression forecast"""
    if len(df) < 5:
        return fallback_forecast_with_context(current_price, commodity, market, days)
    
    try:
        # Simple linear regression on recent data
        prices = df['price'].astype(float).values
        
        # Use simple moving average for trend
        if len(prices) >= 3:
            ma_3 = np.mean(prices[-3:])
            ma_5 = np.mean(prices[-5:]) if len(prices) >= 5 else ma_3
            trend = (ma_3 - ma_5) / ma_5 if ma_5 > 0 else 0
        else:
            trend = 0
        
        results = []
        for i in range(days):
            future_date = datetime.now() + timedelta(days=i+1)
            
            # Extrapolate with trend
            predicted_price = current_price * (1 + trend * (i+1))
            
            # Apply market factors
            seasonal_factor = ForecastConfig.get_seasonal_factor(future_date.month, commodity)
            market_factor = ForecastConfig.get_market_factor(market)
            predicted_price = predicted_price * seasonal_factor * market_factor
            
            # Add small random component
            config = ForecastConfig.get_commodity_config(commodity)
            variation = random.uniform(-config["volatility"]/4, config["volatility"]/4)
            predicted_price = predicted_price * (1 + variation)
            
            results.append({
                "date": future_date.strftime("%Y-%m-%d"),
                "predicted_price": round(predicted_price, 2),
                "change_percent": round(((predicted_price - current_price) / current_price * 100), 2) if current_price > 0 else 0,
                "trend": "up" if predicted_price > current_price else "down" if predicted_price < current_price else "stable"
            })
        
        return results
        
    except Exception as e:
        print(f"Linear regression forecast error: {e}")
        return fallback_forecast_with_context(current_price, commodity, market, days)

def fallback_forecast_with_context(current_price, commodity, market, days=7):
    """Fallback forecast with Zambian market context when data is insufficient"""
    config = ForecastConfig.get_commodity_config(commodity)
    
    results = []
    for i in range(days):
        future_date = datetime.now() + timedelta(days=i+1)
        
        # Get Zambian market factors
        seasonal_factor = ForecastConfig.get_seasonal_factor(future_date.month, commodity)
        market_factor = ForecastConfig.get_market_factor(market)
        
        # Day of week effect
        day_of_week = future_date.weekday()
        day_factor = 1.02 if day_of_week in [2, 5] else 1.0
        
        # Calculate base price (weighted average of current and default)
        default_price = config["default_price"]
        if current_price > 0:
            base_price = (0.6 * current_price) + (0.4 * default_price)
        else:
            base_price = default_price
        
        # Apply factors
        predicted_price = base_price * seasonal_factor * market_factor * day_factor
        
        # Add variation based on commodity volatility
        variation = random.uniform(-config["volatility"], config["volatility"])
        predicted_price = predicted_price * (1 + variation)
        
        # Add small trend
        trend_factor = 1 + (0.0005 * (i+1))
        predicted_price = predicted_price * trend_factor
        
        results.append({
            "date": future_date.strftime("%Y-%m-%d"),
            "predicted_price": round(predicted_price, 2),
            "change_percent": round(((predicted_price - current_price) / current_price * 100), 2) if current_price > 0 else 0,
            "trend": "up" if predicted_price > current_price else "down" if predicted_price < current_price else "stable",
            "model": "fallback_with_context",
            "confidence": "low",
            "zambian_context": {
                "seasonal_factor": round(seasonal_factor, 3),
                "market_factor": round(market_factor, 3),
                "day_factor": round(day_factor, 3),
                "market_day": "yes" if day_of_week in [2, 5] else "no"
            }
        })
    
    return results

def calculate_recent_trend(df):
    """Calculate recent price trend"""
    if len(df) < 7:
        return 0
    
    prices = df['price'].tail(7).astype(float).values
    if len(prices) < 2:
        return 0
    
    # Simple percentage change over last week
    return (prices[-1] - prices[0]) / prices[0] if prices[0] > 0 else 0

# =========================================================
# API COMPATIBILITY FUNCTIONS (Required by app.py)
# =========================================================

def get_market_forecast(commodity, market, days):
    """
    Get forecast for specific market - Required by app.py
    """
    try:
        # Load data from database
        df = load_commodity_data_from_db(commodity, market)
        
        if df.empty or len(df) < 2:
            # Fallback to context-aware forecast
            config = ForecastConfig.get_commodity_config(commodity)
            default_price = config["default_price"]
            current_price = default_price * ForecastConfig.get_market_factor(market)
            return fallback_forecast_with_context(current_price, commodity, market, days)
        
        # Generate enhanced forecast
        return enhanced_price_forecast(df, days, commodity, market)
    
    except Exception as e:
        print(f"Market forecast error for {commodity} in {market}: {e}")
        config = ForecastConfig.get_commodity_config(commodity)
        default_price = config["default_price"]
        current_price = default_price * ForecastConfig.get_market_factor(market)
        return fallback_forecast_with_context(current_price, commodity, market, days)

def get_all_markets_forecast(commodity, days):
    """
    Get forecasts for all markets - Required by app.py
    """
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
    
    return results

def get_forecast_recommendations(commodity="Maize", market="Lusaka"):
    """
    Get trading recommendations - Required by app.py
    """
    try:
        # Get forecast for the market
        forecast = get_market_forecast(commodity, market, 7)
        
        if isinstance(forecast, list) and len(forecast) > 0:
            # Calculate average change
            changes = [day.get("change_percent", 0) for day in forecast]
            avg_change = np.mean(changes) if changes else 0
            
            # Generate recommendation based on average change
            if avg_change > 8:
                return {
                    "buy_sell": "Hold/Buy",
                    "timing": "Within 3 days",
                    "reason": "Strong upward trend expected",
                    "confidence": "high",
                    "action": "Consider holding or buying for short-term gain",
                    "market_advice": f"Check {market} prices daily"
                }
            elif avg_change > 3:
                return {
                    "buy_sell": "Consider Selling",
                    "timing": "In 2-3 days",
                    "reason": "Moderate upward trend",
                    "confidence": "medium",
                    "action": "Good selling opportunity expected soon",
                    "market_advice": "Monitor prices closely"
                }
            elif avg_change < -8:
                return {
                    "buy_sell": "Sell",
                    "timing": "Immediately",
                    "reason": "Strong downward trend",
                    "confidence": "high",
                    "action": "Consider selling to minimize losses",
                    "market_advice": "Check other markets for better prices"
                }
            elif avg_change < -3:
                return {
                    "buy_sell": "Consider Selling",
                    "timing": "Within 1-2 days",
                    "reason": "Moderate downward trend",
                    "confidence": "medium",
                    "action": "Prices may continue to fall",
                    "market_advice": "Consider storing if possible"
                }
        
        # Default recommendation for stable prices
        return {
            "buy_sell": "Hold",
            "timing": "No immediate action",
            "reason": "Prices expected to remain stable",
            "confidence": "medium",
            "action": "Continue normal trading operations",
            "market_advice": f"Monitor {market} for any changes"
        }
    
    except Exception as e:
        print(f"Recommendations error: {e}")
        # Fallback recommendation
        return {
            "buy_sell": "Monitor",
            "timing": "Check daily",
            "reason": "Insufficient data for analysis",
            "confidence": "low",
            "action": "Check market prices regularly",
            "market_advice": "Gather more price data for better predictions"
        }

def analyze_market_forecasts(results):
    """
    Analyze multiple market forecasts - Required by app.py
    """
    if not results:
        return {
            "best_market": "Unknown",
            "worst_market": "Unknown",
            "price_range": "Varies",
            "recommendation": "Check market data availability"
        }
    
    try:
        best_market = None
        best_avg_price = 0
        worst_market = None
        worst_avg_price = float('inf')
        all_prices = []
        
        for market, forecast in results.items():
            if isinstance(forecast, list) and len(forecast) > 0:
                # Calculate average predicted price for this market
                prices = [day.get("predicted_price", 0) for day in forecast if isinstance(day, dict)]
                if prices:
                    avg_price = np.mean(prices)
                    all_prices.append(avg_price)
                    
                    if avg_price > best_avg_price:
                        best_avg_price = avg_price
                        best_market = market
                    
                    if avg_price < worst_avg_price:
                        worst_avg_price = avg_price
                        worst_market = market
        
        if all_prices:
            price_range = f"ZMW {min(all_prices):.2f} - {max(all_prices):.2f}"
        else:
            price_range = "No price data"
        
        return {
            "best_market": best_market or "Unknown",
            "worst_market": worst_market or "Unknown",
            "price_range": price_range,
            "recommendation": f"Consider selling in {best_market} for best prices" if best_market != "Unknown" else "Check all markets"
        }
    
    except Exception as e:
        print(f"Market analysis error: {e}")
        return {
            "best_market": "Unknown",
            "worst_market": "Unknown",
            "price_range": "Error in analysis",
            "recommendation": "Unable to analyze market forecasts"
        }

# =========================================================
# DATABASE FUNCTIONS
# =========================================================

def load_commodity_data_from_db(commodity, market=None, days_back=90):
    """Load commodity data from database"""
    try:
        conn = sqlite3.connect("farm_market.db")
        
        if market:
            query = """
                SELECT price, recorded_at
                FROM market_prices 
                WHERE commodity = ? 
                    AND market LIKE ?
                    AND verified = 1
                ORDER BY recorded_at DESC
                LIMIT ?
            """
            params = [commodity, f"%{market}%", days_back]
        else:
            query = """
                SELECT price, recorded_at
                FROM market_prices 
                WHERE commodity = ? 
                    AND verified = 1
                ORDER BY recorded_at DESC
                LIMIT ?
            """
            params = [commodity, days_back]
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        return df
        
    except Exception as e:
        print(f"Error loading data for {commodity}: {e}")
        return pd.DataFrame()

# =========================================================
# INITIALIZE MODEL MANAGER
# =========================================================

model_manager = ModelManager()

# =========================================================
# TEST FUNCTION
# =========================================================

def test_forecast_system():
    """Test the forecasting system"""
    print("=" * 70)
    print("🌾 FORECASTING SYSTEM TEST")
    print("=" * 70)
    
    # Create sample data
    dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
    base_price = 120.50
    prices = [base_price * (1 + random.uniform(-0.05, 0.05)) for _ in range(30)]
    
    sample_df = pd.DataFrame({
        'price': prices,
        'recorded_at': dates
    })
    
    commodity = "Maize"
    market = "Lusaka"
    
    print(f"\n📊 Testing forecasts for {commodity} in {market}")
    print("-" * 50)
    
    # Test enhanced forecast
    print("1. Enhanced Forecast (7 days):")
    forecast = enhanced_price_forecast(sample_df, days=7, commodity=commodity, market=market)
    for day in forecast[:3]:  # Show first 3 days
        print(f"   {day['date']}: ZMW {day['predicted_price']:.2f} ({day.get('change_percent', 0):+.1f}%) - {day.get('trend', 'stable')}")
    
    # Test market forecast
    print("\n2. Market Forecast:")
    market_forecast = get_market_forecast(commodity, market, 5)
    if isinstance(market_forecast, list):
        for day in market_forecast[:2]:
            print(f"   {day['date']}: ZMW {day['predicted_price']:.2f}")
    else:
        print(f"   Error: {market_forecast}")
    
    # Test recommendations
    print("\n3. Trading Recommendations:")
    recommendations = get_forecast_recommendations(commodity, market)
    print(f"   Action: {recommendations.get('buy_sell', 'N/A')}")
    print(f"   Reason: {recommendations.get('reason', 'N/A')}")
    print(f"   Confidence: {recommendations.get('confidence', 'N/A')}")
    
    # Test multiple markets
    print("\n4. Multiple Market Analysis:")
    all_forecasts = get_all_markets_forecast(commodity, 3)
    analysis = analyze_market_forecasts(all_forecasts)
    print(f"   Best Market: {analysis.get('best_market', 'N/A')}")
    print(f"   Price Range: {analysis.get('price_range', 'N/A')}")
    
    print("\n✅ Forecasting system test completed!")
    print("=" * 70)
    
    return True

# =========================================================
# MAIN EXECUTION
# =========================================================

if __name__ == "__main__":
    test_forecast_system()