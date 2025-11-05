import pandas as pd

def simple_price_forecast(df, days_ahead=3):
    """
    Simple forecasting function using moving averages.
    Input:
        df - DataFrame with columns ['recorded_at', 'price']
        days_ahead - how many days ahead to forecast
    Output:
        list of dict predictions
    """
    if df.empty:
        return []

    # Ensure data sorted by date
    df = df.sort_values("recorded_at")

    # Compute moving average (last 3 values)
    df["rolling_avg"] = df["price"].rolling(window=3, min_periods=1).mean()

    # Get last known date and price
    last_date = pd.to_datetime(df["recorded_at"].iloc[-1])
    last_avg = df["rolling_avg"].iloc[-1]

    # Generate forecast dates and prices
    forecast_data = []
    for i in range(1, days_ahead + 1):
        next_date = last_date + pd.Timedelta(days=i)
        predicted_price = round(last_avg * (1 + 0.01 * (i - 1)), 2)  # small trend up
        forecast_data.append({
            "date": next_date.strftime("%Y-%m-%d"),
            "predicted_price": predicted_price
        })

    return forecast_data


if __name__ == "__main__":
    # Test example (you can run this file independently)
    sample_data = {
        "recorded_at": pd.date_range("2025-10-25", periods=5, freq="D"),
        "price": [5.2, 5.3, 5.4, 5.5, 5.6]
    }
    df = pd.DataFrame(sample_data)
    print(simple_price_forecast(df, days_ahead=3))
