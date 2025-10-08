import pandas as pd
from datetime import datetime, timedelta
from openalgo import api, ta

class IndicatorFactory:
    def __init__(self, client, symbol="NIFTY", exchange="NSE_INDEX", interval="5m", lookback_days=10):
        self.client = client
        self.symbol = symbol
        self.exchange = exchange
        self.interval = interval
        self.lookback_days = lookback_days
        self.df = None
        self.df_daily_cpr = None

    # ======================
    # Fetch Historical Data
    # ======================
    def fetch_history(self):
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.lookback_days)

        self.df = self.client.history(
            symbol=self.symbol,
            exchange=self.exchange,
            interval=self.interval,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d")
        )
        if not isinstance(self.df.index, pd.DatetimeIndex):
            self.df.index = pd.to_datetime(self.df.index)

        # Daily data for CPR
        self.df_daily_cpr = self.client.history(
            symbol=self.symbol,
            exchange=self.exchange,
            interval="D",
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d")
        )
        if not isinstance(self.df_daily_cpr.index, pd.DatetimeIndex):
            self.df_daily_cpr.index = pd.to_datetime(self.df_daily_cpr.index)

        return self.df

    # ======================
    # Trend Indicators
    # ======================
    def add_trend_indicators(self, periods=[9,20,50,200]):
        for p in periods:
            self.df[f"EMA_{p}"] = ta.ema(self.df["close"], p)
            self.df[f"SMA_{p}"] = ta.sma(self.df["close"], p)
        return self.df

    # ======================
    # Momentum Indicators
    # ======================
    def add_momentum_indicators(self, periods=[14,21]):
        for p in periods:
            self.df[f"RSI_{p}"] = ta.rsi(self.df["close"], p)
        return self.df

    # ======================
    # Previous Candle Indicator
    # ======================
    def add_previous_candle(self):
        """Adds previous candle open, high, low, close to intraday df"""
        self.df["prev_candle_open"] = self.df["open"].shift(1)
        self.df["prev_candle_high"] = self.df["high"].shift(1)
        self.df["prev_candle_low"] = self.df["low"].shift(1)
        self.df["prev_candle_close"] = self.df["close"].shift(1)

        return self.df

    # ======================
    # Volatility Indicators
    # ======================
    def add_bollinger_bands(self, period=20, std_dev=2):
        upper, middle, lower = ta.bbands(self.df["close"], period=period, std_dev=std_dev)
        self.df["BB_Upper"], self.df["BB_Middle"], self.df["BB_Lower"] = upper, middle, lower
        return self.df

    # ======================
    # Support / Resistance (CPR)
    # ======================
    def compute_cpr(self):
        df_daily_cpr = self.df_daily_cpr.copy()
        df_daily_cpr["prev_day_high"] = df_daily_cpr["high"].shift(1)
        df_daily_cpr["prev_day_low"] = df_daily_cpr["low"].shift(1)
        df_daily_cpr["prev_day_close"] = df_daily_cpr["close"].shift(1)

        df_daily_cpr["Pivot"] = (df_daily_cpr["prev_day_high"] + df_daily_cpr["prev_day_low"] + df_daily_cpr["prev_day_close"]) / 3.0
        df_daily_cpr["TC"] = (df_daily_cpr["prev_day_high"] + df_daily_cpr["prev_day_low"]) / 2.0
        df_daily_cpr["BC"] = (2.0 * df_daily_cpr["Pivot"]) - df_daily_cpr["TC"]

        df_daily_cpr["R1"] = 2.0 * df_daily_cpr["Pivot"] - df_daily_cpr["prev_day_low"]
        df_daily_cpr["R2"] = df_daily_cpr["Pivot"] + (df_daily_cpr["prev_day_high"] - df_daily_cpr["prev_day_low"])
        df_daily_cpr["R3"] = df_daily_cpr["prev_day_high"] + 2.0 * (df_daily_cpr["Pivot"] - df_daily_cpr["prev_day_low"])
        df_daily_cpr["R4"] = df_daily_cpr["R3"] + (df_daily_cpr["R2"] - df_daily_cpr["R1"])

        df_daily_cpr["S1"] = 2.0 * df_daily_cpr["Pivot"] - df_daily_cpr["prev_day_high"]
        df_daily_cpr["S2"] = df_daily_cpr["Pivot"] - (df_daily_cpr["prev_day_high"] - df_daily_cpr["prev_day_low"])
        df_daily_cpr["S3"] = df_daily_cpr["prev_day_low"] - 2.0 * (df_daily_cpr["prev_day_high"] - df_daily_cpr["Pivot"])
        df_daily_cpr["S4"] = df_daily_cpr["S3"] - (df_daily_cpr["S1"] - df_daily_cpr["S2"])

        self.df_daily_cpr = df_daily_cpr
        return df_daily_cpr

    # ======================
    # Update on Streaming Data
    # ======================
    def update_with_new_data(self, start_date, end_date):
        """Fetch last few candles and update indicators incrementally"""
        new_df = self.client.history(
            symbol=self.symbol,
            exchange=self.exchange,
            interval=self.interval,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d")
        )
        if not isinstance(new_df.index, pd.DatetimeIndex):
            new_df.index = pd.to_datetime(new_df.index)

        self.df = pd.concat([self.df, new_df]).drop_duplicates()

        # Update last 200 rows to keep speed
        self.add_trend_indicators()
        self.add_momentum_indicators()
        self.add_previous_candle()
        return self.df
