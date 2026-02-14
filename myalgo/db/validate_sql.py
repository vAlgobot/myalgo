
import duckdb

DB_PATH = r"C:\Users\Srivi\myalgo_openalgo\myalgo\data\market_data.db"

def validate_query():
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    underlying = "NIFTY"
    exchange = "NFO"
    expiry = "05JUN25"
    strike = 25000
    symbol = "NIFTY05JUN2525000CE"
    timeframe = "1d"
    start_date = "2025-04-01 00:00:00"
    end_date = "2025-06-05 23:59:59"
    
    print(f"Testing Query for: {symbol} in {timeframe}")
    
    query = """
    SELECT symbol, timestamp, open, high, low, close, volume, oi, strike, expiry, underlying
    FROM options_data_master
    WHERE underlying = ? 
      AND exchange = ? 
      AND expiry = ? 
      AND strike = ? 
      AND symbol = ? 
      AND LOWER(timeframe) = ? 
      AND timestamp >= ? 
      AND timestamp <= ?
    ORDER BY timestamp ASC
    """
    
    params = [underlying, exchange, expiry, strike, symbol, timeframe, start_date, end_date]
    
    try:
        df = conn.execute(query, params).df()
        print(f"Rows returned: {len(df)}")
        if not df.empty:
            print("Sample Data:")
            print(df.head())
        else:
            print("No data returned!")
            # Diagnostic: Check if symbol exists at all
            check = conn.execute("SELECT count(*) FROM options_data_master WHERE symbol=?", [symbol]).fetchone()
            print(f"Total rows for {symbol} in DB (any timeframe): {check[0]}")
            print("Sample Data:")
            check_df = conn.execute("SELECT * FROM options_data_master WHERE symbol=?", [symbol]).df()
            print(check_df.head())
    except Exception as e:
        print(f"Query Execution Failed: {e}")

if __name__ == "__main__":
    validate_query()
