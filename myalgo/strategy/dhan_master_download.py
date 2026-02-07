"""
FOR each trading day D:

    Fetch SPOT daily candle
    Build strike ladder using day range + buffer 5 strikes Call & Put
    FOR each strike:
        FOR CE & PE:
            Fetch expired option OHLCV + iv + oi + spot (interval 1 or 5)
            If interval=1 -> lookup only that day
            If interval=5 -> lookup last 60 days
            Store data in local DB / CSV for later analysis
"""

import logging
import time
import json
import os
from datetime import datetime, timedelta

import pyotp
import pandas as pd

from dhanhq import DhanLogin, DhanContext, dhanhq


class DhanExpiredDataDownloader:
    """
    Headless Dhan login + expired options downloader
    Single file | Single class | Production safe
    """

    def __init__(self):
        # ===== CREDENTIALS =====
        self.CLIENT_ID = "1106711453"
        self.PIN = "251988"
        self.TOTP_SECRET = "NKAH37E23LXNVODCEYUJAPDEP3ZLUYMF"

        # ===== TOKEN SETTINGS =====
        self.TOKEN_FILE = "dhan_token.json"
        self.TOKEN_VALIDITY = 6 * 60 * 60  # 6 hours (safe)

        # ===== INSTRUMENT SETTINGS =====
        self.NIFTY_SECURITY_ID = 13          # adjust if needed
        self.EXCHANGE_SEGMENT = "NSE_FNO"
        self.OPT_INSTRUMENT = "OPTIDX"

        self.dhan = self._create_client()

    # ==================================================
    # 🔐 TOKEN HANDLING
    # ==================================================
    def _load_token(self):
        if not os.path.exists(self.TOKEN_FILE):
            return None, 0
        with open(self.TOKEN_FILE, "r") as f:
            data = json.load(f)
        return data.get("access_token"), data.get("generated_at", 0)

    def _save_token(self, token):
        with open(self.TOKEN_FILE, "w") as f:
            json.dump(
                {
                    "access_token": token,
                    "generated_at": int(time.time()),
                },
                f,
            )

    def _generate_token(self):
        print("🔐 Generating new Dhan token (rate-limited)")
        totp = pyotp.TOTP(self.TOTP_SECRET).now()
        login = DhanLogin(self.CLIENT_ID)
        token_data = login.generate_token(self.PIN, totp)
        access_token = token_data["accessToken"]
        self._save_token(access_token)
        return access_token

    def _get_valid_token(self):
        token, ts = self._load_token()
        # ✅ reuse token if valid
        if token and (time.time() - ts) < self.TOKEN_VALIDITY:
            print("♻️ Using cached access token")
            return token
        # ❌ expired → generate new
        return self._generate_token()

    # ==================================================
    # 🧠 DHAN CLIENT
    # ==================================================
    def _create_client(self):
        access_token = self._get_valid_token()
        context = DhanContext(self.CLIENT_ID, access_token)
        return dhanhq(context)

    def _refresh_client(self):
        print("🔄 Refreshing Dhan client")
        self.dhan = self._create_client()

    # ==================================================
    # 📈 SPOT DAILY CANDLES FROM OPENALGO (NIFTY)
    # ==================================================
    

    # ==================================================
    # 📥 EXPIRED OPTIONS DATA FROM DHAN(ROLLING)
    # ==================================================
    def download_expired_options(
        self,
        from_date,
        to_date,
        strike="ATM",
        option_type="CALL",
        expiry_flag="WEEK",
        expiry_code=1,
        interval=1,
    ):
        """
        Wrapper over /charts/rollingoption
        """
        try:
            response = self.dhan.expired_options_data(
                security_id=self.NIFTY_SECURITY_ID,
                exchange_segment=self.EXCHANGE_SEGMENT,
                instrument_type=self.OPT_INSTRUMENT,
                expiry_flag=expiry_flag,
                expiry_code=expiry_code,
                strike=strike,
                drv_option_type=option_type,
                required_data=[
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "strike",
                    "oi",
                    "iv",
                    "spot",
                ],
                from_date=from_date,  # ✅ Use parameter instead of hardcoded
                to_date=to_date,      # ✅ Use parameter instead of hardcoded
                interval=interval,
            )
            time.sleep(1)  # ⏰ Respect rate limit
            return response
        except Exception as e:
            # 🔁 Auto-recover if token expired mid-run
            if "token" in str(e).lower():
                print("⚠️ Token issue detected, refreshing...")
                time.sleep(120)  # respect Dhan cooldown
                self._refresh_client()
                return self.download_expired_options(
                    from_date,
                    to_date,
                    strike,
                    option_type,
                    expiry_flag,
                    expiry_code,
                    interval,
                )
            raise e
            
    # ==================================================
    # 🧮 LADDER & FLATTENING
    # ==================================================
    @staticmethod
    def _chunk_date_range(start_date: str, end_date: str, chunk_days: int = 31):
        """
        Split date range into chunks (default 31 calendar days).
        
        Note: Dhan API limit is "upto 30 days" but their example shows 31 days works.
        Since months can have 31 days, we use 31 to avoid extra chunks.
        
        Args:
            start_date: "2024-01-01"
            end_date: "2024-12-31"
            chunk_days: 31 (calendar days, not trading days)
            
        Returns:
            List of tuples: [("2024-01-01", "2024-01-31"), ("2024-02-01", "2024-03-03"), ...]
        """
        chunks = []
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        while current <= end:
            chunk_end = min(current + timedelta(days=chunk_days - 1), end)
            chunks.append((
                current.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d")
            ))
            current = chunk_end + timedelta(days=1)
        
        return chunks
    
    # ==================================================
    # 🧮 LADDER & FLATTENING
    # ==================================================
    @staticmethod
    def _offset_to_str(k: int) -> str:
        if k == 0:
            return "ATM"
        return f"ATM+{k}" if k > 0 else f"ATM{k}"  # e.g. ATM-2

    @staticmethod
    def _rolling_side_to_rows(side_data: dict, side: str, strike_expr: str,
                              expiry_flag: str, expiry_code: int):
        """
        Convert one side (ce/pe) rolling payload into list of dict rows.
        """
        if not side_data:
            return []

        ts = side_data.get("timestamp", [])
        opens = side_data.get("open", [])
        highs = side_data.get("high", [])
        lows = side_data.get("low", [])
        closes = side_data.get("close", [])
        vols = side_data.get("volume", [])
        ivs = side_data.get("iv", [])
        ois = side_data.get("oi", [])
        strikes = side_data.get("strike", [])
        spots = side_data.get("spot", [])

        rows = []
        for i in range(len(ts)):
            rows.append(
                {
                    "timestamp": datetime.fromtimestamp(ts[i]),
                    "open": opens[i] if i < len(opens) else None,
                    "high": highs[i] if i < len(highs) else None,
                    "low": lows[i] if i < len(lows) else None,
                    "close": closes[i] if i < len(closes) else None,
                    "volume": vols[i] if i < len(vols) else None,
                    "iv": ivs[i] if i < len(ivs) else None,
                    "oi": ois[i] if i < len(ois) else None,
                    "spot": spots[i] if i < len(spots) else None,
                    "strike_price": strikes[i] if i < len(strikes) else None,
                    "side": side,                 # "CE" or "PE"
                    "strike_expr": strike_expr,   # "ATM", "ATM+1", ...
                    "expiry_flag": expiry_flag,
                    "expiry_code": expiry_code,
                }
            )
        return rows

    def download_ladder_for_chunk(
        self,
        from_date: str,
        to_date: str,
        offsets=range(-5, 6),
        expiry_flag="WEEK",
        expiry_code=1,
        interval=1,
        api_delay=1.0,
    ):
        """
        Download entire date range (chunk/month) in single batch.
        
        CRITICAL: Downloads ENTIRE PERIOD (e.g., full month) in one API call per strike.
        This is MUCH faster than day-by-day downloading.
        
        Args:
            from_date: "2024-01-01" (chunk start)
            to_date: "2024-01-31" (chunk end)
            offsets: Strike offsets (e.g., range(-10, 11))
            api_delay: Delay between API calls in seconds
            
        Returns:
            List of row dictionaries for entire period
        """
        all_rows = []

        for k in offsets:
            strike_expr = self._offset_to_str(k)
            for side in ["CALL", "PUT"]:
                print(f"📥 Chunk={from_date} to {to_date} | Offset={strike_expr} | Side={side}")
                try:
                    resp = self.download_expired_options(
                        from_date=from_date,      # ✅ ENTIRE CHUNK START
                        to_date=to_date,          # ✅ ENTIRE CHUNK END
                        strike=strike_expr,
                        option_type=side,
                        expiry_flag=expiry_flag,
                        expiry_code=expiry_code,
                        interval=interval,
                    )
                    
                    # ✅ Validate response structure
                    print(f"    Response type: {type(resp)}, Keys: {list(resp.keys()) if isinstance(resp, dict) else 'N/A'}")
                    
                    status = resp.get("status") if isinstance(resp, dict) else None
                    
                    # 🔁 Handle rate limit with exponential backoff
                    if status == "failure":
                        remarks = resp.get("remarks") if isinstance(resp, dict) else resp
                        if isinstance(remarks, dict) and remarks.get("error_code") == "DH-904":
                            backoff_delay = api_delay * 3
                            print(f"⏱️ Rate limit hit! Waiting {backoff_delay}s before retry...")
                            time.sleep(backoff_delay)
                            continue
                        else:
                            print(f"⚠️ API failed: {remarks}")
                            continue
                    
                    if status != "success":
                        print(f"⚠️ API failed: {resp.get('remarks') if isinstance(resp, dict) else resp}")
                        continue
                    
                    # Get outer data container
                    outer_data = resp.get("data") if isinstance(resp, dict) else None
                    print(f"    Outer data type: {type(outer_data)}")
                    
                    if outer_data is None:
                        print("⚠️ No data in response")
                        continue
                    
                    # Handle nested data["data"] container
                    if isinstance(outer_data, dict):
                        if "data" in outer_data:
                            data = outer_data["data"]
                            print(f"    Inner data type: {type(data)}")
                        else:
                            data = outer_data
                    else:
                        print(f"⚠️ Outer data is not dict, it's {type(outer_data)}: {outer_data}")
                        continue
                    
                    # Validate data is a dict
                    if not isinstance(data, dict):
                        print(f"⚠️ Data is not dict: {type(data)}")
                        continue
                    
                    ce_data = data.get("ce")
                    pe_data = data.get("pe")
                    
                    ce_count = len(ce_data.get('timestamp', [])) if ce_data and isinstance(ce_data, dict) else 0
                    pe_count = len(pe_data.get('timestamp', [])) if pe_data and isinstance(pe_data, dict) else 0
                    print(f"    Retrieved {ce_count} CE candles, {pe_count} PE candles")
                    
                    # Process Call option
                    if side == "CALL" and ce_data and isinstance(ce_data, dict):
                        rows = self._rolling_side_to_rows(
                            ce_data,
                            side="CE",
                            strike_expr=strike_expr,
                            expiry_flag=expiry_flag,
                            expiry_code=expiry_code,
                        )
                        all_rows.extend(rows)
                    # Process Put option
                    elif side == "PUT" and pe_data and isinstance(pe_data, dict):
                        rows = self._rolling_side_to_rows(
                            pe_data,
                            side="PE",
                            strike_expr=strike_expr,
                            expiry_flag=expiry_flag,
                            expiry_code=expiry_code,
                        )
                        all_rows.extend(rows)
                    
                    # ⏰ Throttle between API calls
                    print(f"    ⏱️ Waiting {api_delay}s before next call...")
                    time.sleep(api_delay)
                
                except Exception as e:
                    print(f"❌ Error processing {from_date}-{to_date} {strike_expr} {side}: {e}")
                    import traceback
                    traceback.print_exc()
                    continue

        return all_rows

    def download_ladder_for_day(
        self,
        day: str,
        offsets=range(-5, 6),
        expiry_flag="WEEK",
        expiry_code=1,
        interval=1,
        api_delay=1.0,  # ⏰ Delay between API calls (seconds)
    ):
        """
        For a single trading day (YYYY-MM-DD):
          - For each offset in offsets and each side CE/PE
          - fetch rolling expired options data and flatten into rows.
          
        Args:
            api_delay (float): Delay between API calls in seconds (default: 1.0)
        """
        from_date = day
        # toDate is non-inclusive; next calendar day
        to_date = (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )

        all_rows = []

        for k in offsets:
            strike_expr = self._offset_to_str(k)
            for side in ["CALL", "PUT"]:
                print(f"📥 Day={day} Offset={strike_expr} Side={side}")
                try:
                    resp = self.download_expired_options(
                        from_date=from_date,
                        to_date=to_date,
                        strike=strike_expr,
                        option_type=side,
                        expiry_flag=expiry_flag,
                        expiry_code=expiry_code,
                        interval=interval,
                    )
                    
                    # ✅ Validate response structure
                    print(f"    Response type: {type(resp)}, Keys: {list(resp.keys()) if isinstance(resp, dict) else 'N/A'}")
                    
                    status = resp.get("status") if isinstance(resp, dict) else None
                    
                    # 🔁 Handle rate limit with exponential backoff
                    if status == "failure":
                        remarks = resp.get("remarks") if isinstance(resp, dict) else resp
                        if isinstance(remarks, dict) and remarks.get("error_code") == "DH-904":
                            backoff_delay = api_delay * 3  # 3x delay on rate limit
                            print(f"⏱️ Rate limit hit! Waiting {backoff_delay}s before retry...")
                            time.sleep(backoff_delay)
                            continue
                        else:
                            print(f"⚠️ API failed: {remarks}")
                            continue
                    
                    if status != "success":
                        print(f"⚠️ API failed: {resp.get('remarks') if isinstance(resp, dict) else resp}")
                        continue
                    
                    # Get outer data container
                    outer_data = resp.get("data") if isinstance(resp, dict) else None
                    print(f"    Outer data type: {type(outer_data)}")
                    
                    if outer_data is None:
                        print("⚠️ No data in response")
                        continue
                    
                    # Handle nested data["data"] container
                    if isinstance(outer_data, dict):
                        if "data" in outer_data:
                            data = outer_data["data"]
                            print(f"    Inner data type: {type(data)}")
                        else:
                            data = outer_data
                    else:
                        print(f"⚠️ Outer data is not dict, it's {type(outer_data)}: {outer_data}")
                        continue
                    
                    # Validate data is a dict
                    if not isinstance(data, dict):
                        print(f"⚠️ Data is not dict: {type(data)}")
                        continue
                    
                    ce_data = data.get("ce")
                    pe_data = data.get("pe")
                    
                    ce_count = len(ce_data.get('timestamp', [])) if ce_data and isinstance(ce_data, dict) else 0
                    pe_count = len(pe_data.get('timestamp', [])) if pe_data and isinstance(pe_data, dict) else 0
                    print(f"    Retrieved {ce_count} CE candles, {pe_count} PE candles")
                    
                    # Process Call option
                    if side == "CALL" and ce_data and isinstance(ce_data, dict):
                        rows = self._rolling_side_to_rows(
                            ce_data,
                            side="CE",
                            strike_expr=strike_expr,
                            expiry_flag=expiry_flag,
                            expiry_code=expiry_code,
                        )
                        all_rows.extend(rows)
                    # Process Put option
                    elif side == "PUT" and pe_data and isinstance(pe_data, dict):
                        rows = self._rolling_side_to_rows(
                            pe_data,
                            side="PE",
                            strike_expr=strike_expr,
                            expiry_flag=expiry_flag,
                            expiry_code=expiry_code,
                        )
                        all_rows.extend(rows)
                    
                    # ⏰ Throttle between API calls
                    print(f"    ⏱️ Waiting {api_delay}s before next call...")
                    time.sleep(api_delay)
                
                except Exception as e:
                    print(f"❌ Error processing {day} {strike_expr} {side}: {e}")
                    import traceback
                    traceback.print_exc()
                    continue

        return all_rows

    def download_ladder_and_save(
        self,
        start_date: str,
        end_date: str,
        offsets=range(-5, 6),
        expiry_flag="WEEK",
        expiry_code=1,
        interval=1,
        out_dir="expired_monthly",
        api_delay=1.0,
        chunk_days=31,  # ✅ 31 calendar days (Dhan API accepts this per their example)
        ):
        """
        Chunk-wise download with automatic 31-day batching.
        Month-wise CSV saving.
        
        CRITICAL: Downloads ENTIRE CHUNK (e.g., full month) in single batch per strike.
        Much faster than day-by-day downloading!
        
        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            chunk_days (int): Split date range into chunks (default 31 CALENDAR days)
                             Note: 31 calendar days = ~23 trading days
            api_delay: Delay between API calls (seconds)
        """
        os.makedirs(out_dir, exist_ok=True)

        monthly_rows = {}  # key = YYYY-MM, value = list of rows
        
        # ✅ Split date range into 31-day chunks
        chunks = self._chunk_date_range(start_date, end_date, chunk_days)
        print(f"📦 Splitting date range into {len(chunks)} chunk(s) of max {chunk_days} days each")
        
        for chunk_idx, (chunk_start, chunk_end) in enumerate(chunks, 1):
            print(f"\n🔄 Processing chunk {chunk_idx}/{len(chunks)}: {chunk_start} to {chunk_end}")
            
            # ✅ Download ENTIRE CHUNK in single batch (not day-by-day)
            rows = self.download_ladder_for_chunk(
                from_date=chunk_start,
                to_date=chunk_end,
                offsets=offsets,
                expiry_flag=expiry_flag,
                expiry_code=expiry_code,
                interval=interval,
                api_delay=api_delay,
            )
            
            # Organize rows by month for CSV saving
            if rows:
                for row in rows:
                    month_key = row["timestamp"].strftime("%Y-%m")
                    monthly_rows.setdefault(month_key, []).append(row)
                
                print(f"✅ Chunk {chunk_idx}/{len(chunks)}: Downloaded {len(rows)} total rows")

    # ============================
    # SAVE MONTH-WISE CSV
    # ============================
        for month, rows in monthly_rows.items():
            if not rows:
                continue

            df = pd.DataFrame(rows)
            df["date"] = df["timestamp"].dt.date

            file_path = os.path.join(
                out_dir,
                f"NIFTY_{month}_{interval}min.csv"
            )

            df.to_csv(file_path, index=False)
            print(f"✅ Saved {len(df)} rows → {file_path}")

        if not monthly_rows:
            print("⚠️ No data fetched for given period")

# ==================================================
# 🚀 RUN
# ==================================================
if __name__ == "__main__":
    downloader = DhanExpiredDataDownloader()

    # Example: Full year download with automatic 31-day chunking
    # The script will automatically split "2024-01-01" to "2024-12-31" into ~12 chunks
    # Each chunk downloads ENTIRE MONTH in single batch (not day-by-day)
    # 
    # Note: chunk_days = 31 CALENDAR days (includes weekends)
    #       This equals ~23 TRADING days per chunk
    # 
    # Rate limit recommendations:
    # - api_delay: 1-2 seconds between API calls (per strike per side) 
    downloader.download_ladder_and_save(
        start_date="2026-01-01",  # ✅ User can specify any date range
        end_date="2026-02-06",    # ✅ Script auto-splits into 31-day chunks
        offsets=range(-10, 11),   # 10 strikes on each side of ATM (21 total)
        expiry_flag="WEEK",
        expiry_code=1,           
        interval=1,              # 1-min; use 5 for 5-min
        out_dir="expired_monthly",
        api_delay=1.5,           # ⏰ 1.5 seconds between each API call 
        chunk_days=31,           # ✅ 31 CALENDAR days = ~23 trading days per chunk
    )