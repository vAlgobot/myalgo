import sqlite3
import pandas as pd
import numpy as np
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "myalgo.db")

def safe_max(values):
    return max(values) if values else 0

def safe_avg(values):
    return round(sum(values) / len(values), 1) if values else 0

def format_profit_factor(gross_profit, gross_loss):
    if gross_loss == 0 and gross_profit > 0:
        return "∞ (No losing trades)"
    if gross_profit == 0:
        return "0.0"
    return round(gross_profit / gross_loss, 2)

def safe_avg_loss(losers):
    return round(losers.pnl.mean(), 2) if len(losers) else 0

def streaks(values):
    out, cur = [], 0
    for v in values:
        if v:
            cur += 1
        else:
            if cur:
                out.append(cur)
            cur = 0
    if cur:
        out.append(cur)
    return out

def generate_analysis_report(db_path=None, brokerage_per_order=20.0, other_charges_factor=10.0):
    """
    Connects to the database and generates a trading performance report.
    Args:
        db_path (str): Path to sqlite db
        brokerage_per_order (float): Flat fee per executed order (approx ₹20)
        other_charges_factor (float): Estimated multiplier for STT/GST/etc per order or turnover.
                                      For simplicity, we'll use a flat per-order estimate modification if needed,
                                      or just stick to per-order brokerage + slippage estimation.
                                      Let's keep it simple: Brokerage * Completed Orders.
    """
    target_db = db_path if db_path else DEFAULT_DB_PATH
    
    if not os.path.exists(target_db):
        print(f"❌ Database not found at: {target_db}")
        return

    # -------------------------------------------------
    # LOAD DATA
    # --------------------------------------------------
    try:
        conn = sqlite3.connect(target_db)
        
        positionbook = pd.read_sql(
            "SELECT * FROM positionbook ORDER BY timestamp",
            conn, parse_dates=["timestamp"]
        )
        
        orderbook = pd.read_sql(
            "SELECT * FROM orderbook",
            conn, parse_dates=["timestamp"]
        )
        
        tradebook = pd.read_sql(
            "SELECT * FROM tradebook ORDER BY timestamp",
            conn, parse_dates=["timestamp"]
        )
        
        conn.close()
    except Exception as e:
        print(f"❌ Error reading database: {e}")
        return

    # --------------------------------------------------
    # TRADE RECONSTRUCTION FROM TRADEBOOK
    # --------------------------------------------------
    if tradebook.empty:
        print("⚠️ No trades found in tradebook.")
        return

    trades = []

    for symbol, df in tradebook.groupby("symbol"):
        df = df.sort_values("timestamp")

        open_buys = []

        for _, row in df.iterrows():
            if row["action"] == "BUY":
                open_buys.append(row)
            elif row["action"] == "SELL" and open_buys:
                buy = open_buys.pop(0)   # FIFO

                pnl = row["trade_value"] - buy["trade_value"]

                trades.append({
                    "symbol": symbol,
                    "entry_time": buy["timestamp"],
                    "exit_time": row["timestamp"],
                    "pnl": pnl
                })

    trades = pd.DataFrame(trades)

    if trades.empty:
        print("⚠️ No realized trades reconstructed.")
        return

    trades["date"] = trades["exit_time"].dt.date

    period_start = trades["date"].min()
    period_end = trades["date"].max()

    # --------------------------------------------------
    # 1️⃣ TRADE LEVEL PERFORMANCE
    # --------------------------------------------------
    total_trades = len(trades)
    winners = trades[trades.pnl > 0]
    losers = trades[trades.pnl < 0]

    gross_profit = winners.pnl.sum()
    gross_loss = abs(losers.pnl.sum())
    gross_pnl = trades.pnl.sum()

    trade_metrics = {
        "Total Trades": total_trades,
        "Winning Trades": len(winners),
        "Losing Trades": len(losers),
        "Breakeven Trades": (trades.pnl == 0).sum(),
        "Win Rate": round(len(winners) / total_trades * 100, 1),
        "Gross Profit": gross_profit,
        "Gross Loss": -gross_loss,
        "Gross PnL": gross_pnl,  # Renamed from Net PnL to Gross PnL
        "Avg Trade": round(trades.pnl.mean(), 0),
        "Avg Win": round(winners.pnl.mean(), 0),
        "Avg Loss": safe_avg_loss(losers),
        "Profit Factor": format_profit_factor(gross_profit, gross_loss),
        "Expectancy": round(trades.pnl.mean(), 0)
    }

    # --------------------------------------------------
    # 2️⃣ DAY LEVEL PERFORMANCE
    # --------------------------------------------------
    day_pnl = trades.groupby("date")["pnl"].sum()
    
    day_metrics = {
        "Trading Days": len(day_pnl),
        "Winning Days": (day_pnl > 0).sum(),
        "Losing Days": (day_pnl < 0).sum(),
        "Win-Day %": round((day_pnl > 0).sum() / len(day_pnl) * 100, 1),
        "Avg Day PnL": round(day_pnl.mean(), 0),
        "Best Day": day_pnl.max(),
        "Worst Day": day_pnl.min()
    }

    # --------------------------------------------------
    # 3️⃣ STREAK ANALYSIS
    # --------------------------------------------------
    trade_win_streaks = streaks(trades.pnl > 0)
    trade_loss_streaks = streaks(trades.pnl < 0)

    day_win_streaks = streaks(day_pnl > 0)
    day_loss_streaks = streaks(day_pnl < 0)

    # --------------------------------------------------
    # 4️⃣ EQUITY & DRAWDOWN
    # --------------------------------------------------
    equity = trades.pnl.cumsum()
    peak = equity.cummax()
    drawdown = equity - peak

    # --------------------------------------------------
    # 5️⃣ SYMBOL PERFORMANCE
    # --------------------------------------------------
    symbol_perf = trades.groupby("symbol").agg(
        Trades=("pnl", "count"),
        Wins=("pnl", lambda x: (x > 0).sum()),
        PnL=("pnl", "sum")
    )
    symbol_perf["Win %"] = round(symbol_perf["Wins"] / symbol_perf["Trades"] * 100, 1)

    best_symbol = symbol_perf["PnL"].idxmax()
    worst_symbol = symbol_perf["PnL"].idxmin()

    # --------------------------------------------------
    # 6️⃣ EXECUTION QUALITY & CHARGES
    # --------------------------------------------------
    total_orders = len(orderbook)
    completed = (orderbook.order_status == "complete").sum()
    cancelled = (orderbook.order_status == "cancelled").sum()
    
    # Calculate Charges
    # Simple model: Brokerage per COMPLETED order
    total_brokerage = completed * (brokerage_per_order + other_charges_factor)
    
    # Approximate other charges (STT, etc) if needed, for now mainly brokerage
    # If user wants customized "broker charges" we focus on that.
    total_charges = total_brokerage 
    
    net_pnl_after_charges = gross_pnl - total_charges

    # --------------------------------------------------
    # 7️⃣ POSITION RISK
    # --------------------------------------------------
    if not positionbook.empty:
        peak_exposure = (positionbook.quantity * positionbook.average_price).abs().max()
        max_mtm_profit = positionbook.pnl.max()
        max_mtm_loss = positionbook.pnl.min()
    else:
        peak_exposure = 0
        max_mtm_profit = 0
        max_mtm_loss = 0

    # --------------------------------------------------
    # PRINT FINAL REPORT
    # --------------------------------------------------
    print("\n" + "="*50)
    print("📈 TRADE JOURNAL PERFORMANCE REPORT")
    print("="*50)
    print(f"Period: {period_start} → {period_end}")
    print("Instrument: NIFTY OPTIONS")
    print("Strategy: Breakout\n")

    print("🧮 1️⃣ TRADE-LEVEL PERFORMANCE (GROSS)")
    for k, v in trade_metrics.items():
        print(f"{k:<20}: ₹{v}" if "PnL" in k or "Profit" in k or "Loss" in k else f"{k:<20}: {v}")

    print("\n📆 2️⃣ DAY-LEVEL PERFORMANCE")
    for k, v in day_metrics.items():
        print(f"{k:<20}: ₹{v}" if "PnL" in k or "Day" in k else f"{k:<20}: {v}")

    print("\n🔁 3️⃣ STREAK ANALYSIS")
    print(f"Max Winning Trades in a Row : {safe_max(trade_win_streaks)}")
    print(f"Max Losing Trades in a Row  : {safe_max(trade_loss_streaks)}")
    print(f"Avg Winning Streak          : {safe_avg(trade_win_streaks)}")
    print(f"Avg Losing Streak           : {safe_avg(trade_loss_streaks)}")
    print(f"Max Winning Days in a Row   : {safe_max(day_win_streaks)}")
    print(f"Max Losing Days in a Row    : {safe_max(day_loss_streaks)}")

    print("\n📉 4️⃣ EQUITY & DRAWDOWN")
    if not equity.empty:
        print(f"Peak Equity     : ₹{peak.max()}")
        print(f"Final Equity    : ₹{equity.iloc[-1]}")
        print(f"Max Drawdown    : ₹{drawdown.min()}")
        peak_val = peak.max()
        drawdown_pct = round(drawdown.min()/peak_val*100,1) if peak_val != 0 else 0
        print(f"Max Drawdown %  : {drawdown_pct}%")
    else:
        print("No equity data.")

    print("\n🎯 5️⃣ SYMBOL-WISE PERFORMANCE")
    print(symbol_perf[["Trades", "Win %", "PnL"]])
    print(f"\nBest Strike  : {best_symbol}")
    print(f"Worst Strike : {worst_symbol}")

    print("\n🧩 6️⃣ POSITION RISK")
    print(f"Max MTM Profit   : ₹{max_mtm_profit}")
    print(f"Max MTM Loss     : ₹{max_mtm_loss}")
    print(f"Peak Exposure    : ₹{peak_exposure}")
    # print(f"Realized PnL     : ₹{net_pnl}")
    print("Unrealized PnL   : ₹0")

    print("\n⚙️ 7️⃣ EXECUTION & CHARGES")
    print(f"Total Orders     : {total_orders}")
    print(f"Completed Orders : {completed}")
    print(f"Cancelled Orders : {cancelled}")
    cancel_rate = round(cancelled/total_orders*100) if total_orders > 0 else 0
    fill_rate = round(completed/total_orders*100) if total_orders > 0 else 0
    print(f"Cancel Rate      : {cancel_rate}%")
    print(f"Fill Rate        : {fill_rate}%")
    print(f"-"*30)
    print(f"Brokerage (Est)  : ₹{total_charges:.2f}  (@ ₹{brokerage_per_order}/order)")
    print(f"-"*30)
    
    print(f"\n💰 NET PnL (After Charges): ₹{net_pnl_after_charges:.2f}")
    print("="*50 + "\n")

if __name__ == "__main__":
    generate_analysis_report()
