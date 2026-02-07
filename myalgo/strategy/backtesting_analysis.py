import sqlite3
import pandas as pd
import os
from logger import get_logger

analysis_logger = get_logger("backtest_analysis")

INITIAL_CAPITAL = 50000

class BacktestingAnalysis:
    def __init__(self, db_path: str, brokerage_per_order: float = 20):
        self.db_path = db_path
        self.brokerage_per_order = brokerage_per_order

        self.raw_df = None
        self.trade_df = None


    # -------------------------------
    # Load raw trade legs
    # -------------------------------
    
    def load_trade_legs(self):
        conn = sqlite3.connect(self.db_path)
        self.raw_df = pd.read_sql(
            """
            SELECT timestamp, symbol, quantity, price, leg_type, reason
            FROM my_trade_logs
            WHERE leg_type IN ('ENTRY', 'EXIT', 'CPR_TP_HIT')
            ORDER BY timestamp
            """,
            conn
        )
        conn.close()

        if self.raw_df.empty:
            raise ValueError("No ENTRY / EXIT records found in DB")

        self.raw_df["timestamp"] = pd.to_datetime(self.raw_df["timestamp"])

    # -------------------------------
    # Pair ENTRY → EXIT & calculate PnL (Enhanced with Opportunity)
    # -------------------------------
    def build_trades(self):
        trades = []
        open_trade = None
        tp_prices = []

        for _, row in self.raw_df.iterrows():
            if row["leg_type"] == "ENTRY":
                open_trade = row
                tp_prices = []

            elif row["leg_type"] == "CPR_TP_HIT" and open_trade is not None:
                tp_prices.append(row["price"])

            elif row["leg_type"] == "EXIT" and open_trade is not None:
                entry_price = float(open_trade["price"])
                exit_price = float(row["price"])
                qty = float(row["quantity"])

                # If we had TPs hit, the 'opportunity' was the max price we realized along the way or at exit
                # Assumes Long Option Buying strategy
                max_price = max([exit_price] + tp_prices) if tp_prices else exit_price

                execution_pnl = (exit_price - entry_price) * qty
                opportunity_pnl = (max_price - entry_price) * qty
                missed_pnl = opportunity_pnl - execution_pnl
                
                capital = entry_price * qty
                
                # 🎯 Determine exit reason/status
                exit_reason = row.get("reason", "Unknown")
                
                # Classify based on REASON field from database
                # Check if reason contains "Breakeven" or "Break Even"
                if "breakeven" in str(exit_reason).lower() or "break even" in str(exit_reason).lower():
                    trade_status = "BREAKEVEN"
                elif execution_pnl > 0:
                    trade_status = "WIN"
                elif execution_pnl < 0:
                    trade_status = "LOSS"
                else:
                    # Exactly 0 PnL (edge case)
                    trade_status = "BREAKEVEN"

                trades.append({
                    "exit_time": row["timestamp"],
                    "pnl": execution_pnl, # For existing metrics
                    "capital": capital,
                    "execution_pnl": execution_pnl,
                    "opportunity_pnl": opportunity_pnl,
                    "missed_pnl": missed_pnl,
                    "exit_reason": exit_reason,
                    "trade_status": trade_status
                })

                open_trade = None
                tp_prices = []

        self.trade_df = pd.DataFrame(trades)

        if self.trade_df.empty:
            raise ValueError("No completed trades (ENTRY → EXIT) found")

        self.trade_df["date"] = self.trade_df["exit_time"].dt.date

    # -------------------------------
    # Utility: streak calculation
    # -------------------------------
    @staticmethod
    def _streaks(series):
        streaks = []
        current = 0

        for v in series:
            if v:
                current += 1
            else:
                if current:
                    streaks.append(current)
                current = 0

        if current:
            streaks.append(current)

        return (
            max(streaks) if streaks else 0,
            sum(streaks) / len(streaks) if streaks else 0
        )

    # -------------------------------
    # Compute all performance metrics
    # -------------------------------
    def analyze(self):
        df = self.trade_df

        # ---- Trade level (using trade_status for accurate classification) ----
        self.total_trades = len(df)
        self.wins = df[df["trade_status"] == "WIN"]
        self.losses = df[df["trade_status"] == "LOSS"]
        self.breakeven = df[df["trade_status"] == "BREAKEVEN"]

        self.gross_profit = self.wins["pnl"].sum()
        self.gross_loss = self.losses["pnl"].sum()
        self.gross_pnl = df["pnl"].sum()

        self.avg_trade = df["pnl"].mean()
        self.avg_win = self.wins["pnl"].mean() if not self.wins.empty else 0
        self.avg_loss = self.losses["pnl"].mean() if not self.losses.empty else 0
        self.win_rate = len(self.wins) / self.total_trades * 100 if self.total_trades > 0 else 0
        self.avg_capital = df["capital"].mean() if "capital" in df.columns else 0

        # ---- Day level ----
        self.day_pnl = df.groupby("date")["pnl"].sum()

        self.trading_days = len(self.day_pnl)
        self.winning_days = (self.day_pnl > 0).sum()
        self.losing_days = (self.day_pnl < 0).sum()

        self.avg_day_pnl = self.day_pnl.mean()
        self.best_day = self.day_pnl.max()
        self.worst_day = self.day_pnl.min()
        self.win_day_pct = self.winning_days / self.trading_days * 100 if self.trading_days > 0 else 0

        # ---- Streaks ----
        self.max_win_streak, self.avg_win_streak = self._streaks(df["trade_status"] == "WIN")
        self.max_loss_streak, self.avg_loss_streak = self._streaks(df["trade_status"] == "LOSS")

        self.max_win_day_streak, _ = self._streaks(self.day_pnl > 0)
        self.max_loss_day_streak, _ = self._streaks(self.day_pnl < 0)

        # ---- Equity & Drawdown ----
        df["cum_pnl"] = df["pnl"].cumsum()
        df["portfolio_equity"] = INITIAL_CAPITAL + df["cum_pnl"]
        df["peak_equity"] = df["portfolio_equity"].cummax()
        df["drawdown"] = df["portfolio_equity"] - df["peak_equity"]

        self.peak_equity = df["peak_equity"].max()
        self.final_equity = df["portfolio_equity"].iloc[-1]
        self.max_drawdown = df["drawdown"].min()
        self.max_drawdown_pct = (self.max_drawdown / self.peak_equity) * 100

        self.total_orders = self.total_trades * 2
        self.brokerage = self.total_orders * self.brokerage_per_order
        self.net_pnl = self.gross_pnl - self.brokerage

        # ---- Risk Metrics ----
        avg_loss_abs = abs(self.avg_loss) if self.avg_loss != 0 else 0
        self.risk_reward_ratio = (self.avg_win / avg_loss_abs) if avg_loss_abs > 0 else 0

        gross_loss_abs = abs(self.gross_loss) if self.gross_loss != 0 else 0
        self.profit_factor = (self.gross_profit / gross_loss_abs) if gross_loss_abs > 0 else float('inf')

        # ---- Opportunity Analysis ----
        self.analyze_opportunity()

    def analyze_opportunity(self):
        """Calculate Execution vs Opportunity metrics"""
        df = self.trade_df

        # EXECUTION METRICS
        df["execution_equity"] = df["execution_pnl"].cumsum()
        df["exec_peak"] = df["execution_equity"].cummax()
        df["exec_drawdown"] = df["execution_equity"] - df["exec_peak"]

        # OPPORTUNITY METRICS
        df["opportunity_equity"] = df["opportunity_pnl"].cumsum()
        df["opp_peak"] = df["opportunity_equity"].cummax()
        df["opp_drawdown"] = df["opportunity_equity"] - df["opp_peak"]

        # Final calculations for report
        # Use net_pnl (after brokerage) for final execution value to reflect actual portfolio change
        self.final_execution_value = INITIAL_CAPITAL + self.net_pnl
        self.final_opportunity_value = INITIAL_CAPITAL + df["opportunity_equity"].iloc[-1]

        self.execution_dd_pct = df["exec_drawdown"].min() / (INITIAL_CAPITAL + df["exec_peak"].max()) * 100
        self.opportunity_dd_pct = df["opp_drawdown"].min() / (INITIAL_CAPITAL + df["opp_peak"].max()) * 100
        
        # Growth Percentages
        # Use net_pnl (after brokerage) to calculate final portfolio growth percentage
        self.pnl_growth_pct = (self.net_pnl / INITIAL_CAPITAL) * 100
        self.opp_growth_pct = ((self.final_opportunity_value - INITIAL_CAPITAL) / INITIAL_CAPITAL) * 100

        self.total_missed_pnl = df["missed_pnl"].sum()
        self.avg_missed_pnl = df["missed_pnl"].mean()
        self.max_exec_drawdown = df['exec_drawdown'].min()
        self.max_opp_drawdown = df['opp_drawdown'].min()

    # -------------------------------
    # Print report
    # -------------------------------
    # -------------------------------
    # Print report
    # -------------------------------
    def print_report(self):
        analysis_logger.info("\n🧮 1️⃣ TRADE-LEVEL PERFORMANCE (GROSS)")
        analysis_logger.info(f"Total Trades        : {self.total_trades}")
        analysis_logger.info(f"Winning Trades      : {len(self.wins)} ({len(self.wins)/self.total_trades*100:.1f}%)")
        analysis_logger.info(f"Losing Trades       : {len(self.losses)} ({len(self.losses)/self.total_trades*100:.1f}%)")
        analysis_logger.info(f"Breakeven Trades    : {len(self.breakeven)} ({len(self.breakeven)/self.total_trades*100:.1f}%)")
        analysis_logger.info(f"Win Rate (W/(W+L))  : {self.win_rate:.1f}%")
        analysis_logger.info(f"Gross Profit        : ₹{self.gross_profit:.2f}")
        analysis_logger.info(f"Gross Loss          : ₹{self.gross_loss:.2f}")
        analysis_logger.info(f"Gross PnL           : ₹{self.gross_pnl:.2f}")
        analysis_logger.info(f"Avg Trade           : ₹{self.avg_trade:.2f}")
        analysis_logger.info(f"Avg Win             : ₹{self.avg_win:.2f}")
        analysis_logger.info(f"Avg Loss            : ₹{self.avg_loss:.2f}")
        analysis_logger.info(f"Avg Breakeven       : ₹{self.breakeven['pnl'].mean() if not self.breakeven.empty else 0:.2f}")
        analysis_logger.info(f"Avg Capital Deployed: ₹{self.avg_capital:.2f}")
        analysis_logger.info(f"Risk:Reward Ratio   : 1:{self.risk_reward_ratio:.2f}")
        analysis_logger.info(f"Profit Factor       : {self.profit_factor:.2f}")
        
        # 🎯 Exit reason breakdown
        analysis_logger.info("\n🎯 EXIT REASON BREAKDOWN")
        exit_reasons = self.trade_df['exit_reason'].value_counts()
        for reason, count in exit_reasons.items():
            pnl = self.trade_df[self.trade_df['exit_reason'] == reason]['pnl'].sum()
            avg_pnl = self.trade_df[self.trade_df['exit_reason'] == reason]['pnl'].mean()
            analysis_logger.info(f"  {reason:<20} : {count:>2} trades | Total PnL: ₹{pnl:>9.2f} | Avg: ₹{avg_pnl:>7.2f}")

        analysis_logger.info("\n📆 2️⃣ DAY-LEVEL PERFORMANCE")
        analysis_logger.info(f"Trading Days        : {self.trading_days}")
        analysis_logger.info(f"Winning Days        : {self.winning_days}")
        analysis_logger.info(f"Losing Days         : {self.losing_days}")
        analysis_logger.info(f"Win-Day %           : {self.win_day_pct:.1f}%")
        analysis_logger.info(f"Avg Day PnL         : ₹{self.avg_day_pnl:.2f}")
        analysis_logger.info(f"Best Day            : ₹{self.best_day:.2f}")
        analysis_logger.info(f"Worst Day           : ₹{self.worst_day:.2f}")

        analysis_logger.info("\n🔁 3️⃣ STREAK ANALYSIS")
        analysis_logger.info(f"Max Winning Trades in a Row : {self.max_win_streak}")
        analysis_logger.info(f"Max Losing Trades in a Row  : {self.max_loss_streak}")
        analysis_logger.info(f"Avg Winning Streak          : {self.avg_win_streak:.1f}")
        analysis_logger.info(f"Avg Losing Streak           : {self.avg_loss_streak:.1f}")
        analysis_logger.info(f"Max Winning Days in a Row   : {self.max_win_day_streak}")
        analysis_logger.info(f"Max Losing Days in a Row    : {self.max_loss_day_streak}")

        analysis_logger.info("\n📉 4️⃣ EQUITY & DRAWDOWN")
        analysis_logger.info(f"Peak Equity     : ₹{self.peak_equity:.2f}")
        analysis_logger.info(f"Final Equity    : ₹{self.final_equity:.2f}")
        analysis_logger.info(f"Max Drawdown    : ₹{self.max_drawdown:.2f}")
        analysis_logger.info(f"Max Drawdown %  : {self.max_drawdown_pct:.1f}%")

        analysis_logger.info("\n------------------------------")
        analysis_logger.info(f"Brokerage (Est)  : ₹{self.brokerage:.2f}  (@ ₹{self.brokerage_per_order}/order)")
        analysis_logger.info("------------------------------")
        analysis_logger.info(f"💰 NET PnL (After Charges): ₹{self.net_pnl:.2f}")
        
        analysis_logger.info("\n📊 5️⃣ CAPITAL GROWTH")
        analysis_logger.info(f"Initial Capital            : ₹{INITIAL_CAPITAL:,.2f}")   
        analysis_logger.info(f"Final Portfolio Value      : ₹{self.final_execution_value:,.2f}  (Growth: {self.pnl_growth_pct:+.2f}%)")
        analysis_logger.info(f"Max Drawdown (₹)           : ₹{self.max_drawdown:,.2f}")
        analysis_logger.info(f"Max Drawdown (%)           : {self.max_drawdown_pct:.2f}%")

        # analysis_logger.info("\n🚀 OPPORTUNITY PERFORMANCE (Potential)")
        # analysis_logger.info(f"Final Opportunity Value    : ₹{self.final_opportunity_value:,.2f}  (Growth: {self.opp_growth_pct:+.2f}%)")
        # analysis_logger.info(f"Max Opportunity Drawdown   : ₹{self.max_opp_drawdown:,.2f}")
        # analysis_logger.info(f"Max Opportunity DD (%)     : {self.opportunity_dd_pct:.2f}%")

        # analysis_logger.info("\n🎯 MISSED OPPORTUNITY")
        # analysis_logger.info(f"Total Missed PnL           : ₹{self.total_missed_pnl:,.2f}")
        # analysis_logger.info(f"Avg Missed Per Trade       : ₹{self.avg_missed_pnl:,.2f}")
        analysis_logger.info("=" * 50)


# -------------------------------
# main entry point
# -------------------------------
def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_dir, "myalgo.db")

    analyzer = BacktestingAnalysis(db_path=db_path)
    analyzer.load_trade_legs()
    analyzer.build_trades()
    analyzer.analyze()
    analyzer.print_report()


if __name__ == "__main__":
    main()
