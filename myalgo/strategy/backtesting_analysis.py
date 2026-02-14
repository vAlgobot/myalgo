import sqlite3
import pandas as pd
import os
import uuid
import math
from logger import get_logger

analysis_logger = get_logger("backtest_analysis")

INITIAL_CAPITAL = 50000

class BacktestingAnalysis:
    def __init__(
        self,
        db_path: str,
        brokerage_per_order: float = 25,
        strategy_name: str = "default_strategy",
        symbol: str = None,
        timeframe: str = "5m",
    ):
        self.db_path = db_path
        self.brokerage_per_order = brokerage_per_order
        self.strategy_name = strategy_name
        self.symbol = symbol
        self.timeframe = timeframe

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
        self.profit_factor = (self.gross_profit / gross_loss_abs) if gross_loss_abs > 0 else 0

        # ---- Opportunity Analysis ----
        self.analyze_opportunity()

    @staticmethod
    def _safe_float(value, default=0.0):
        if pd.isna(value):
            return default
        return float(value)

    def _ensure_strategy_runs_table(self, conn: sqlite3.Connection):
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_runs (
                run_id TEXT PRIMARY KEY,
                strategy_name TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                initial_capital REAL NOT NULL,
                final_equity REAL NOT NULL,
                net_pnl REAL NOT NULL,
                growth_percent REAL NOT NULL,
                total_trades INTEGER NOT NULL DEFAULT 0,
                winning_trades INTEGER NOT NULL DEFAULT 0,
                losing_trades INTEGER NOT NULL DEFAULT 0,
                breakeven_trades INTEGER NOT NULL DEFAULT 0,
                win_rate REAL NOT NULL DEFAULT 0.0,
                gross_profit REAL NOT NULL DEFAULT 0.0,
                gross_loss REAL NOT NULL DEFAULT 0.0,
                avg_trade REAL NOT NULL DEFAULT 0.0,
                avg_win REAL NOT NULL DEFAULT 0.0,
                avg_loss REAL NOT NULL DEFAULT 0.0,
                risk_reward REAL NOT NULL DEFAULT 0.0,
                profit_factor REAL NOT NULL DEFAULT 0.0,
                expectancy REAL NOT NULL DEFAULT 0.0,
                max_drawdown REAL NOT NULL DEFAULT 0.0,
                max_drawdown_percent REAL NOT NULL DEFAULT 0.0,
                return_over_drawdown REAL NOT NULL DEFAULT 0.0,
                sharpe_ratio REAL NOT NULL DEFAULT 0.0,
                avg_risk_per_trade REAL NOT NULL DEFAULT 0.0,
                max_win_streak INTEGER NOT NULL DEFAULT 0,
                max_loss_streak INTEGER NOT NULL DEFAULT 0,
                trading_days INTEGER NOT NULL DEFAULT 0,
                win_day_percent REAL NOT NULL DEFAULT 0.0,
                best_day REAL NOT NULL DEFAULT 0.0,
                worst_day REAL NOT NULL DEFAULT 0.0,
                brokerage REAL NOT NULL DEFAULT 0.0,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CHECK (end_date >= start_date),
                CHECK (winning_trades + losing_trades + breakeven_trades <= total_trades)
            )
            """
        )

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_strategy_runs_strategy_name ON strategy_runs(strategy_name)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_strategy_runs_symbol ON strategy_runs(symbol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_strategy_runs_timeframe ON strategy_runs(timeframe)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_strategy_runs_start_date ON strategy_runs(start_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_strategy_runs_end_date ON strategy_runs(end_date)")

    def _build_strategy_run_row(self):
        start_date = self.trade_df["date"].min().isoformat()
        end_date = self.trade_df["date"].max().isoformat()

        symbol = self.symbol
        if not symbol and self.raw_df is not None and "symbol" in self.raw_df.columns and not self.raw_df.empty:
            symbol = str(self.raw_df["symbol"].mode().iloc[0])
        symbol = symbol or "UNKNOWN"

        total_trades = int(self.total_trades)
        winning_trades = int(len(self.wins))
        losing_trades = int(len(self.losses))
        breakeven_trades = int(len(self.breakeven))

        # Expectancy per trade in absolute PnL terms.
        expectancy = self._safe_float(self.avg_trade)

        max_drawdown_abs = abs(self._safe_float(self.max_drawdown))
        return_over_drawdown = (self._safe_float(self.net_pnl) / max_drawdown_abs) if max_drawdown_abs > 0 else 0.0

        trade_returns = pd.Series(dtype=float)
        if "capital" in self.trade_df.columns:
            capital = self.trade_df["capital"].replace(0, pd.NA)
            trade_returns = (self.trade_df["pnl"] / capital).dropna()

        if len(trade_returns) >= 2:
            ret_std = trade_returns.std(ddof=1)
            sharpe_ratio = (math.sqrt(len(trade_returns)) * trade_returns.mean() / ret_std) if ret_std > 0 else 0.0
        else:
            sharpe_ratio = 0.0

        row = {
            "run_id": str(uuid.uuid4()),
            "strategy_name": self.strategy_name,
            "symbol": symbol,
            "timeframe": self.timeframe,
            "start_date": start_date,
            "end_date": end_date,
            "initial_capital": self._safe_float(INITIAL_CAPITAL),
            "final_equity": self._safe_float(self.final_execution_value),
            "net_pnl": self._safe_float(self.net_pnl),
            "growth_percent": self._safe_float(self.pnl_growth_pct),
            "total_trades": total_trades,
            "winning_trades": winning_trades,
            "losing_trades": losing_trades,
            "breakeven_trades": breakeven_trades,
            "win_rate": self._safe_float(self.win_rate),
            "gross_profit": self._safe_float(self.gross_profit),
            "gross_loss": self._safe_float(self.gross_loss),
            "avg_trade": self._safe_float(self.avg_trade),
            "avg_win": self._safe_float(self.avg_win),
            "avg_loss": self._safe_float(self.avg_loss),
            "risk_reward": self._safe_float(self.risk_reward_ratio),
            "profit_factor": self._safe_float(self.profit_factor),
            "expectancy": expectancy,
            "max_drawdown": self._safe_float(self.max_drawdown),
            "max_drawdown_percent": self._safe_float(self.max_drawdown_pct),
            "return_over_drawdown": self._safe_float(return_over_drawdown),
            "sharpe_ratio": self._safe_float(sharpe_ratio),
            "avg_risk_per_trade": self._safe_float(abs(self.avg_loss)),
            "max_win_streak": int(self.max_win_streak),
            "max_loss_streak": int(self.max_loss_streak),
            "trading_days": int(self.trading_days),
            "win_day_percent": self._safe_float(self.win_day_pct),
            "best_day": self._safe_float(self.best_day),
            "worst_day": self._safe_float(self.worst_day),
            "brokerage": self._safe_float(self.brokerage),
        }
        return row

    def save_strategy_run(self):
        row = self._build_strategy_run_row()

        columns = [
            "run_id", "strategy_name", "symbol", "timeframe", "start_date", "end_date",
            "initial_capital", "final_equity", "net_pnl", "growth_percent",
            "total_trades", "winning_trades", "losing_trades", "breakeven_trades", "win_rate",
            "gross_profit", "gross_loss", "avg_trade", "avg_win", "avg_loss", "risk_reward",
            "profit_factor", "expectancy", "max_drawdown", "max_drawdown_percent",
            "return_over_drawdown", "sharpe_ratio", "avg_risk_per_trade", "max_win_streak",
            "max_loss_streak", "trading_days", "win_day_percent", "best_day", "worst_day",
            "brokerage"
        ]

        placeholders = ", ".join(["?"] * len(columns))
        values = [row[c] for c in columns]

        conn = sqlite3.connect(self.db_path)
        try:
            self._ensure_strategy_runs_table(conn)
            conn.execute(
                f"INSERT INTO strategy_runs ({', '.join(columns)}) VALUES ({placeholders})",
                values,
            )
            conn.commit()
            analysis_logger.info(
                f"✅ Saved strategy run summary to strategy_runs (run_id={row['run_id']})"
            )
        finally:
            conn.close()

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
        analysis_logger.warning("\n🧮 1️⃣ TRADE-LEVEL PERFORMANCE (GROSS)")
        analysis_logger.warning(f"Total Trades        : {self.total_trades}")
        analysis_logger.warning(f"Winning Trades      : {len(self.wins)} ({len(self.wins)/self.total_trades*100:.1f}%)")
        analysis_logger.warning(f"Losing Trades       : {len(self.losses)} ({len(self.losses)/self.total_trades*100:.1f}%)")
        analysis_logger.warning(f"Breakeven Trades    : {len(self.breakeven)} ({len(self.breakeven)/self.total_trades*100:.1f}%)")
        analysis_logger.warning(f"Win Rate (W/(W+L))  : {self.win_rate:.1f}%")
        analysis_logger.warning(f"Gross Profit        : ₹{self.gross_profit:.2f}")
        analysis_logger.warning(f"Gross Loss          : ₹{self.gross_loss:.2f}")
        analysis_logger.warning(f"Gross PnL           : ₹{self.gross_pnl:.2f}")
        analysis_logger.warning(f"Avg Trade           : ₹{self.avg_trade:.2f}")
        analysis_logger.warning(f"Avg Win             : ₹{self.avg_win:.2f}")
        analysis_logger.warning(f"Avg Loss            : ₹{self.avg_loss:.2f}")
        analysis_logger.warning(f"Avg Breakeven       : ₹{self.breakeven['pnl'].mean() if not self.breakeven.empty else 0:.2f}")
        analysis_logger.warning(f"Avg Capital Deployed: ₹{self.avg_capital:.2f}")
        analysis_logger.warning(f"Risk:Reward Ratio   : 1:{self.risk_reward_ratio:.2f}")
        analysis_logger.warning(f"Profit Factor       : {self.profit_factor:.2f}")
        
        # 🎯 Exit reason breakdown
        analysis_logger.warning("\n🎯 EXIT REASON BREAKDOWN")
        exit_reasons = self.trade_df['exit_reason'].value_counts()
        for reason, count in exit_reasons.items():
            pnl = self.trade_df[self.trade_df['exit_reason'] == reason]['pnl'].sum()
            avg_pnl = self.trade_df[self.trade_df['exit_reason'] == reason]['pnl'].mean()
            analysis_logger.warning(f"  {reason:<20} : {count:>2} trades | Total PnL: ₹{pnl:>9.2f} | Avg: ₹{avg_pnl:>7.2f}")

        analysis_logger.warning("\n📆 2️⃣ DAY-LEVEL PERFORMANCE")
        analysis_logger.warning(f"Trading Days        : {self.trading_days}")
        analysis_logger.warning(f"Winning Days        : {self.winning_days}")
        analysis_logger.warning(f"Losing Days         : {self.losing_days}")
        analysis_logger.warning(f"Win-Day %           : {self.win_day_pct:.1f}%")
        analysis_logger.warning(f"Avg Day PnL         : ₹{self.avg_day_pnl:.2f}")
        analysis_logger.warning(f"Best Day            : ₹{self.best_day:.2f}")
        analysis_logger.warning(f"Worst Day           : ₹{self.worst_day:.2f}")

        analysis_logger.warning("\n🔁 3️⃣ STREAK ANALYSIS")
        analysis_logger.warning(f"Max Winning Trades in a Row : {self.max_win_streak}")
        analysis_logger.warning(f"Max Losing Trades in a Row  : {self.max_loss_streak}")
        analysis_logger.warning(f"Avg Winning Streak          : {self.avg_win_streak:.1f}")
        analysis_logger.warning(f"Avg Losing Streak           : {self.avg_loss_streak:.1f}")
        analysis_logger.warning(f"Max Winning Days in a Row   : {self.max_win_day_streak}")
        analysis_logger.warning(f"Max Losing Days in a Row    : {self.max_loss_day_streak}")

        analysis_logger.warning("\n📉 4️⃣ EQUITY & DRAWDOWN")
        analysis_logger.warning(f"Peak Equity     : ₹{self.peak_equity:.2f}")
        analysis_logger.warning(f"Final Equity    : ₹{self.final_equity:.2f}")
        analysis_logger.warning(f"Max Drawdown    : ₹{self.max_drawdown:.2f}")
        analysis_logger.warning(f"Max Drawdown %  : {self.max_drawdown_pct:.1f}%")

        analysis_logger.warning("\n------------------------------")
        analysis_logger.warning(f"Brokerage (Est)  : ₹{self.brokerage:.2f}  (@ ₹{self.brokerage_per_order}/order)")
        analysis_logger.warning("------------------------------")
        analysis_logger.warning(f"💰 NET PnL (After Charges): ₹{self.net_pnl:.2f}")
        
        analysis_logger.warning("\n📊 5️⃣ CAPITAL GROWTH")
        analysis_logger.warning(f"Initial Capital            : ₹{INITIAL_CAPITAL:,.2f}")   
        analysis_logger.warning(f"Final Portfolio Value      : ₹{self.final_execution_value:,.2f}  (Growth: {self.pnl_growth_pct:+.2f}%)")
        analysis_logger.warning(f"Max Drawdown (₹)           : ₹{self.max_drawdown:,.2f}")
        analysis_logger.warning(f"Max Drawdown (%)           : {self.max_drawdown_pct:.2f}%")

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
    analyzer.save_strategy_run()
    analyzer.print_report()


if __name__ == "__main__":
    main()
