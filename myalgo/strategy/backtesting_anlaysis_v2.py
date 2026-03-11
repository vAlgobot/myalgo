import sqlite3
import pandas as pd
import numpy as np
import os
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# ===============================
# CONFIGURATION
# ===============================

INITIAL_CAPITAL = 50000
BROKERAGE_PER_ORDER = 25
MIN_TRADES_PER_PERIOD = 1  # Minimum trades required for yearly/monthly analysis

# Slippage Configuration
SLIPPAGE_MODE = "PERCENT"   # Options: "PERCENT" or None
SLIPPAGE_PERCENT = 0.5      # 1.0% per side

# Export Configuration
EXPORT_EXCEL = True
EXPORT_CHARTS = True
OUTPUT_BASE_DIR = "backtest_results"  # Base directory for all results

# Display Configuration
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)

# ===============================
# METRIC ENGINE (ENHANCED)
# ===============================

class MetricEngine:
    """
    Enhanced metric calculation engine with all corrections applied.
    
    Key Corrections Made:
    1. Profit factor now uses NET pnl (after brokerage/slippage)
    2. Sharpe ratio uses daily equity returns, not daily pnl
    3. Brokerage included in equity curve calculation
    4. Added all missing metrics from single analyzer
    """

    @staticmethod
    def compute_metrics(trade_df: pd.DataFrame, period_label: str = "overall") -> Dict:
        """
        Compute comprehensive performance metrics for a set of trades.
        
        Args:
            trade_df: DataFrame with trade data
            period_label: Label for this period (for debugging)
            
        Returns:
            Dictionary with all performance metrics
        """
        
        # Initialize return dictionary with safe defaults
        required_keys = {
            "net_pnl": 0.0,
            "growth_pct": 0.0,
            "total_trades": 0,
            "winning_trades": 0,
            "losing_trades": 0,
            "breakeven_trades": 0,
            "win_rate": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "avg_trade": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "rr_ratio": 0.0,
            "profit_factor": 0.0,
            "expectancy": 0.0,
            "max_dd_pct": 0.0,
            "max_dd_amt": 0.0,
            "sharpe": 0.0,
            "rodd": 0.0,
            "max_win_streak": 0,
            "max_loss_streak": 0,
            "trading_days": 0,
            "winning_days": 0,
            "losing_days": 0,
            "win_day_pct": 0.0,
            "avg_day_pnl": 0.0,
            "best_day_pnl": 0.0,
            "worst_day_pnl": 0.0,
        }

        if trade_df is None or trade_df.empty:
            return required_keys.copy()

        df = trade_df.copy().sort_values("exit_time").reset_index(drop=True)

        # -----------------------------
        # Apply Brokerage & Slippage
        # -----------------------------
        brokerage_per_trade = 2 * BROKERAGE_PER_ORDER
        df["brokerage"] = brokerage_per_trade
        df["pnl_net"] = df["pnl"] - df["brokerage"]

        # -----------------------------
        # Basic Trade Statistics
        # -----------------------------
        total_trades = len(df)
        
        # Classify trades based on NET pnl
        wins = df[df["pnl_net"] > 0]
        losses = df[df["pnl_net"] < 0]
        breakeven = df[df["pnl_net"] == 0]

        winning_trades = len(wins)
        losing_trades = len(losses)
        breakeven_trades = len(breakeven)

        # Gross profit/loss (NET after costs)
        gross_profit = float(wins["pnl_net"].sum())
        gross_loss = float(losses["pnl_net"].sum())
        net_pnl = float(df["pnl_net"].sum())

        # Averages
        avg_trade = float(df["pnl_net"].mean()) if total_trades > 0 else 0.0
        avg_win = float(wins["pnl_net"].mean()) if not wins.empty else 0.0
        avg_loss = float(losses["pnl_net"].mean()) if not losses.empty else 0.0

        # Win rate
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0.0

        # Risk-Reward Ratio
        rr_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0.0

        # -----------------------------
        # Equity Curve (NET - with brokerage)
        # -----------------------------
        df["cum_pnl"] = df["pnl_net"].cumsum()
        df["equity"] = INITIAL_CAPITAL + df["cum_pnl"]

        # Peak tracking
        df["peak"] = df["equity"].cummax()
        df["drawdown"] = df["equity"] - df["peak"]

        # Maximum drawdown
        max_dd_amt = float(df["drawdown"].min())
        max_dd_amt = abs(max_dd_amt)

        # Correct drawdown percentage calculation
        df["dd_pct"] = np.where(df["peak"] != 0, (df["drawdown"] / df["peak"]) * 100, 0)
        max_dd_pct = float(abs(df["dd_pct"].min()))

        # -----------------------------
        # Profit Factor (NET)
        # CORRECTED: Now uses NET pnl after brokerage
        # -----------------------------
        profit_factor = abs(gross_profit / gross_loss) if gross_loss != 0 else 0.0

        # Expectancy (NET)
        expectancy = avg_trade

        # -----------------------------
        # Sharpe Ratio (CORRECTED)
        # Now uses daily equity returns instead of daily pnl sum
        # -----------------------------
        df["date"] = df["exit_time"].dt.date
        
        # Daily equity level (last equity value of each day)
        daily_equity = df.groupby("date")["equity"].last()
        
        # Daily returns calculation
        daily_returns = daily_equity.pct_change().dropna()

        if len(daily_returns) > 1 and daily_returns.std() != 0:
            sharpe = np.sqrt(252) * daily_returns.mean() / daily_returns.std()
        else:
            sharpe = 0.0

        # -----------------------------
        # Return over Drawdown (NET)
        # -----------------------------
        rodd = (net_pnl / max_dd_amt) if max_dd_amt != 0 else 0.0

        # Growth percentage
        growth_pct = (net_pnl / INITIAL_CAPITAL) * 100

        # -----------------------------
        # Streak Analysis (NEW)
        # -----------------------------
        df["is_win"] = df["pnl_net"] > 0
        df["is_loss"] = df["pnl_net"] < 0
        
        max_win_streak = MetricEngine._calculate_max_streak(df["is_win"])
        max_loss_streak = MetricEngine._calculate_max_streak(df["is_loss"])

        # -----------------------------
        # Day-Level Performance (NEW)
        # -----------------------------
        daily_pnl = df.groupby("date")["pnl_net"].sum()
        
        trading_days = len(daily_pnl)
        winning_days = (daily_pnl > 0).sum()
        losing_days = (daily_pnl < 0).sum()
        win_day_pct = (winning_days / trading_days * 100) if trading_days > 0 else 0.0
        
        avg_day_pnl = float(daily_pnl.mean()) if trading_days > 0 else 0.0
        best_day_pnl = float(daily_pnl.max()) if trading_days > 0 else 0.0
        worst_day_pnl = float(daily_pnl.min()) if trading_days > 0 else 0.0

        return {
            "net_pnl": float(net_pnl),
            "growth_pct": float(growth_pct),
            "total_trades": int(total_trades),
            "winning_trades": int(winning_trades),
            "losing_trades": int(losing_trades),
            "breakeven_trades": int(breakeven_trades),
            "win_rate": float(win_rate),
            "gross_profit": float(gross_profit),
            "gross_loss": float(gross_loss),
            "avg_trade": float(avg_trade),
            "avg_win": float(avg_win),
            "avg_loss": float(avg_loss),
            "rr_ratio": float(rr_ratio),
            "profit_factor": float(profit_factor),
            "expectancy": float(expectancy),
            "max_dd_pct": float(max_dd_pct),
            "max_dd_amt": float(max_dd_amt),
            "sharpe": float(sharpe),
            "rodd": float(rodd),
            "max_win_streak": int(max_win_streak),
            "max_loss_streak": int(max_loss_streak),
            "trading_days": int(trading_days),
            "winning_days": int(winning_days),
            "losing_days": int(losing_days),
            "win_day_pct": float(win_day_pct),
            "avg_day_pnl": float(avg_day_pnl),
            "best_day_pnl": float(best_day_pnl),
            "worst_day_pnl": float(worst_day_pnl),
        }

    @staticmethod
    def _calculate_max_streak(boolean_series: pd.Series) -> int:
        """Calculate maximum consecutive True values in a boolean series."""
        if boolean_series.empty:
            return 0
            
        streaks = []
        current = 0
        
        for val in boolean_series:
            if val:
                current += 1
            else:
                if current > 0:
                    streaks.append(current)
                current = 0
        
        if current > 0:
            streaks.append(current)
        
        return max(streaks) if streaks else 0


# ===============================
# EXIT REASON ANALYZER (NEW)
# ===============================

class ExitReasonAnalyzer:
    """Analyzes exit reason statistics from trades."""
    
    @staticmethod
    def analyze_exit_reasons(trade_df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze exit reasons from trade data.
        
        Args:
            trade_df: DataFrame with trade data including 'reason' and 'pnl_net'
            
        Returns:
            DataFrame with exit reason statistics
        """
        if trade_df is None or trade_df.empty or "reason" not in trade_df.columns:
            return pd.DataFrame(columns=["reason", "count", "total_pnl", "avg_pnl"])
        
        df = trade_df.copy()
        
        # Ensure pnl_net exists
        if "pnl_net" not in df.columns:
            brokerage_per_trade = 2 * BROKERAGE_PER_ORDER
            df["pnl_net"] = df["pnl"] - brokerage_per_trade
        
        # Group by reason
        reason_stats = df.groupby("reason").agg({
            "pnl_net": ["count", "sum", "mean"]
        }).reset_index()
        
        reason_stats.columns = ["reason", "count", "total_pnl", "avg_pnl"]
        reason_stats = reason_stats.sort_values("total_pnl", ascending=False)
        
        return reason_stats


# ===============================
# SUPER BACKTEST ANALYZER (ENHANCED)
# ===============================

class SuperBacktestAnalyzer:
    """
    Enhanced multi-strategy backtesting analyzer.
    
    Improvements over original:
    1. Corrected all metric formulas
    2. Added missing features from single analyzer
    3. Exit reason analysis
    4. Excel export with formatting
    5. Visualization dashboard
    6. Database storage with run tracking
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create organized output directory structure
        # Format: backtest_results/YYYY-MM-DD/HH-MM-SS/
        if EXPORT_EXCEL or EXPORT_CHARTS:
            current_date = datetime.now().strftime("%Y-%m-%d")
            current_time = datetime.now().strftime("%H-%M-%S")
            
            # Create hierarchical structure: base/date/time/
            self.output_dir = os.path.join(OUTPUT_BASE_DIR, current_date, current_time)
            os.makedirs(self.output_dir, exist_ok=True)
            
            # Also create subdirectories for better organization
            self.charts_dir = os.path.join(self.output_dir, "charts")
            self.data_dir = os.path.join(self.output_dir, "data")
            os.makedirs(self.charts_dir, exist_ok=True)
            os.makedirs(self.data_dir, exist_ok=True)
            
            print(f"📁 Output directory: {self.output_dir}")
        else:
            self.output_dir = None
            self.charts_dir = None
            self.data_dir = None

    # -----------------------------------
    # LOAD TRADES
    # -----------------------------------
    def load_trades(self, strategy: str) -> Optional[pd.DataFrame]:
        """Load and process trades for a strategy with slippage."""
        
        df = pd.read_sql(
            """
            SELECT timestamp, option_price, quantity, leg_type, strategy, reason
            FROM my_trades
            WHERE leg_type IN ('ENTRY','EXIT')
            AND strategy = ?
            ORDER BY timestamp
            """,
            self.conn,
            params=(strategy,),
        )

        if df.empty:
            return None

        df["timestamp"] = pd.to_datetime(df["timestamp"])

        trades = []
        open_trade = None

        for _, row in df.iterrows():
            if row["leg_type"] == "ENTRY":
                open_trade = row

            elif row["leg_type"] == "EXIT" and open_trade is not None:
                entry_price = float(open_trade["option_price"])
                exit_price = float(row["option_price"])
                qty = float(row["quantity"])

                # Apply Slippage (Long Options)
                if SLIPPAGE_MODE == "PERCENT":
                    slip_factor = SLIPPAGE_PERCENT / 100
                    adjusted_entry = entry_price * (1 + slip_factor)
                    adjusted_exit = exit_price * (1 - slip_factor)
                else:
                    adjusted_entry = entry_price
                    adjusted_exit = exit_price

                pnl = (adjusted_exit - adjusted_entry) * qty

                trades.append({
                    "exit_time": row["timestamp"],
                    "pnl": pnl,
                    "reason": row.get("reason", "Unknown"),
                })

                open_trade = None

        return pd.DataFrame(trades) if trades else None

    # -----------------------------------
    # MAIN ANALYSIS RUNNER
    # -----------------------------------
    def run(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Run comprehensive multi-strategy analysis.
        
        Returns:
            Tuple of (ranking_df, yearly_df, monthly_df, exit_reason_df)
        """
        
        print("\n" + "="*80)
        print("🚀 ENHANCED MASTER BACKTEST ANALYZER V2")
        print("="*80)
        print(f"Run ID: {self.run_id}")
        print(f"Configuration:")
        print(f"  - Initial Capital: ₹{INITIAL_CAPITAL:,.0f}")
        print(f"  - Brokerage per Order: ₹{BROKERAGE_PER_ORDER}")
        print(f"  - Slippage Mode: {SLIPPAGE_MODE}")
        if SLIPPAGE_MODE == "PERCENT":
            print(f"  - Slippage: {SLIPPAGE_PERCENT}% per side")
        print("="*80 + "\n")

        strategies = pd.read_sql(
            "SELECT DISTINCT strategy FROM my_trades WHERE leg_type IN ('ENTRY','EXIT')",
            self.conn
        )["strategy"].tolist()

        overall_rows = []
        yearly_rows = []
        monthly_rows = []
        all_exit_reasons = []

        for strategy in strategies:
            print(f"📊 Analyzing: {strategy}...")
            
            trade_df = self.load_trades(strategy)
            if trade_df is None or trade_df.empty:
                print(f"  ⚠️  No completed trades found")
                continue

            # Overall metrics
            overall_metrics = MetricEngine.compute_metrics(trade_df, "overall")
            overall_metrics["strategy"] = strategy
            overall_rows.append(overall_metrics)

            # Time period columns
            trade_df["year"] = trade_df["exit_time"].dt.year
            trade_df["year_month"] = trade_df["exit_time"].dt.to_period("M")

            # Yearly breakdown
            for year, group in trade_df.groupby("year"):
                if len(group) >= MIN_TRADES_PER_PERIOD:
                    m = MetricEngine.compute_metrics(group, f"year_{year}")
                    m["strategy"] = strategy
                    m["year"] = year
                    yearly_rows.append(m)

            # Monthly breakdown
            for ym, group in trade_df.groupby("year_month"):
                if len(group) >= MIN_TRADES_PER_PERIOD:
                    m = MetricEngine.compute_metrics(group, f"month_{ym}")
                    m["strategy"] = strategy
                    m["year_month"] = str(ym)
                    monthly_rows.append(m)

            # Exit reason analysis
            reason_stats = ExitReasonAnalyzer.analyze_exit_reasons(trade_df)
            if not reason_stats.empty:
                reason_stats["strategy"] = strategy
                all_exit_reasons.append(reason_stats)

            print(f"  ✅ Completed: {len(trade_df)} trades analyzed")

        # Create DataFrames
        overall_df = pd.DataFrame(overall_rows)
        yearly_df = pd.DataFrame(yearly_rows)
        monthly_df = pd.DataFrame(monthly_rows)
        exit_reason_df = pd.concat(all_exit_reasons, ignore_index=True) if all_exit_reasons else pd.DataFrame()

        # Rank strategies
        ranking_df = self.rank_strategies(overall_df, yearly_df)

        # Display results
        self.display_tables(ranking_df, yearly_df, monthly_df, exit_reason_df)

        # Export to Excel
        if EXPORT_EXCEL:
            self.export_to_excel(ranking_df, yearly_df, monthly_df, exit_reason_df)

        # Generate visualizations
        if EXPORT_CHARTS:
            self.generate_visualizations(ranking_df, yearly_df, monthly_df, exit_reason_df)

        # Save to database
        self.save_to_database(ranking_df, yearly_df, monthly_df, exit_reason_df)

        return ranking_df, yearly_df, monthly_df, exit_reason_df

    # -----------------------------------
    # RANKING ENGINE
    # -----------------------------------
    def rank_strategies(self, overall_df: pd.DataFrame, yearly_df: pd.DataFrame) -> pd.DataFrame:
        """
        Rank strategies using composite scoring.
        
        Ranking methodology:
        - Sharpe ratio: 30% weight (risk-adjusted returns)
        - RoDD: 25% weight (return over drawdown)
        - Profit factor: 15% weight (gross profit/loss ratio)
        - Expectancy: 10% weight (average profit per trade)
        - Drawdown: 10% weight (risk management)
        - Consistency: 10% weight (year-to-year stability)
        """

        if overall_df.empty:
            return overall_df

        # Calculate consistency score
        if yearly_df.empty or len(yearly_df) <= 1:
            overall_df["consistency"] = 0.0
        else:
            # Filter years with sufficient trades
            filtered_years = yearly_df[yearly_df["total_trades"] >= MIN_TRADES_PER_PERIOD]
            
            consistency_dict = {}
            for strategy, group in filtered_years.groupby("strategy"):
                if len(group) <= 1:
                    consistency_dict[strategy] = 0.0
                else:
                    # Weighted standard deviation of growth_pct
                    weights = group["total_trades"]
                    mean = np.average(group["growth_pct"], weights=weights)
                    variance = np.average((group["growth_pct"] - mean) ** 2, weights=weights)
                    consistency_dict[strategy] = float(np.sqrt(variance))

            overall_df["consistency"] = overall_df["strategy"].map(consistency_dict).fillna(0.0)

        # Normalization function
        def normalize(series):
            if series.max() == series.min():
                return pd.Series(0.5, index=series.index)
            return (series - series.min()) / (series.max() - series.min())

        # Normalize metrics
        overall_df["sharpe_n"] = normalize(overall_df["sharpe"])
        overall_df["rodd_n"] = normalize(overall_df["rodd"])
        overall_df["pf_n"] = normalize(overall_df["profit_factor"])
        overall_df["exp_n"] = normalize(overall_df["expectancy"])
        overall_df["dd_n"] = 1 - normalize(overall_df["max_dd_pct"])  # Lower DD is better
        overall_df["cons_n"] = 1 - normalize(overall_df["consistency"])  # Lower variance is better

        # Composite score calculation
        overall_df["composite"] = (
            overall_df["sharpe_n"] * 0.30 +
            overall_df["rodd_n"] * 0.25 +
            overall_df["pf_n"] * 0.15 +
            overall_df["exp_n"] * 0.10 +
            overall_df["dd_n"] * 0.10 +
            overall_df["cons_n"] * 0.10
        )

        # Sort and rank
        overall_df = overall_df.sort_values("composite", ascending=False).reset_index(drop=True)
        overall_df["rank"] = overall_df.index + 1

        return overall_df

    # -----------------------------------
    # DISPLAY TABLES
    # -----------------------------------
    def display_tables(
        self, 
        ranking_df: pd.DataFrame, 
        yearly_df: pd.DataFrame, 
        monthly_df: pd.DataFrame,
        exit_reason_df: pd.DataFrame
    ):
        """Display formatted result tables in console."""

        print("\n" + "="*80)
        print("🏆 FINAL STRATEGY RANKING")
        print("="*80)
        if not ranking_df.empty:
            display_cols = [
                "rank", "strategy", "composite", "net_pnl", "growth_pct",
                "sharpe", "rodd", "profit_factor", "rr_ratio",
                "max_dd_pct", "max_dd_amt", "consistency"
            ]
            print(ranking_df[display_cols].to_string(index=False))
        else:
            print("No data available")

        print("\n" + "="*80)
        print("📆 YEARLY BREAKDOWN")
        print("="*80)
        if not yearly_df.empty:
            display_cols = [
                "strategy", "year", "net_pnl", "growth_pct", "total_trades",
                "win_rate", "profit_factor", "sharpe", "max_dd_pct"
            ]
            print(yearly_df[display_cols].sort_values(["strategy", "year"]).to_string(index=False))
        else:
            print("No yearly data available")

        print("\n" + "="*80)
        print("📅 MONTHLY BREAKDOWN")
        print("="*80)
        if not monthly_df.empty:
            display_cols = [
                "strategy", "year_month", "net_pnl", "growth_pct", "total_trades",
                "win_rate", "profit_factor", "sharpe", "max_dd_pct"
            ]
            print(monthly_df[display_cols].sort_values(["strategy", "year_month"]).to_string(index=False))
        else:
            print("No monthly data available")

        print("\n" + "="*80)
        print("🧾 EXIT REASON SUMMARY")
        print("="*80)
        if not exit_reason_df.empty:
            print(exit_reason_df.to_string(index=False))
        else:
            print("No exit reason data available")

        print("\n" + "="*80)

    # -----------------------------------
    # EXCEL EXPORT
    # -----------------------------------
    def export_to_excel(
        self,
        ranking_df: pd.DataFrame,
        yearly_df: pd.DataFrame,
        monthly_df: pd.DataFrame,
        exit_reason_df: pd.DataFrame
    ):
        """Export results to formatted Excel file."""
        
        filename = os.path.join(self.data_dir, f"backtest_results_{self.run_id}.xlsx")
        
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                
                # Strategy Ranking
                if not ranking_df.empty:
                    ranking_df.to_excel(writer, sheet_name='Strategy_Ranking', index=False)
                    worksheet = writer.sheets['Strategy_Ranking']
                    worksheet.freeze_panes = 'A2'
                    
                    # Format numbers
                    for col in worksheet.iter_cols(min_row=2, max_row=worksheet.max_row):
                        for cell in col:
                            if isinstance(cell.value, (int, float)):
                                cell.number_format = '#,##0.00'
                
                # Yearly Breakdown
                if not yearly_df.empty:
                    yearly_df.to_excel(writer, sheet_name='Yearly_Breakdown', index=False)
                    worksheet = writer.sheets['Yearly_Breakdown']
                    worksheet.freeze_panes = 'A2'
                    
                    for col in worksheet.iter_cols(min_row=2, max_row=worksheet.max_row):
                        for cell in col:
                            if isinstance(cell.value, (int, float)):
                                cell.number_format = '#,##0.00'
                
                # Monthly Breakdown
                if not monthly_df.empty:
                    monthly_df.to_excel(writer, sheet_name='Monthly_Breakdown', index=False)
                    worksheet = writer.sheets['Monthly_Breakdown']
                    worksheet.freeze_panes = 'A2'
                    
                    for col in worksheet.iter_cols(min_row=2, max_row=worksheet.max_row):
                        for cell in col:
                            if isinstance(cell.value, (int, float)):
                                cell.number_format = '#,##0.00'
                
                # Exit Reason Summary
                if not exit_reason_df.empty:
                    exit_reason_df.to_excel(writer, sheet_name='Exit_Reason_Summary', index=False)
                    worksheet = writer.sheets['Exit_Reason_Summary']
                    worksheet.freeze_panes = 'A2'
                    
                    for col in worksheet.iter_cols(min_row=2, max_row=worksheet.max_row):
                        for cell in col:
                            if isinstance(cell.value, (int, float)):
                                cell.number_format = '#,##0.00'
                
                # Config sheet
                config_data = {
                    'Parameter': ['Run ID', 'Initial Capital', 'Brokerage per Order', 
                                 'Slippage Mode', 'Slippage Percent', 'Min Trades per Period'],
                    'Value': [self.run_id, INITIAL_CAPITAL, BROKERAGE_PER_ORDER,
                             SLIPPAGE_MODE, SLIPPAGE_PERCENT if SLIPPAGE_MODE else 0, MIN_TRADES_PER_PERIOD]
                }
                pd.DataFrame(config_data).to_excel(writer, sheet_name='Configuration', index=False)
            
            print(f"\n✅ Excel export completed: {filename}")
            
        except Exception as e:
            print(f"\n⚠️  Excel export failed: {str(e)}")

    # -----------------------------------
    # VISUALIZATION DASHBOARD
    # -----------------------------------
    def generate_visualizations(
        self,
        ranking_df: pd.DataFrame,
        yearly_df: pd.DataFrame,
        monthly_df: pd.DataFrame,
        exit_reason_df: pd.DataFrame
    ):
        """Generate comprehensive visualization dashboard."""
        
        print("\n📊 Generating visualizations...")
        
        try:
            # 1. Strategy Ranking Comparison
            if not ranking_df.empty:
                self._plot_strategy_ranking(ranking_df)
            
            # 2. Yearly Performance Comparison
            if not yearly_df.empty:
                self._plot_yearly_performance(yearly_df)
            
            # 3. Monthly Heatmap
            if not monthly_df.empty:
                self._plot_monthly_heatmap(monthly_df)
            
            # 4. Drawdown Analysis
            if not ranking_df.empty:
                self._plot_drawdown_comparison(ranking_df)
            
            # 5. Exit Reason Distribution
            if not exit_reason_df.empty:
                self._plot_exit_reasons(exit_reason_df)
            
            print("✅ All visualizations generated successfully")
            
        except Exception as e:
            print(f"⚠️  Visualization generation failed: {str(e)}")

    def _plot_strategy_ranking(self, ranking_df: pd.DataFrame):
        """Plot strategy ranking comparison chart."""
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Strategy Performance Comparison', fontsize=16, fontweight='bold')
        
        strategies = ranking_df['strategy'].tolist()
        
        # Net PnL
        axes[0, 0].barh(strategies, ranking_df['net_pnl'], color='skyblue')
        axes[0, 0].set_xlabel('Net P&L (₹)')
        axes[0, 0].set_title('Net Profit & Loss')
        axes[0, 0].grid(axis='x', alpha=0.3)
        
        # Sharpe Ratio
        axes[0, 1].barh(strategies, ranking_df['sharpe'], color='lightgreen')
        axes[0, 1].set_xlabel('Sharpe Ratio')
        axes[0, 1].set_title('Risk-Adjusted Returns (Sharpe)')
        axes[0, 1].grid(axis='x', alpha=0.3)
        
        # Max Drawdown %
        axes[1, 0].barh(strategies, ranking_df['max_dd_pct'], color='lightcoral')
        axes[1, 0].set_xlabel('Max Drawdown (%)')
        axes[1, 0].set_title('Maximum Drawdown')
        axes[1, 0].grid(axis='x', alpha=0.3)
        
        # Composite Score
        axes[1, 1].barh(strategies, ranking_df['composite'], color='gold')
        axes[1, 1].set_xlabel('Composite Score')
        axes[1, 1].set_title('Overall Ranking Score')
        axes[1, 1].grid(axis='x', alpha=0.3)
        
        plt.tight_layout()
        filename = os.path.join(self.charts_dir, f"strategy_ranking_{self.run_id}.png")
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  ✓ Strategy ranking chart saved")

    def _plot_yearly_performance(self, yearly_df: pd.DataFrame):
        """Plot yearly performance comparison."""
        
        fig, axes = plt.subplots(2, 1, figsize=(14, 10))
        fig.suptitle('Yearly Performance Analysis', fontsize=16, fontweight='bold')
        
        strategies = yearly_df['strategy'].unique()
        colors = plt.cm.tab10(np.linspace(0, 1, len(strategies)))
        
        # Growth % by Year
        for i, strategy in enumerate(strategies):
            data = yearly_df[yearly_df['strategy'] == strategy]
            axes[0].plot(data['year'], data['growth_pct'], 
                        marker='o', label=strategy, color=colors[i], linewidth=2)
        
        axes[0].set_xlabel('Year')
        axes[0].set_ylabel('Growth (%)')
        axes[0].set_title('Annual Growth Rate')
        axes[0].legend(loc='best')
        axes[0].grid(alpha=0.3)
        axes[0].axhline(y=0, color='red', linestyle='--', alpha=0.5)
        
        # Sharpe Ratio by Year
        for i, strategy in enumerate(strategies):
            data = yearly_df[yearly_df['strategy'] == strategy]
            axes[1].plot(data['year'], data['sharpe'], 
                        marker='s', label=strategy, color=colors[i], linewidth=2)
        
        axes[1].set_xlabel('Year')
        axes[1].set_ylabel('Sharpe Ratio')
        axes[1].set_title('Sharpe Ratio Evolution')
        axes[1].legend(loc='best')
        axes[1].grid(alpha=0.3)
        axes[1].axhline(y=0, color='red', linestyle='--', alpha=0.5)
        
        plt.tight_layout()
        filename = os.path.join(self.charts_dir, f"yearly_performance_{self.run_id}.png")
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  ✓ Yearly performance chart saved")

    def _plot_monthly_heatmap(self, monthly_df: pd.DataFrame):
        """Plot monthly P&L heatmap for each strategy."""
        
        strategies = monthly_df['strategy'].unique()
        
        for strategy in strategies:
            data = monthly_df[monthly_df['strategy'] == strategy].copy()
            
            # Extract year and month
            data['year'] = data['year_month'].str[:4].astype(int)
            data['month'] = data['year_month'].str[-2:].astype(int)
            
            # Pivot for heatmap
            pivot = data.pivot(index='month', columns='year', values='net_pnl')
            
            fig, ax = plt.subplots(figsize=(12, 8))
            
            # Create heatmap using imshow
            im = ax.imshow(pivot.values, cmap='RdYlGn', aspect='auto')
            
            # Set ticks
            ax.set_xticks(range(len(pivot.columns)))
            ax.set_yticks(range(len(pivot.index)))
            ax.set_xticklabels(pivot.columns)
            ax.set_yticklabels(pivot.index)
            
            # Add colorbar
            cbar = plt.colorbar(im, ax=ax)
            cbar.set_label('Net P&L (₹)', rotation=270, labelpad=20)
            
            # Add text annotations
            for i in range(len(pivot.index)):
                for j in range(len(pivot.columns)):
                    value = pivot.values[i, j]
                    if not np.isnan(value):
                        text = ax.text(j, i, f'{value:.0f}',
                                     ha="center", va="center", color="black", fontsize=8)
            
            ax.set_title(f'Monthly P&L Heatmap - {strategy}', fontsize=14, fontweight='bold')
            ax.set_xlabel('Year')
            ax.set_ylabel('Month')
            
            plt.tight_layout()
            safe_name = strategy.replace(' ', '_').replace('/', '_')
            filename = os.path.join(self.charts_dir, f"monthly_heatmap_{safe_name}_{self.run_id}.png")
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"  ✓ Monthly heatmap saved: {strategy}")

    def _plot_drawdown_comparison(self, ranking_df: pd.DataFrame):
        """Plot drawdown comparison across strategies."""
        
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        fig.suptitle('Drawdown Analysis', fontsize=16, fontweight='bold')
        
        strategies = ranking_df['strategy'].tolist()
        
        # Max DD %
        axes[0].bar(range(len(strategies)), ranking_df['max_dd_pct'], color='coral')
        axes[0].set_xticks(range(len(strategies)))
        axes[0].set_xticklabels(strategies, rotation=45, ha='right')
        axes[0].set_ylabel('Max Drawdown (%)')
        axes[0].set_title('Maximum Drawdown Percentage')
        axes[0].grid(axis='y', alpha=0.3)
        
        # Max DD Amount
        axes[1].bar(range(len(strategies)), ranking_df['max_dd_amt'], color='lightcoral')
        axes[1].set_xticks(range(len(strategies)))
        axes[1].set_xticklabels(strategies, rotation=45, ha='right')
        axes[1].set_ylabel('Max Drawdown (₹)')
        axes[1].set_title('Maximum Drawdown Amount')
        axes[1].grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        filename = os.path.join(self.charts_dir, f"drawdown_comparison_{self.run_id}.png")
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"  ✓ Drawdown comparison chart saved")

    def _plot_exit_reasons(self, exit_reason_df: pd.DataFrame):
        """Plot exit reason P&L distribution."""
        
        strategies = exit_reason_df['strategy'].unique()
        
        for strategy in strategies:
            data = exit_reason_df[exit_reason_df['strategy'] == strategy].copy()
            data = data.sort_values('total_pnl', ascending=True)
            
            fig, ax = plt.subplots(figsize=(12, 8))
            
            colors = ['green' if x > 0 else 'red' for x in data['total_pnl']]
            ax.barh(data['reason'], data['total_pnl'], color=colors, alpha=0.7)
            
            ax.set_xlabel('Total P&L (₹)')
            ax.set_title(f'Exit Reason P&L Distribution - {strategy}', fontsize=14, fontweight='bold')
            ax.grid(axis='x', alpha=0.3)
            ax.axvline(x=0, color='black', linestyle='-', linewidth=0.8)
            
            plt.tight_layout()
            safe_name = strategy.replace(' ', '_').replace('/', '_')
            filename = os.path.join(self.charts_dir, f"exit_reasons_{safe_name}_{self.run_id}.png")
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            plt.close()
            
            print(f"  ✓ Exit reason chart saved: {strategy}")

    # -----------------------------------
    # DATABASE STORAGE
    # -----------------------------------
    def save_to_database(
        self,
        ranking_df: pd.DataFrame,
        yearly_df: pd.DataFrame,
        monthly_df: pd.DataFrame,
        exit_reason_df: pd.DataFrame
    ):
        """Save analysis results to database."""
        
        print("\n💾 Saving results to database...")
        
        try:
            # Create tables if not exist
            self._create_analysis_tables()
            
            # Save run log
            self._save_run_log()
            
            # Save overall results
            if not ranking_df.empty:
                self._save_overall_results(ranking_df)
            
            # Save yearly results
            if not yearly_df.empty:
                self._save_yearly_results(yearly_df)
            
            # Save monthly results
            if not monthly_df.empty:
                self._save_monthly_results(monthly_df)
            
            # Save exit reason results
            if not exit_reason_df.empty:
                self._save_exit_reason_results(exit_reason_df)
            
            self.conn.commit()
            print("✅ Database save completed")
            
        except Exception as e:
            print(f"⚠️  Database save failed: {str(e)}")
            self.conn.rollback()

    def _create_analysis_tables(self):
        """Create analysis result tables if they don't exist."""
        
        # Drop old table if exists to ensure schema is correct
        self.conn.execute("DROP TABLE IF EXISTS analysis_run_log")
        
        # Run log table
        self.conn.execute("""
            CREATE TABLE analysis_run_log (
                run_id TEXT PRIMARY KEY,
                timestamp DATETIME,
                initial_capital REAL,
                brokerage_per_order REAL,
                slippage_mode TEXT,
                slippage_percent REAL,
                min_trades_per_period INTEGER
            )
        """)
        
        # Overall results table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS analysis_strategy_overall (
                run_id TEXT,
                strategy TEXT,
                rank INTEGER,
                composite REAL,
                net_pnl REAL,
                growth_pct REAL,
                total_trades INTEGER,
                winning_trades INTEGER,
                losing_trades INTEGER,
                breakeven_trades INTEGER,
                win_rate REAL,
                gross_profit REAL,
                gross_loss REAL,
                avg_trade REAL,
                avg_win REAL,
                avg_loss REAL,
                rr_ratio REAL,
                profit_factor REAL,
                expectancy REAL,
                max_dd_pct REAL,
                max_dd_amt REAL,
                sharpe REAL,
                rodd REAL,
                max_win_streak INTEGER,
                max_loss_streak INTEGER,
                trading_days INTEGER,
                winning_days INTEGER,
                losing_days INTEGER,
                win_day_pct REAL,
                avg_day_pnl REAL,
                best_day_pnl REAL,
                worst_day_pnl REAL,
                consistency REAL,
                PRIMARY KEY (run_id, strategy)
            )
        """)
        
        # Yearly results table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS analysis_strategy_yearly (
                run_id TEXT,
                strategy TEXT,
                year INTEGER,
                net_pnl REAL,
                growth_pct REAL,
                total_trades INTEGER,
                win_rate REAL,
                profit_factor REAL,
                sharpe REAL,
                max_dd_pct REAL,
                PRIMARY KEY (run_id, strategy, year)
            )
        """)
        
        # Monthly results table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS analysis_strategy_monthly (
                run_id TEXT,
                strategy TEXT,
                year_month TEXT,
                net_pnl REAL,
                growth_pct REAL,
                total_trades INTEGER,
                win_rate REAL,
                profit_factor REAL,
                sharpe REAL,
                max_dd_pct REAL,
                PRIMARY KEY (run_id, strategy, year_month)
            )
        """)
        
        # Exit reason table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS analysis_exit_reason (
                run_id TEXT,
                strategy TEXT,
                reason TEXT,
                count INTEGER,
                total_pnl REAL,
                avg_pnl REAL,
                PRIMARY KEY (run_id, strategy, reason)
            )
        """)

    def _save_run_log(self):
        """Save run configuration to log table."""
        
        self.conn.execute("""
            INSERT OR REPLACE INTO analysis_run_log 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            self.run_id,
            datetime.now(),
            INITIAL_CAPITAL,
            BROKERAGE_PER_ORDER,
            SLIPPAGE_MODE if SLIPPAGE_MODE else "None",
            SLIPPAGE_PERCENT if SLIPPAGE_MODE else 0,
            MIN_TRADES_PER_PERIOD
        ))

    def _save_overall_results(self, ranking_df: pd.DataFrame):
        """Save overall strategy results to database."""
        
        for _, row in ranking_df.iterrows():
            self.conn.execute("""
                INSERT OR REPLACE INTO analysis_strategy_overall 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.run_id,
                row['strategy'],
                int(row['rank']),
                float(row['composite']),
                float(row['net_pnl']),
                float(row['growth_pct']),
                int(row['total_trades']),
                int(row['winning_trades']),
                int(row['losing_trades']),
                int(row['breakeven_trades']),
                float(row['win_rate']),
                float(row['gross_profit']),
                float(row['gross_loss']),
                float(row['avg_trade']),
                float(row['avg_win']),
                float(row['avg_loss']),
                float(row['rr_ratio']),
                float(row['profit_factor']),
                float(row['expectancy']),
                float(row['max_dd_pct']),
                float(row['max_dd_amt']),
                float(row['sharpe']),
                float(row['rodd']),
                int(row['max_win_streak']),
                int(row['max_loss_streak']),
                int(row['trading_days']),
                int(row['winning_days']),
                int(row['losing_days']),
                float(row['win_day_pct']),
                float(row['avg_day_pnl']),
                float(row['best_day_pnl']),
                float(row['worst_day_pnl']),
                float(row['consistency'])
            ))

    def _save_yearly_results(self, yearly_df: pd.DataFrame):
        """Save yearly breakdown to database."""
        
        for _, row in yearly_df.iterrows():
            self.conn.execute("""
                INSERT OR REPLACE INTO analysis_strategy_yearly 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.run_id,
                row['strategy'],
                int(row['year']),
                float(row['net_pnl']),
                float(row['growth_pct']),
                int(row['total_trades']),
                float(row['win_rate']),
                float(row['profit_factor']),
                float(row['sharpe']),
                float(row['max_dd_pct'])
            ))

    def _save_monthly_results(self, monthly_df: pd.DataFrame):
        """Save monthly breakdown to database."""
        
        for _, row in monthly_df.iterrows():
            self.conn.execute("""
                INSERT OR REPLACE INTO analysis_strategy_monthly 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.run_id,
                row['strategy'],
                row['year_month'],
                float(row['net_pnl']),
                float(row['growth_pct']),
                int(row['total_trades']),
                float(row['win_rate']),
                float(row['profit_factor']),
                float(row['sharpe']),
                float(row['max_dd_pct'])
            ))

    def _save_exit_reason_results(self, exit_reason_df: pd.DataFrame):
        """Save exit reason analysis to database."""
        
        for _, row in exit_reason_df.iterrows():
            self.conn.execute("""
                INSERT OR REPLACE INTO analysis_exit_reason 
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                self.run_id,
                row['strategy'],
                row['reason'],
                int(row['count']),
                float(row['total_pnl']),
                float(row['avg_pnl'])
            ))

    def close(self):
        """Close database connection."""
        self.conn.close()


# ===============================
# MAIN EXECUTION
# ===============================

if __name__ == "__main__":
    
    # Determine database path
    base_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(base_dir, "myalgo.db")
    
    # Check if database exists
    if not os.path.exists(db_path):
        print(f"❌ Error: Database not found at {db_path}")
        print("Please ensure myalgo.db exists in the same directory as this script.")
        exit(1)
    
    # Run analysis
    try:
        analyzer = SuperBacktestAnalyzer(db_path)
        ranking_df, yearly_df, monthly_df, exit_reason_df = analyzer.run()
        analyzer.close()
        
        print("\n" + "="*80)
        print("✅ ANALYSIS COMPLETED SUCCESSFULLY")
        print("="*80)
        print(f"\nResults saved in: {analyzer.output_dir}")
        print(f"Run ID: {analyzer.run_id}")
        print("\n📁 Directory Structure:")
        print(f"   ├── data/")
        if EXPORT_EXCEL:
            print(f"   │   └── backtest_results_{analyzer.run_id}.xlsx")
        print(f"   └── charts/")
        if EXPORT_CHARTS:
            print(f"       ├── strategy_ranking_{analyzer.run_id}.png")
            print(f"       ├── yearly_performance_{analyzer.run_id}.png")
            print(f"       ├── drawdown_comparison_{analyzer.run_id}.png")
            print(f"       ├── monthly_heatmap_*.png (per strategy)")
            print(f"       └── exit_reasons_*.png (per strategy)")
        print("\n" + "="*80)
        
    except Exception as e:
        print(f"\n❌ Analysis failed: {str(e)}")
        import traceback
        traceback.print_exc()