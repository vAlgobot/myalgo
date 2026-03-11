import duckdb
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Set, List, Tuple
import pandas as pd

# ===============================
# CONFIGURATION
# ===============================

project_root = Path(__file__).parent.parent
data_dir = project_root / "data"
data_dir.mkdir(exist_ok=True)
DB_PATH = str(data_dir / "market_data.db")

SPOT_TABLE = "spot_data"
OPTIONS_TABLE = "options_data"

# Expected symbols
EXPECTED_UNDERLYINGS = ["NIFTY", "BANKNIFTY", "SENSEX"]

# Timeframes to analyze (ignore 5m)
ANALYZE_TIMEFRAMES = ["1m", "d"]

# Timeframe specifications
TIMEFRAME_SPECS = {
    "1m": {
        "name": "1-Minute",
        "expected_candles_per_day": 375,  # 6h 15m = 375 minutes
        "threshold": 0.95  # 95% completeness
    },
    "d": {
        "name": "Daily", 
        "expected_candles_per_day": 1,  # 1 candle per day
        "threshold": 1.0  # Must be exact
    }
}

# Output configuration
OUTPUT_DIR = Path("data_integrity_reports")
GENERATE_REPORT = True


# ===============================
# SCHEMA INSPECTOR
# ===============================

class SchemaInspector:
    """Inspect and validate database schema."""
    
    def __init__(self, connection):
        self.conn = connection
    
    def inspect_table(self, table_name: str) -> Dict:
        """Get complete schema information for a table."""
        
        print(f"\n{'='*80}")
        print(f"📋 SCHEMA INSPECTION: {table_name}")
        print(f"{'='*80}")
        
        # Check if table exists
        table_exists = self.conn.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        """).fetchone()[0]
        
        if not table_exists:
            print(f"❌ Table '{table_name}' does not exist!")
            return None
        
        # Get column information
        columns = self.conn.execute(f"""
            SELECT 
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """).fetchall()
        
        print(f"\n📊 Columns ({len(columns)}):")
        print(f"{'Column Name':<20} {'Data Type':<15} {'Nullable':<10}")
        print("-" * 50)
        
        schema_info = {
            'columns': [],
            'column_types': {}
        }
        
        for col_name, data_type, nullable in columns:
            print(f"{col_name:<20} {data_type:<15} {nullable:<10}")
            schema_info['columns'].append(col_name)
            schema_info['column_types'][col_name] = data_type
        
        # Get row count
        row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"\n📦 Total Rows: {row_count:,}")
        schema_info['row_count'] = row_count
        
        return schema_info


# ===============================
# DATA ANALYZER
# ===============================

class MarketDataAnalyzer:
    """Comprehensive market data integrity analyzer for 1m and d timeframes."""
    
    def __init__(self, connection):
        self.conn = connection
        self.report_data = {
            'timestamp': datetime.now(),
            'spot_summary': {},
            'options_summary': {},
            'per_timeframe_analysis': {},
            'per_underlying_analysis': {},
            'issues': []
        }
    
    # ===============================
    # TIMEFRAME DETECTION
    # ===============================
    
    def detect_available_timeframes(self):
        """Detect which timeframes are available in both tables."""
        
        print(f"\n{'='*80}")
        print("📊 TIMEFRAME DETECTION")
        print(f"{'='*80}")
        
        # SPOT timeframes
        spot_tfs = self.conn.execute(
            f"SELECT DISTINCT timeframe FROM {SPOT_TABLE} ORDER BY timeframe"
        ).fetchall()
        spot_timeframes = {tf[0] for tf in spot_tfs}
        
        # OPTIONS timeframes
        opt_tfs = self.conn.execute(
            f"SELECT DISTINCT timeframe FROM {OPTIONS_TABLE} ORDER BY timeframe"
        ).fetchall()
        options_timeframes = {tf[0] for tf in opt_tfs}
        
        print(f"\n📍 SPOT Timeframes: {', '.join(sorted(spot_timeframes))}")
        print(f"🎯 OPTIONS Timeframes: {', '.join(sorted(options_timeframes))}")
        
        # Check against expected (1m and d only)
        expected_set = set(ANALYZE_TIMEFRAMES)
        
        missing_in_spot = expected_set - spot_timeframes
        missing_in_options = expected_set - options_timeframes
        
        if missing_in_spot:
            print(f"\n⚠️  Expected timeframes missing in SPOT: {', '.join(missing_in_spot)}")
            self.report_data['issues'].append({
                'type': 'missing_timeframes_spot',
                'timeframes': list(missing_in_spot)
            })
        
        if missing_in_options:
            print(f"⚠️  Expected timeframes missing in OPTIONS: {', '.join(missing_in_options)}")
            self.report_data['issues'].append({
                'type': 'missing_timeframes_options',
                'timeframes': list(missing_in_options)
            })
        
        # Available for analysis
        available = expected_set & spot_timeframes & options_timeframes
        
        if available:
            print(f"\n✅ Available for analysis: {', '.join(sorted(available))}")
        else:
            print(f"\n❌ No common timeframes found for analysis!")
        
        return {
            'spot': spot_timeframes,
            'options': options_timeframes,
            'available': available
        }
    
    # ===============================
    # BASIC STATISTICS PER TIMEFRAME
    # ===============================
    
    def get_stats_per_timeframe(self, table_name: str, timeframe: str) -> Dict:
        """Get statistics for a specific timeframe."""
        
        # Total rows for this timeframe
        total_rows = self.conn.execute(f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE timeframe = '{timeframe}'
        """).fetchone()[0]
        
        # Date range
        date_info = self.conn.execute(f"""
            SELECT
                MIN(DATE(timestamp)) AS start_date,
                MAX(DATE(timestamp)) AS end_date,
                COUNT(DISTINCT DATE(timestamp)) AS total_days
            FROM {table_name}
            WHERE timeframe = '{timeframe}'
        """).fetchone()
        
        # Symbols/underlyings
        if table_name == SPOT_TABLE:
            symbols = self.conn.execute(f"""
                SELECT DISTINCT symbol FROM {table_name}
                WHERE timeframe = '{timeframe}'
                ORDER BY symbol
            """).fetchall()
            entities = [s[0] for s in symbols]
        else:
            underlyings = self.conn.execute(f"""
                SELECT DISTINCT underlying FROM {table_name}
                WHERE timeframe = '{timeframe}'
                ORDER BY underlying
            """).fetchall()
            entities = [u[0] for u in underlyings]
        
        return {
            'total_rows': total_rows,
            'start_date': date_info[0],
            'end_date': date_info[1],
            'total_days': date_info[2],
            'entities': entities
        }
    
    # ===============================
    # DATE EXTRACTION PER UNDERLYING PER TIMEFRAME
    # ===============================
    
    def get_trading_dates_per_underlying_timeframe(
        self, 
        underlying: str, 
        timeframe: str
    ) -> Dict:
        """Get trading dates for specific underlying and timeframe from both tables."""
        
        # From SPOT table
        spot_dates_query = f"""
        SELECT DISTINCT DATE(timestamp) AS trading_date
        FROM {SPOT_TABLE}
        WHERE symbol = '{underlying}'
        AND timeframe = '{timeframe}'
        ORDER BY trading_date
        """
        
        spot_dates = self.conn.execute(spot_dates_query).fetchall()
        spot_date_set = {d[0] for d in spot_dates}
        
        # From OPTIONS table
        options_dates_query = f"""
        SELECT DISTINCT DATE(timestamp) AS trading_date
        FROM {OPTIONS_TABLE}
        WHERE underlying = '{underlying}'
        AND timeframe = '{timeframe}'
        ORDER BY trading_date
        """
        
        options_dates = self.conn.execute(options_dates_query).fetchall()
        options_date_set = {d[0] for d in options_dates}
        
        return {
            'spot_dates': spot_date_set,
            'options_dates': options_date_set,
            'spot_count': len(spot_date_set),
            'options_count': len(options_date_set)
        }
    
    # ===============================
    # COMPARE SPOT VS OPTIONS
    # ===============================
    
    def compare_spot_vs_options(
        self, 
        underlying: str, 
        timeframe: str
    ):
        """Compare SPOT vs OPTIONS for specific underlying and timeframe."""
        
        tf_name = TIMEFRAME_SPECS.get(timeframe, {}).get('name', timeframe)
        
        print(f"\n{'='*80}")
        print(f"📊 COMPARISON: {underlying} - {tf_name} ({timeframe})")
        print(f"{'='*80}")
        
        # Get dates
        dates_info = self.get_trading_dates_per_underlying_timeframe(
            underlying, 
            timeframe
        )
        
        spot_dates = dates_info['spot_dates']
        options_dates = dates_info['options_dates']
        
        # Summary
        print(f"\n📅 Date Summary:")
        print(f"  SPOT Days     : {len(spot_dates):>4}")
        print(f"  OPTIONS Days  : {len(options_dates):>4}")
        
        # Find missing dates
        missing_in_options = sorted(spot_dates - options_dates)
        missing_in_spot = sorted(options_dates - spot_dates)
        common_dates = sorted(spot_dates & options_dates)
        
        print(f"  Common Days   : {len(common_dates):>4}")
        print(f"  Missing in OPT: {len(missing_in_options):>4}")
        print(f"  Missing in SPT: {len(missing_in_spot):>4}")
        
        # Completeness percentage
        if spot_dates:
            completeness = (len(common_dates) / len(spot_dates)) * 100
            print(f"  Completeness  : {completeness:>6.2f}%")
        else:
            completeness = 0
            print(f"  Completeness  : N/A (no SPOT data)")
        
        # Detail missing dates
        if missing_in_options:
            print(f"\n❌ Dates in SPOT but MISSING in OPTIONS ({len(missing_in_options)} dates):")
            if len(missing_in_options) <= 20:
                for date in missing_in_options:
                    print(f"  - {date}")
            else:
                for date in missing_in_options[:10]:
                    print(f"  - {date}")
                print(f"  ... and {len(missing_in_options) - 10} more")
            
            self.report_data['issues'].append({
                'type': 'missing_options_dates',
                'underlying': underlying,
                'timeframe': timeframe,
                'count': len(missing_in_options),
                'dates': missing_in_options[:50]
            })
        else:
            print(f"\n✅ All SPOT dates present in OPTIONS")
        
        if missing_in_spot:
            print(f"\n⚠️  Dates in OPTIONS but MISSING in SPOT ({len(missing_in_spot)} dates):")
            if len(missing_in_spot) <= 20:
                for date in missing_in_spot:
                    print(f"  - {date}")
            else:
                for date in missing_in_spot[:10]:
                    print(f"  - {date}")
                print(f"  ... and {len(missing_in_spot) - 10} more")
            
            self.report_data['issues'].append({
                'type': 'missing_spot_dates',
                'underlying': underlying,
                'timeframe': timeframe,
                'count': len(missing_in_spot),
                'dates': missing_in_spot[:50]
            })
        else:
            print(f"\n✅ All OPTIONS dates present in SPOT")
        
        # Store analysis
        key = f"{underlying}_{timeframe}"
        self.report_data['per_underlying_analysis'][key] = {
            'underlying': underlying,
            'timeframe': timeframe,
            'spot_days': len(spot_dates),
            'options_days': len(options_dates),
            'common_days': len(common_dates),
            'missing_in_options': len(missing_in_options),
            'missing_in_spot': len(missing_in_spot),
            'completeness_pct': completeness
        }
        
        return {
            'common_dates': common_dates,
            'missing_in_options': missing_in_options,
            'missing_in_spot': missing_in_spot,
            'completeness': completeness
        }
    
    # ===============================
    # INTRADAY COMPLETENESS CHECK (1m only)
    # ===============================
    
    def check_intraday_completeness(
        self, 
        underlying: str, 
        timeframe: str,
        sample_dates: List = None
    ):
        """Check intraday candle completeness (for 1m timeframe only)."""
        
        if timeframe != "1m":
            print(f"\n⏭️  Skipping intraday check for {timeframe} (only applicable to 1m)")
            return
        
        print(f"\n{'='*80}")
        print(f"⏰ INTRADAY COMPLETENESS: {underlying} - 1-Minute")
        print(f"{'='*80}")
        
        # Get common dates
        dates_info = self.get_trading_dates_per_underlying_timeframe(
            underlying, 
            timeframe
        )
        common_dates = sorted(dates_info['spot_dates'] & dates_info['options_dates'])
        
        if not common_dates:
            print(f"⚠️  No common dates to analyze")
            return
        
        # Sample dates if too many
        if sample_dates:
            analysis_dates = sample_dates
        elif len(common_dates) > 30:
            # Sample: first 10, last 10, and 10 random from middle
            analysis_dates = (
                common_dates[:10] + 
                common_dates[-10:] + 
                sorted(list(set(common_dates) - set(common_dates[:10]) - set(common_dates[-10:])))[:10]
            )
            analysis_dates = sorted(set(analysis_dates))
            print(f"📊 Analyzing sample of {len(analysis_dates)} days (from {len(common_dates)} total)")
        else:
            analysis_dates = common_dates
        
        expected_candles = TIMEFRAME_SPECS["1m"]["expected_candles_per_day"]
        threshold = TIMEFRAME_SPECS["1m"]["threshold"]
        min_candles = int(expected_candles * threshold)
        
        print(f"📏 Expected: {expected_candles} candles/day (threshold: {min_candles})")
        print(f"\n🔍 Checking {len(analysis_dates)} days...")
        
        issues_found = []
        
        for date in analysis_dates:
            # SPOT candle count
            spot_count = self.conn.execute(f"""
                SELECT COUNT(*)
                FROM {SPOT_TABLE}
                WHERE symbol = '{underlying}'
                AND DATE(timestamp) = '{date}'
                AND timeframe = '{timeframe}'
            """).fetchone()[0]
            
            # OPTIONS candle count (distinct timestamps)
            options_count = self.conn.execute(f"""
                SELECT COUNT(DISTINCT timestamp)
                FROM {OPTIONS_TABLE}
                WHERE underlying = '{underlying}'
                AND DATE(timestamp) = '{date}'
                AND timeframe = '{timeframe}'
            """).fetchone()[0]
            
            # Check against threshold
            spot_complete = spot_count >= min_candles
            options_complete = options_count >= min_candles
            
            if not spot_complete or not options_complete:
                issues_found.append({
                    'date': date,
                    'spot_candles': spot_count,
                    'options_candles': options_count,
                    'expected': expected_candles
                })
        
        if issues_found:
            print(f"\n⚠️  Found {len(issues_found)} days with incomplete data:")
            print(f"{'Date':<12} {'SPOT':<10} {'OPTIONS':<10} {'Expected':<10} {'Status':<30}")
            print("-" * 75)
            
            for issue in issues_found[:20]:
                status = []
                if issue['spot_candles'] < min_candles:
                    status.append(f"SPOT ({issue['spot_candles']}/{expected_candles})")
                if issue['options_candles'] < min_candles:
                    status.append(f"OPT ({issue['options_candles']}/{expected_candles})")
                
                print(f"{issue['date']!s:<12} {issue['spot_candles']:<10} "
                      f"{issue['options_candles']:<10} {issue['expected']:<10} "
                      f"{', '.join(status):<30}")
            
            if len(issues_found) > 20:
                print(f"... and {len(issues_found) - 20} more days with issues")
            
            self.report_data['issues'].append({
                'type': 'incomplete_intraday_data',
                'underlying': underlying,
                'timeframe': timeframe,
                'count': len(issues_found)
            })
        else:
            print(f"✅ All {len(analysis_dates)} analyzed days have complete intraday data")
        
        return issues_found
    
    # ===============================
    # DAILY COMPLETENESS CHECK (d only)
    # ===============================
    
    def check_daily_completeness(
        self, 
        underlying: str,
        timeframe: str
    ):
        """Check daily candle completeness (for d timeframe only)."""
        
        if timeframe != "d":
            return
        
        print(f"\n{'='*80}")
        print(f"📅 DAILY CANDLE CHECK: {underlying} - Daily")
        print(f"{'='*80}")
        
        # Get common dates
        dates_info = self.get_trading_dates_per_underlying_timeframe(
            underlying, 
            timeframe
        )
        common_dates = sorted(dates_info['spot_dates'] & dates_info['options_dates'])
        
        if not common_dates:
            print(f"⚠️  No common dates to analyze")
            return
        
        print(f"📊 Checking {len(common_dates)} days...")
        
        issues_found = []
        
        for date in common_dates:
            # SPOT should have exactly 1 daily candle
            spot_count = self.conn.execute(f"""
                SELECT COUNT(*)
                FROM {SPOT_TABLE}
                WHERE symbol = '{underlying}'
                AND DATE(timestamp) = '{date}'
                AND timeframe = '{timeframe}'
            """).fetchone()[0]
            
            # OPTIONS should have exactly 1 daily candle per option
            # We check if there are any options for this date
            options_count = self.conn.execute(f"""
                SELECT COUNT(DISTINCT timestamp)
                FROM {OPTIONS_TABLE}
                WHERE underlying = '{underlying}'
                AND DATE(timestamp) = '{date}'
                AND timeframe = '{timeframe}'
            """).fetchone()[0]
            
            # For daily, we expect exactly 1 timestamp per day
            if spot_count != 1 or options_count != 1:
                issues_found.append({
                    'date': date,
                    'spot_candles': spot_count,
                    'options_timestamps': options_count
                })
        
        if issues_found:
            print(f"\n⚠️  Found {len(issues_found)} days with issues:")
            print(f"{'Date':<12} {'SPOT':<10} {'OPT TS':<10} {'Expected':<10}")
            print("-" * 45)
            
            for issue in issues_found[:20]:
                print(f"{issue['date']!s:<12} {issue['spot_candles']:<10} "
                      f"{issue['options_timestamps']:<10} 1")
            
            if len(issues_found) > 20:
                print(f"... and {len(issues_found) - 20} more days")
            
            self.report_data['issues'].append({
                'type': 'daily_candle_issues',
                'underlying': underlying,
                'timeframe': timeframe,
                'count': len(issues_found)
            })
        else:
            print(f"✅ All {len(common_dates)} days have correct daily candles")
        
        return issues_found
    
    # ===============================
    # DUPLICATE DETECTION
    # ===============================
    
    def check_duplicates(self, table_name: str, timeframe: str, label: str) -> Dict:
        """Check for duplicate records for a specific timeframe."""
        
        print(f"\n{'='*80}")
        print(f"🔍 DUPLICATE CHECK: {label} - {timeframe}")
        print(f"{'='*80}")
        
        if table_name == SPOT_TABLE:
            query = f"""
            SELECT
                symbol,
                timestamp,
                COUNT(*) AS cnt
            FROM {table_name}
            WHERE timeframe = '{timeframe}'
            GROUP BY symbol, timestamp
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC
            LIMIT 10
            """
        else:
            query = f"""
            SELECT
                symbol,
                timestamp,
                COUNT(*) AS cnt
            FROM {table_name}
            WHERE timeframe = '{timeframe}'
            GROUP BY symbol, timestamp
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC
            LIMIT 10
            """
        
        duplicates = self.conn.execute(query).fetchall()
        
        if duplicates:
            print(f"\n⚠️  Found {len(duplicates)} duplicate groups (showing top 10):")
            print(f"{'Symbol':<40} {'Timestamp':<20} {'Count':<5}")
            print("-" * 70)
            
            for dup in duplicates[:10]:
                print(f"{str(dup[0]):<40} {str(dup[1]):<20} {dup[2]:<5}")
            
            self.report_data['issues'].append({
                'type': 'duplicates',
                'table': table_name,
                'timeframe': timeframe,
                'count': len(duplicates)
            })
            
            return {'has_duplicates': True, 'count': len(duplicates)}
        else:
            print("✅ No duplicates found")
            return {'has_duplicates': False, 'count': 0}
    
    # ===============================
    # SYMBOL COVERAGE CHECK
    # ===============================
    
    def check_symbol_coverage_per_timeframe(self, timeframe: str):
        """Check symbol coverage for a specific timeframe."""
        
        print(f"\n{'='*80}")
        print(f"📋 SYMBOL COVERAGE: {timeframe}")
        print(f"{'='*80}")
        
        # Get actual symbols from SPOT
        spot_symbols = self.conn.execute(f"""
            SELECT DISTINCT symbol 
            FROM {SPOT_TABLE}
            WHERE timeframe = '{timeframe}'
        """).fetchall()
        spot_symbols = {s[0] for s in spot_symbols}
        
        # Get actual underlyings from OPTIONS
        option_underlyings = self.conn.execute(f"""
            SELECT DISTINCT underlying 
            FROM {OPTIONS_TABLE}
            WHERE timeframe = '{timeframe}'
        """).fetchall()
        option_underlyings = {u[0] for u in option_underlyings}
        
        print(f"\n📊 Expected: {', '.join(EXPECTED_UNDERLYINGS)}")
        print(f"\n📍 Found in SPOT: {', '.join(sorted(spot_symbols)) if spot_symbols else 'None'}")
        print(f"🎯 Found in OPTIONS: {', '.join(sorted(option_underlyings)) if option_underlyings else 'None'}")
        
        # Check missing
        missing_in_spot = set(EXPECTED_UNDERLYINGS) - spot_symbols
        missing_in_options = set(EXPECTED_UNDERLYINGS) - option_underlyings
        
        if missing_in_spot:
            print(f"\n❌ Missing in SPOT: {', '.join(missing_in_spot)}")
            self.report_data['issues'].append({
                'type': 'missing_symbols_spot',
                'timeframe': timeframe,
                'symbols': list(missing_in_spot)
            })
        else:
            print(f"\n✅ All expected symbols present in SPOT")
        
        if missing_in_options:
            print(f"❌ Missing in OPTIONS: {', '.join(missing_in_options)}")
            self.report_data['issues'].append({
                'type': 'missing_symbols_options',
                'timeframe': timeframe,
                'symbols': list(missing_in_options)
            })
        else:
            print(f"✅ All expected underlyings present in OPTIONS")
        
        return {
            'spot_symbols': spot_symbols,
            'option_underlyings': option_underlyings,
            'missing_in_spot': missing_in_spot,
            'missing_in_options': missing_in_options
        }
    
    # ===============================
    # GENERATE COMPREHENSIVE REPORT
    # ===============================
    
    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        
        print(f"\n{'='*80}")
        print(f"📋 COMPREHENSIVE INTEGRITY REPORT")
        print(f"{'='*80}")
        
        print(f"\n🕐 Report Generated: {self.report_data['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Overall health score
        total_issues = len(self.report_data['issues'])
        
        print(f"\n🏥 OVERALL HEALTH CHECK")
        print(f"Total Issues Found: {total_issues}")
        
        if total_issues == 0:
            print("✅ ✅ ✅  DATABASE IS HEALTHY - READY FOR BACKTESTING & LIVE TRADING")
        elif total_issues < 5:
            print("⚠️  Minor issues found - Review recommended")
        else:
            print("❌ Multiple issues found - Action required before trading")
        
        # Per-timeframe, per-underlying summary
        if self.report_data['per_underlying_analysis']:
            print(f"\n📊 COMPLETENESS SUMMARY (SPOT vs OPTIONS):")
            print(f"{'Underlying':<12} {'Timeframe':<10} {'SPOT':<8} {'OPT':<8} {'Common':<8} {'Match %':<10} {'Status':<10}")
            print("-" * 75)
            
            for key, analysis in sorted(self.report_data['per_underlying_analysis'].items()):
                match_pct = analysis['completeness_pct']
                status = "✅ Good" if match_pct >= 95 else ("⚠️  Fair" if match_pct >= 80 else "❌ Poor")
                
                print(f"{analysis['underlying']:<12} {analysis['timeframe']:<10} "
                      f"{analysis['spot_days']:<8} {analysis['options_days']:<8} "
                      f"{analysis['common_days']:<8} {match_pct:>6.1f}%   {status:<10}")
        
        # Issue summary by type
        if self.report_data['issues']:
            print(f"\n⚠️  ISSUES BREAKDOWN:")
            issue_types = {}
            for issue in self.report_data['issues']:
                issue_type = issue['type']
                issue_types[issue_type] = issue_types.get(issue_type, 0) + 1
            
            for issue_type, count in sorted(issue_types.items()):
                print(f"  - {issue_type}: {count}")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        if total_issues == 0:
            print("  ✅ Database is production-ready for all timeframes")
            print("  ✅ Proceed with backtesting and live trading")
        else:
            if any(i['type'] == 'missing_options_dates' for i in self.report_data['issues']):
                print("  📥 Fetch missing OPTIONS data for identified dates and timeframes")
            if any(i['type'] == 'missing_spot_dates' for i in self.report_data['issues']):
                print("  📥 Fetch missing SPOT data for identified dates and timeframes")
            if any(i['type'] == 'incomplete_intraday_data' for i in self.report_data['issues']):
                print("  🔧 Re-fetch incomplete 1m intraday candles")
            if any(i['type'] == 'daily_candle_issues' for i in self.report_data['issues']):
                print("  🔧 Fix daily candle issues (should be exactly 1 per day)")
            if any(i['type'].startswith('duplicates') for i in self.report_data['issues']):
                print("  🧹 Clean duplicate records")
        
        print("\n" + "="*80)
    
    # ===============================
    # EXPORT REPORT TO FILE
    # ===============================
    
    def export_report(self):
        """Export detailed report to file."""
        
        if not GENERATE_REPORT:
            return
        
        OUTPUT_DIR.mkdir(exist_ok=True)
        
        timestamp = self.report_data['timestamp'].strftime("%Y%m%d_%H%M%S")
        report_file = OUTPUT_DIR / f"integrity_report_{timestamp}.txt"
        
        with open(report_file, 'w') as f:
            f.write("="*80 + "\n")
            f.write("MARKET DATA INTEGRITY REPORT (1m & d Timeframes)\n")
            f.write("="*80 + "\n\n")
            
            f.write(f"Generated: {self.report_data['timestamp']}\n\n")
            
            # Per-underlying, per-timeframe analysis
            f.write("\nCOMPLETENESS SUMMARY\n")
            f.write("-"*80 + "\n")
            
            for key, analysis in sorted(self.report_data['per_underlying_analysis'].items()):
                f.write(f"\n{analysis['underlying']} - {analysis['timeframe']}:\n")
                f.write(f"  SPOT Days: {analysis['spot_days']}\n")
                f.write(f"  OPTIONS Days: {analysis['options_days']}\n")
                f.write(f"  Common Days: {analysis['common_days']}\n")
                f.write(f"  Missing in OPTIONS: {analysis['missing_in_options']}\n")
                f.write(f"  Missing in SPOT: {analysis['missing_in_spot']}\n")
                f.write(f"  Completeness: {analysis['completeness_pct']:.2f}%\n")
            
            # Issues
            if self.report_data['issues']:
                f.write("\n\nISSUES FOUND\n")
                f.write("-"*80 + "\n")
                
                for idx, issue in enumerate(self.report_data['issues'], 1):
                    f.write(f"\n{idx}. {issue['type']}\n")
                    for key, value in issue.items():
                        if key != 'type':
                            if isinstance(value, list) and len(value) > 10:
                                f.write(f"   {key}: {value[:10]} ... (and {len(value)-10} more)\n")
                            else:
                                f.write(f"   {key}: {value}\n")
        
        print(f"\n📄 Detailed report saved: {report_file}")


# ===============================
# MAIN EXECUTION
# ===============================

def main():
    """Main execution function."""
    
    print("\n" + "="*80)
    print("🔍 MARKET DATA INTEGRITY ANALYZER v3.0 (1m & d Timeframes)")
    print("="*80)
    
    # Check database exists
    if not os.path.exists(DB_PATH):
        print(f"\n❌ Database not found: {DB_PATH}")
        print("Please ensure market_data.db exists in the data/ directory")
        return
    
    print(f"\n📁 Database: {DB_PATH}")
    print(f"📊 Analyzing Timeframes: {', '.join(ANALYZE_TIMEFRAMES)}")
    
    try:
        # Connect to database
        conn = duckdb.connect(DB_PATH, read_only=True)
        
        # ===============================
        # PHASE 1: SCHEMA INSPECTION
        # ===============================
        
        inspector = SchemaInspector(conn)
        
        spot_schema = inspector.inspect_table(SPOT_TABLE)
        options_schema = inspector.inspect_table(OPTIONS_TABLE)
        
        if not spot_schema or not options_schema:
            print("\n❌ Required tables not found. Exiting.")
            conn.close()
            return
        
        # ===============================
        # PHASE 2: TIMEFRAME DETECTION
        # ===============================
        
        analyzer = MarketDataAnalyzer(conn)
        
        timeframe_info = analyzer.detect_available_timeframes()
        available_timeframes = timeframe_info['available']
        
        if not available_timeframes:
            print("\n❌ No common timeframes (1m or d) found. Exiting.")
            conn.close()
            return
        
        # ===============================
        # PHASE 3: PER-TIMEFRAME ANALYSIS
        # ===============================
        
        for timeframe in sorted(available_timeframes):
            
            tf_name = TIMEFRAME_SPECS.get(timeframe, {}).get('name', timeframe)
            
            print(f"\n{'='*80}")
            print(f"📊 ANALYZING TIMEFRAME: {tf_name} ({timeframe})")
            print(f"{'='*80}")
            
            # Basic stats
            spot_stats = analyzer.get_stats_per_timeframe(SPOT_TABLE, timeframe)
            options_stats = analyzer.get_stats_per_timeframe(OPTIONS_TABLE, timeframe)
            
            print(f"\n📍 SPOT ({timeframe}):")
            print(f"  Total Rows: {spot_stats['total_rows']:,}")
            print(f"  Symbols: {', '.join(spot_stats['entities'])}")
            print(f"  Date Range: {spot_stats['start_date']} → {spot_stats['end_date']}")
            print(f"  Trading Days: {spot_stats['total_days']}")
            
            print(f"\n🎯 OPTIONS ({timeframe}):")
            print(f"  Total Rows: {options_stats['total_rows']:,}")
            print(f"  Underlyings: {', '.join(options_stats['entities'])}")
            print(f"  Date Range: {options_stats['start_date']} → {options_stats['end_date']}")
            print(f"  Trading Days: {options_stats['total_days']}")
            
            # Duplicate check
            analyzer.check_duplicates(SPOT_TABLE, timeframe, "SPOT")
            analyzer.check_duplicates(OPTIONS_TABLE, timeframe, "OPTIONS")
            
            # Symbol coverage
            coverage = analyzer.check_symbol_coverage_per_timeframe(timeframe)
            
            # Get available underlyings
            available_underlyings = (
                set(EXPECTED_UNDERLYINGS) & 
                coverage['spot_symbols'] & 
                coverage['option_underlyings']
            )
            
            if not available_underlyings:
                print(f"\n⚠️  No common underlyings for {timeframe}")
                continue
            
            # ===============================
            # PER-UNDERLYING ANALYSIS
            # ===============================
            
            for underlying in sorted(available_underlyings):
                
                # Compare SPOT vs OPTIONS
                comparison = analyzer.compare_spot_vs_options(underlying, timeframe)
                
                # Intraday completeness (1m only)
                if timeframe == "1m":
                    analyzer.check_intraday_completeness(underlying, timeframe)
                
                # Daily completeness (d only)
                if timeframe == "d":
                    analyzer.check_daily_completeness(underlying, timeframe)
        
        # ===============================
        # PHASE 4: FINAL REPORT
        # ===============================
        
        analyzer.generate_summary_report()
        analyzer.export_report()
        
        conn.close()
        
        print("\n✅ Analysis completed successfully")
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"\n❌ Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()