#!/usr/bin/env python3
"""
vAlgo Database Inspector
========================

This script provides comprehensive inspection of the vAlgo database,
showing data availability, coverage, gaps, and quality metrics.

Usage:
    python inspect_database.py

Features:
- Database overview and statistics
- Per-instrument data analysis
- Date coverage and gap detection
- Data quality validation
- Export capabilities
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import io
from typing import Dict, List, Any, Optional
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
# Add vAlgo to path (go up one level from data folder)
sys.path.append(str(Path(__file__).parent.parent))

from db.database_main import DatabaseManager
from logger import get_logger


class DatabaseInspector:
    """
    Comprehensive database inspection and analysis tool.
    """
    
    def __init__(self):
        """Initialize database inspector."""
        self.logger = get_logger(__name__)
        self.db_manager: Optional[DatabaseManager] = None
        
    def run_inspection(self) -> None:
        """Run complete database inspection."""
        try:
            print("🔍 vAlgo Database Inspector")
            print("=" * 60)
            
            # Initialize database connection
            self._initialize_database()
            
            # Run inspection steps
            self._show_database_overview()
            self._show_all_tables_overview()
            self._show_instruments_summary()
            self._show_options_chain_summary()
            self._show_metadata_tables()
            self._show_data_coverage()
            self._analyze_timeframe_consistency()
            self._show_sample_data()
            self._check_data_quality("spot_data")
            # self._check_data_quality("options_data")
            self._show_monthly_breakdown("spot_data")
            self._show_monthly_breakdown("options_data")
            
            print("\n" + "=" * 60)
            print("✅ Database inspection completed!")
            
        except Exception as e:
            print(f"❌ Inspection failed: {e}")
            self.logger.error(f"Database inspection error: {e}")
        
        finally:
            if self.db_manager:
                self.db_manager.close()
    
    def _initialize_database(self) -> None:
        """Initialize database connection."""
        try:
            self.db_manager = DatabaseManager()
            print("✅ Database connection established")
            
        except Exception as e:
            raise Exception(f"Failed to connect to database: {e}")
    
    def _show_database_overview(self) -> None:
        """Show high-level database statistics."""
        print("\n📊 DATABASE OVERVIEW")
        print("-" * 40)
        
        if not self.db_manager:
            print("❌ Database not initialized")
            return
            
        try:
            # Get database statistics
            assert self.db_manager is not None
            stats = self.db_manager.get_database_stats()
            
            print(f"📁 Database path: {stats['database_path']}")
            print(f"💾 Database size: {stats['database_size_mb']:.2f} MB")
            print(f"📊 Total records: {stats['total_records']:,}")
            print(f"🔤 Unique symbols: {stats['unique_symbols']}")
            
            if stats['start_date'] and stats['end_date']:
                # Convert to datetime for calculation
                start_dt = pd.to_datetime(stats['start_date'])
                end_dt = pd.to_datetime(stats['end_date'])
                days_span = (end_dt - start_dt).days
                
                print(f"📅 Date range: {stats['start_date']} to {stats['end_date']}")
                print(f"⏳ Total span: {days_span} days")
                
                if stats['total_records'] > 0:
                    avg_records_per_day = stats['total_records'] / max(days_span, 1)
                    print(f"📈 Avg records/day: {avg_records_per_day:.0f}")
            
        except Exception as e:
            print(f"❌ Error getting database overview: {e}")
    
    def _show_instruments_summary(self) -> None:
        """Show summary of all instruments in database."""
        print("\n🎯 INSTRUMENTS SUMMARY")
        print("-" * 40)
        
        if not self.db_manager:
            print("❌ Database not initialized")
            return
            
        try:
            # Get data summary
            assert self.db_manager is not None
            summary_df = self.db_manager.get_data_summary()
            
            if summary_df.empty:
                print("❌ No instrument data found")
                return
            
            print(f"📋 Instruments found: {len(summary_df)}")
            print()
            
            # Display summary table
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', 20)
            
            print(summary_df.to_string(index=False))
            
        except Exception as e:
            print(f"❌ Error getting instruments summary: {e}")
    
    def _show_data_coverage(self) -> None:
        """Show detailed data coverage analysis."""
        print("\n📈 DATA COVERAGE ANALYSIS")
        print("-" * 40)
        
        if not self.db_manager:
            print("❌ Database not initialized")
            return
            
        try:
            # Get all symbols and their date ranges
            assert self.db_manager is not None
            coverage_query = """
            SELECT 
                symbol,
                exchange,
                timeframe,
                COUNT(*) as total_records,
                MIN(timestamp) as first_record,
                MAX(timestamp) as last_record,
                COUNT(DISTINCT DATE(timestamp)) as unique_dates
            FROM spot_data 
            GROUP BY symbol, exchange, timeframe
            ORDER BY symbol, timeframe
            """
            
            assert self.db_manager.connection is not None
            coverage_df = self.db_manager.connection.execute(coverage_query).df()
            
            if coverage_df.empty:
                print("❌ No data coverage information available")
                return
            
            for _, row in coverage_df.iterrows():
                symbol = row['symbol']
                exchange = row['exchange']
                timeframe = row['timeframe']
                total_records = row['total_records']
                first_record = pd.to_datetime(row['first_record'])
                last_record = pd.to_datetime(row['last_record'])
                unique_dates = row['unique_dates']
                
                # Calculate expected trading days (approximate)
                total_days = (last_record - first_record).days + 1
                coverage_percentage = (unique_dates / total_days) * 100 if total_days > 0 else 0
                
                print(f"\n🔸 {symbol} ({exchange}, {timeframe}):")
                print(f"   📊 Records: {total_records:,}")
                print(f"   📅 Period: {first_record.strftime('%Y-%m-%d')} to {last_record.strftime('%Y-%m-%d')}")
                print(f"   📆 Trading days: {unique_dates} / {total_days} ({coverage_percentage:.1f}%)")
                
                # Calculate average records per day
                if unique_dates > 0:
                    avg_per_day = total_records / unique_dates
                    print(f"   📈 Avg records/day: {avg_per_day:.0f}")
                
                # Detect potential gaps
                if coverage_percentage < 70:
                    print(f"   ⚠️  Low coverage - potential data gaps")
            
        except Exception as e:
            print(f"❌ Error analyzing data coverage: {e}")
    
    def _show_sample_data(self) -> None:
        """Show sample data from each instrument."""
        print("\n📋 SAMPLE DATA")
        print("-" * 40)
        
        if not self.db_manager:
            print("❌ Database not initialized")
            return
            
        try:
            # Get list of instruments
            assert self.db_manager is not None
            instruments_query = """
            SELECT DISTINCT symbol, exchange, timeframe 
            FROM spot_data 
            ORDER BY symbol, timeframe
            """
            
            assert self.db_manager.connection is not None
            instruments_df = self.db_manager.connection.execute(instruments_query).df()
            
            for _, row in instruments_df.iterrows():
                symbol = row['symbol']
                exchange = row['exchange']
                timeframe = row['timeframe']
                
                print(f"\n🔸 {symbol} ({exchange}, {timeframe}) - Sample Data:")
                
                # Get first 3 and last 3 records
                sample_query = f"""
                (SELECT timestamp, open, high, low, close, volume 
                 FROM spot_data 
                 WHERE symbol = '{symbol}' AND exchange = '{exchange}' AND timeframe = '{timeframe}'
                 ORDER BY timestamp ASC 
                 LIMIT 3)
                UNION ALL
                (SELECT timestamp, open, high, low, close, volume 
                 FROM spot_data 
                 WHERE symbol = '{symbol}' AND exchange = '{exchange}' AND timeframe = '{timeframe}'
                 ORDER BY timestamp DESC 
                 LIMIT 3)
                ORDER BY timestamp
                """
                
                assert self.db_manager.connection is not None
                sample_df = self.db_manager.connection.execute(sample_query).df()
                
                if not sample_df.empty:
                    # Format for display
                    sample_df['timestamp'] = pd.to_datetime(sample_df['timestamp'])
                    sample_df['timestamp'] = sample_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M')
                    
                    print(sample_df.to_string(index=False, float_format='%.2f'))
                else:
                    print("   ❌ No sample data available")
            
        except Exception as e:
            print(f"❌ Error showing sample data: {e}")
    
    def _check_data_quality(self, tablename) -> None:
        """Check data quality and integrity."""
        print("\n🔍 DATA QUALITY CHECK")
        print("-" * 40)
        
        if not self.db_manager:
            print("❌ Database not initialized")
            return
            
        try:
            quality_checks = []
            assert self.db_manager is not None
            
            # Check 1: OHLC relationships
            ohlc_check_query = f"""
            SELECT 
                symbol,
                COUNT(*) as total_records,
                SUM(CASE WHEN high >= low AND high >= open AND high >= close 
                         AND low <= open AND low <= close THEN 1 ELSE 0 END) as valid_ohlc,
                SUM(CASE WHEN open > 0 AND high > 0 AND low > 0 AND close > 0 THEN 1 ELSE 0 END) as positive_prices,
                SUM(CASE WHEN volume >= 0 THEN 1 ELSE 0 END) as valid_volume
            FROM {tablename} 
            GROUP BY symbol
            """
            
            assert self.db_manager.connection is not None
            quality_df = self.db_manager.connection.execute(ohlc_check_query).df()
            
            for _, row in quality_df.iterrows():
                symbol = row['symbol']
                total = row['total_records']
                valid_ohlc = row['valid_ohlc']
                positive_prices = row['positive_prices']
                valid_volume = row['valid_volume']
                
                ohlc_pct = (valid_ohlc / total) * 100 if total > 0 else 0
                price_pct = (positive_prices / total) * 100 if total > 0 else 0
                volume_pct = (valid_volume / total) * 100 if total > 0 else 0
                
                print(f"\n🔸 {symbol} Quality Metrics:")
                print(f"   ✅ Valid OHLC: {valid_ohlc:,} / {total:,} ({ohlc_pct:.1f}%)")
                print(f"   ✅ Positive prices: {positive_prices:,} / {total:,} ({price_pct:.1f}%)")
                print(f"   ✅ Valid volume: {valid_volume:,} / {total:,} ({volume_pct:.1f}%)")
                
                # Quality assessment
                if ohlc_pct >= 99 and price_pct >= 99 and volume_pct >= 99:
                    print(f"   🎉 Excellent data quality")
                elif ohlc_pct >= 95 and price_pct >= 95:
                    print(f"   ✅ Good data quality")
                else:
                    print(f"   ⚠️  Data quality issues detected")
            
            # Check 2: Duplicate timestamps
            duplicates_query = f"""
            SELECT 
                symbol,
                exchange,
                timeframe,
                COUNT(*) - COUNT(DISTINCT timestamp) as duplicate_timestamps
            FROM {tablename} 
            GROUP BY symbol, exchange, timeframe
            HAVING duplicate_timestamps > 0
            """
            
            assert self.db_manager.connection is not None
            duplicates_df = self.db_manager.connection.execute(duplicates_query).df()
            
            if not duplicates_df.empty:
                print(f"\n⚠️  DUPLICATE TIMESTAMPS FOUND:")
                for _, row in duplicates_df.iterrows():
                    print(f"   🔸 {row['symbol']}: {row['duplicate_timestamps']} duplicates")
            else:
                print(f"\n✅ No duplicate timestamps found")
            
        except Exception as e:
            print(f"❌ Error checking data quality: {e}")
    
    def _show_monthly_breakdown(self, tablename) -> None:
        """Show monthly data breakdown."""
        print("\n📅 MONTHLY DATA BREAKDOWN")
        print("-" * 40)
        
        if not self.db_manager:
            print("❌ Database not initialized")
            return
            
        try:
            assert self.db_manager is not None
            if tablename == "spot_data":
                monthly_query = f"""
                SELECT 
                    symbol,
                    strftime('%Y-%m', timestamp) as month,
                    COUNT(*) as records,
                    MIN(timestamp) as first_day,
                    MAX(timestamp) as last_day
                FROM {tablename} 
                GROUP BY symbol, strftime('%Y-%m', timestamp)
                ORDER BY symbol, month
                """
            else:
                monthly_query = f"""
                SELECT 
                    underlying as symbol,
                    strftime('%Y-%m', timestamp) as month,
                    COUNT(*) as records,
                    MIN(timestamp) as first_day,
                    MAX(timestamp) as last_day
                FROM {tablename} 
                GROUP BY underlying, strftime('%Y-%m', timestamp)
                ORDER BY underlying, month
                """
            
            assert self.db_manager.connection is not None
            monthly_df = self.db_manager.connection.execute(monthly_query).df()
            
            if monthly_df.empty:
                print("❌ No monthly data available")
                return
            
            current_symbol = None
            total_records = 0
            
            for _, row in monthly_df.iterrows():
                symbol = row['symbol']
                month = row['month']
                records = row['records']
                first_day = row['first_day']
                last_day = row['last_day']
                
                if symbol != current_symbol:
                    if current_symbol is not None:
                        print(f"   📊 Total records: {total_records:,}")
                    print(f"\n🔸 {symbol}:")
                    current_symbol = symbol
                    total_records = 0
                
                total_records += records
                
                # Convert dates for display
                first_dt = pd.to_datetime(first_day)
                last_dt = pd.to_datetime(last_day)
                days_in_month = (last_dt - first_dt).days + 1
                
                print(f"   📅 {month}: {records:,} records ({days_in_month} days)")
            
            # Show total for last symbol
            if current_symbol is not None:
                print(f"   📊 Total records: {total_records:,}")
            
        except Exception as e:
            print(f"❌ Error showing monthly breakdown: {e}")
    
    def _show_all_tables_overview(self) -> None:
        """Show overview of all tables in the database."""
        print("\n🗃️  ALL TABLES OVERVIEW")
        print("-" * 40)
        
        if not self.db_manager:
            print("❌ Database not initialized")
            return
            
        try:
            assert self.db_manager is not None
            assert self.db_manager.connection is not None
            
            # Get all tables
            tables_query = "SHOW TABLES"
            tables_result = self.db_manager.connection.execute(tables_query).fetchall()
            
            if not tables_result:
                print("❌ No tables found in database")
                return
            
            print(f"📋 Total tables found: {len(tables_result)}")
            print()
            
            for table_row in tables_result:
                table_name = table_row[0]
                
                # Get row count
                try:
                    count_query = f"SELECT COUNT(*) FROM {table_name}"
                    row_count = self.db_manager.connection.execute(count_query).fetchone()[0]
                    
                    # Get table schema
                    schema_query = f"DESCRIBE {table_name}"
                    schema_result = self.db_manager.connection.execute(schema_query).fetchall()
                    
                    print(f"🔸 Table: {table_name}")
                    print(f"   📊 Records: {row_count:,}")
                    print(f"   📋 Columns: {len(schema_result)}")
                    
                    # Show column details
                    print(f"   🔧 Schema:")
                    for col in schema_result[:5]:  # Show first 5 columns
                        col_name = col[0]
                        col_type = col[1]
                        print(f"      • {col_name}: {col_type}")
                    
                    if len(schema_result) > 5:
                        print(f"      ... and {len(schema_result) - 5} more columns")
                    
                    print()
                    
                except Exception as e:
                    print(f"   ❌ Error analyzing table {table_name}: {e}")
                    
        except Exception as e:
            print(f"❌ Error showing tables overview: {e}")
    
    def _show_options_chain_summary(self) -> None:
        """Show detailed options chain data summary."""
        print("\n📈 OPTIONS CHAIN SUMMARY")
        print("-" * 40)
        
        if not self.db_manager:
            print("❌ Database not initialized")
            return
            
        try:
            assert self.db_manager is not None
            assert self.db_manager.connection is not None
            
            # Check if options table exists
            tables_query = "SHOW TABLES"
            tables_result = self.db_manager.connection.execute(tables_query).fetchall()
            table_names = [row[0] for row in tables_result]
            
            options_table = None
            for table in table_names:
                if 'option' in table.lower():
                    options_table = table
                    break
            
            if not options_table:
                print("❌ No options table found in database")
                return
            
            print(f"🔸 Options table: {options_table}")
            
            # Get options overview
            overview_query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT strike) as unique_strikes,
                COUNT(DISTINCT expiry) as unique_expiries,
                MIN(timestamp) as first_record,
                MAX(timestamp) as last_record,
                MIN(strike) as min_strike,
                MAX(strike) as max_strike
            FROM {options_table}
            """
            
            overview_result = self.db_manager.connection.execute(overview_query).fetchone()
            
            if overview_result:
                total_records = overview_result[0]
                unique_strikes = overview_result[1]
                unique_expiries = overview_result[2]
                first_record = overview_result[3]
                last_record = overview_result[4]
                min_strike = overview_result[5]
                max_strike = overview_result[6]
                
                print(f"   📊 Total records: {total_records:,}")
                print(f"   🎯 Unique strikes: {unique_strikes}")
                print(f"   📅 Unique expiries: {unique_expiries}")
                print(f"   📆 Time range: {first_record} to {last_record}")
                print(f"   💰 Strike range: {min_strike} to {max_strike}")
                
                # Get sample expiry dates
                expiry_query = f"""
                SELECT DISTINCT expiry 
                FROM {options_table} 
                ORDER BY expiry 
                LIMIT 5
                """
                
                expiry_result = self.db_manager.connection.execute(expiry_query).fetchall()
                if expiry_result:
                    expirys = [str(row[0]) for row in expiry_result]
                    print(f"   📋 Sample expiries: {', '.join(expirys)}")
                
                # Show sample options data
                sample_query = f"""
                SELECT timestamp, strike, expiry
                FROM {options_table}
                ORDER BY timestamp DESC
                LIMIT 3
                """
                
                sample_result = self.db_manager.connection.execute(sample_query).fetchall()
                if sample_result:
                    print(f"\n   📋 Sample Options Data:")
                    print(f"   {'Timestamp':<19} {'Strike':<6} {'Expiry':<12}")
                    print(f"   {'-'*75}")
                    for row in sample_result:
                        timestamp = str(row[0])[:19]
                        strike = row[1]
                        expiry = str(row[2])
                        print(f"   {timestamp:<19} {strike:<6} {expiry:<12}")
            
        except Exception as e:
            print(f"❌ Error showing options chain summary: {e}")
    
    def _show_metadata_tables(self) -> None:
        """Show contents of metadata tables."""
        print("\n📋 METADATA TABLES")
        print("-" * 40)
        
        if not self.db_manager:
            print("❌ Database not initialized")
            return
            
        try:
            assert self.db_manager is not None
            assert self.db_manager.connection is not None
            
            # Get all tables
            tables_query = "SHOW TABLES"
            tables_result = self.db_manager.connection.execute(tables_query).fetchall()
            table_names = [row[0] for row in tables_result]
            
            # Find metadata tables
            metadata_tables = [table for table in table_names if 'metadata' in table.lower()]
            
            if not metadata_tables:
                print("❌ No metadata tables found")
                return
            
            for table_name in metadata_tables:
                print(f"\n🔸 {table_name}:")
                
                # Get row count
                count_query = f"SELECT COUNT(*) FROM {table_name}"
                row_count = self.db_manager.connection.execute(count_query).fetchone()[0]
                print(f"   📊 Records: {row_count}")
                
                if row_count > 0:
                    # Show all data for small metadata tables
                    if row_count <= 20:
                        data_query = f"SELECT * FROM {table_name}"
                        data_df = self.db_manager.connection.execute(data_query).df()
                        
                        if not data_df.empty:
                            print(f"   📋 Complete data:")
                            # Format for display
                            pd.set_option('display.max_columns', None)
                            pd.set_option('display.width', None)
                            pd.set_option('display.max_colwidth', 25)
                            
                            # Convert to string and indent
                            table_str = data_df.to_string(index=False)
                            for line in table_str.split('\n'):
                                print(f"      {line}")
                    else:
                        # Show sample for larger tables
                        sample_query = f"SELECT * FROM {table_name} LIMIT 5"
                        sample_df = self.db_manager.connection.execute(sample_query).df()
                        
                        if not sample_df.empty:
                            print(f"   📋 Sample data (first 5 rows):")
                            table_str = sample_df.to_string(index=False)
                            for line in table_str.split('\n'):
                                print(f"      {line}")
                            print(f"      ... and {row_count - 5} more rows")
                
        except Exception as e:
            print(f"❌ Error showing metadata tables: {e}")
    
    def _analyze_timeframe_consistency(self) -> None:
        """Analyze consistency across different timeframes to identify data gaps."""
        print("\n🔍 TIMEFRAME CONSISTENCY ANALYSIS")
        print("-" * 40)
        
        if not self.db_manager:
            print("❌ Database not initialized")
            return
            
        try:
            assert self.db_manager is not None
            assert self.db_manager.connection is not None
            
            # Get distinct dates for each timeframe
            timeframes = ['1m', '5m', 'd']
            timeframe_dates = {}
            
            for tf in timeframes:
                date_query = f"""
                SELECT DISTINCT DATE(timestamp) as trade_date
                FROM spot_data 
                WHERE timeframe = '{tf}'
                ORDER BY trade_date
                """
                
                dates_result = self.db_manager.connection.execute(date_query).fetchall()
                timeframe_dates[tf] = set(str(row[0]) for row in dates_result)
                
                print(f"🔸 {tf} timeframe: {len(timeframe_dates[tf])} unique dates")
            
            print()
            
            # Find date discrepancies
            all_dates = set()
            for dates in timeframe_dates.values():
                all_dates.update(dates)
            
            print(f"📅 Total unique dates across all timeframes: {len(all_dates)}")
            print()
            
            # Analyze each timeframe against others
            for tf in timeframes:
                missing_dates = all_dates - timeframe_dates[tf]
                extra_dates = timeframe_dates[tf] - all_dates
                
                if missing_dates:
                    print(f"⚠️  {tf} timeframe missing {len(missing_dates)} dates:")
                    sorted_missing = sorted(list(missing_dates))
                    for i, date in enumerate(sorted_missing[:10]):  # Show first 10
                        print(f"   • {date}")
                    if len(sorted_missing) > 10:
                        print(f"   ... and {len(sorted_missing) - 10} more dates")
                    print()
                
                if extra_dates:
                    print(f"➕ {tf} timeframe has {len(extra_dates)} extra dates:")
                    sorted_extra = sorted(list(extra_dates))
                    for date in sorted_extra:
                        print(f"   • {date}")
                    print()
            
            # Cross-comparison between specific timeframes
            print("🔄 CROSS-TIMEFRAME COMPARISON:")
            
            # 1m vs 5m
            missing_in_5m = timeframe_dates['1m'] - timeframe_dates['5m']
            missing_in_1m = timeframe_dates['5m'] - timeframe_dates['1m']
            
            if missing_in_5m:
                print(f"   📊 Dates in 1m but missing in 5m ({len(missing_in_5m)}):")
                for date in sorted(list(missing_in_5m)[:5]):
                    print(f"      • {date}")
                if len(missing_in_5m) > 5:
                    print(f"      ... and {len(missing_in_5m) - 5} more")
                print()
            
            if missing_in_1m:
                print(f"   📊 Dates in 5m but missing in 1m ({len(missing_in_1m)}):")
                for date in sorted(list(missing_in_1m)):
                    print(f"      • {date}")
                print()
            
            # 1m vs daily
            missing_in_daily = timeframe_dates['1m'] - timeframe_dates['d']
            missing_in_1m_vs_daily = timeframe_dates['d'] - timeframe_dates['1m']
            
            if missing_in_daily:
                print(f"   📊 Dates in 1m but missing in daily ({len(missing_in_daily)}):")
                for date in sorted(list(missing_in_daily)[:5]):
                    print(f"      • {date}")
                if len(missing_in_daily) > 5:
                    print(f"      ... and {len(missing_in_daily) - 5} more")
                print()
            
            if missing_in_1m_vs_daily:
                print(f"   📊 Dates in daily but missing in 1m ({len(missing_in_1m_vs_daily)}):")
                for date in sorted(list(missing_in_1m_vs_daily)):
                    print(f"      • {date}")
                print()
            
            # Detailed record count analysis for suspicious dates
            if missing_in_1m:
                print("🔍 DETAILED ANALYSIS OF SUSPICIOUS DATES:")
                for date in sorted(list(missing_in_1m))[:3]:  # Analyze first 3 suspicious dates
                    print(f"\n   📅 Date: {date}")
                    
                    for tf in timeframes:
                        count_query = f"""
                        SELECT COUNT(*) as record_count
                        FROM spot_data 
                        WHERE DATE(timestamp) = '{date}' AND timeframe = '{tf}'
                        """
                        
                        count_result = self.db_manager.connection.execute(count_query).fetchone()
                        record_count = count_result[0] if count_result else 0
                        
                        if record_count > 0:
                            # Get time range for this date
                            time_query = f"""
                            SELECT MIN(timestamp) as first_time, MAX(timestamp) as last_time
                            FROM spot_data 
                            WHERE DATE(timestamp) = '{date}' AND timeframe = '{tf}'
                            """
                            
                            time_result = self.db_manager.connection.execute(time_query).fetchone()
                            if time_result:
                                first_time = str(time_result[0])[11:16]  # Extract HH:MM
                                last_time = str(time_result[1])[11:16]
                                print(f"      {tf}: {record_count} records ({first_time} to {last_time})")
                            else:
                                print(f"      {tf}: {record_count} records")
                        else:
                            print(f"      {tf}: 0 records ❌")
            
            # Summary and recommendations
            print(f"\n📋 CONSISTENCY SUMMARY:")
            consistent = len(missing_in_1m) == 0 and len(missing_in_5m) == 0 and len(missing_in_daily) == 0
            
            if consistent:
                print("   ✅ All timeframes have consistent date coverage")
            else:
                print("   ⚠️  Timeframe inconsistencies detected")
                print("   📝 Recommendations:")
                print("      • Investigate data loading processes")
                print("      • Check for partial day loads")
                print("      • Verify data source consistency")
                print("      • Consider data cleanup/re-sync")
            
        except Exception as e:
            print(f"❌ Error analyzing timeframe consistency: {e}")
            import traceback
            print(f"   Traceback: {traceback.format_exc()}")
    
    def export_detailed_report(self, output_path: str = "../outputs/database_report.csv") -> None:
        """Export detailed database report to CSV."""
        try:
            print(f"\n📄 Exporting detailed report to {output_path}...")
            
            # Create output directory
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Get comprehensive data
            assert self.db_manager is not None
            report_query = """
            SELECT 
                symbol,
                exchange,
                timeframe,
                timestamp,
                open,
                high,
                low,
                close,
                volume,
                DATE(timestamp) as trade_date,
                strftime('%Y-%m', timestamp) as month,
                strftime('%H:%M', timestamp) as time_of_day
            FROM spot_data 
            ORDER BY symbol, timestamp
            """
            
            assert self.db_manager.connection is not None
            report_df = self.db_manager.connection.execute(report_query).df()
            
            if not report_df.empty:
                report_df.to_csv(output_path, index=False)
                file_size = Path(output_path).stat().st_size / 1024  # KB
                print(f"✅ Report exported: {file_size:.1f} KB")
            else:
                print("❌ No data to export")
                
        except Exception as e:
            print(f"❌ Error exporting report: {e}")


def main():
    """Main entry point."""
    inspector = DatabaseInspector()
    
    try:
        # Run inspection
        inspector.run_inspection()
        
        # Ask if user wants detailed report
        while True:
            export_choice = input("\n🤔 Export detailed CSV report? (y/n): ").lower().strip()
            if export_choice in ['y', 'yes']:
                inspector.export_detailed_report()
                break
            elif export_choice in ['n', 'no']:
                break
            else:
                print("Please enter 'y' or 'n'")
        
    except KeyboardInterrupt:
        print(f"\n\n⚠️  Inspection interrupted by user")
    except Exception as e:
        print(f"\n❌ Inspection error: {e}")


if __name__ == "__main__":
    main()