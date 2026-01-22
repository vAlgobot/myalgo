"""
OpenAlgo Order, Trade & Position Sync Engine (SQLAlchemy ORM)
------------------------------------------------------------

• Orderbook   → Event log (append-only)
• Tradebook   → Execution log (append-only)
• Positionbook → Snapshot history (time-series)

Safe to run every minute / EOD.
"""

from datetime import datetime, date
import time
import os
from typing import Optional
import sys
from openalgo import api
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, DateTime
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import func
from sqlalchemy import cast, Date
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import get_logger

logger = get_logger("trade_journal")
Base = declarative_base()

API_RETRY = 3
RETRY_DELAY = 2
BROKERAGE_PER_ORDER = 20.0  # ₹20 per executed order

API_KEY = "24a210802403e7053994c6ea2aa690bf5acc0db7e1112a5286fc27b93d1e29e4"
API_HOST = "https://myalgo.vralgo.com/"
WS_URL = "wss://myalgo.vralgo.com/ws"


client = api(api_key=API_KEY, host=API_HOST, ws_url=WS_URL)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "myalgo.db")

engine = create_engine(f"sqlite:///{DB_PATH}", echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

class OrderBook(Base):
    __tablename__ = "orderbook"

    orderid = Column(String(50), primary_key=True)
    symbol = Column(String(50))
    action = Column(String(10))
    exchange = Column(String(20))
    product = Column(String(20))
    pricetype = Column(String(20))
    price = Column(Float)
    trigger_price = Column(Float)
    quantity = Column(Integer)
    order_status = Column(String(20))
    timestamp = Column(DateTime)


class TradeBook(Base):
    __tablename__ = "tradebook"

    id = Column(Integer, primary_key=True, autoincrement=True)
    orderid = Column(String(50), index=True)
    symbol = Column(String(50))
    action = Column(String(10))
    exchange = Column(String(20))
    product = Column(String(20))
    average_price = Column(Float)
    quantity = Column(Integer)
    trade_value = Column(Float)
    timestamp = Column(DateTime, index=True)


class PositionBook(Base):
    __tablename__ = "positionbook"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(50))
    exchange = Column(String(20))
    product = Column(String(20))
    quantity = Column(Integer)
    average_price = Column(Float)
    ltp = Column(Float)
    pnl = Column(Float)
    timestamp = Column(DateTime, index=True)

Base.metadata.create_all(engine)

def retry_api(fn):
    for i in range(API_RETRY):
        try:
            return fn()
        except Exception as e:
            logger.error(f"API error {i+1}/{API_RETRY}: {e}")
            time.sleep(RETRY_DELAY)
    return None


def parse_timestamp(ts: str) -> Optional[datetime]:
    try:
        if "-" in ts:
            return datetime.strptime(ts, "%d-%b-%Y %H:%M:%S")
        today = date.today().strftime("%Y-%m-%d")
        return datetime.strptime(f"{today} {ts}", "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None
    

def sync_orderbook():
    response = retry_api(client.orderbook)
    if not response or "data" not in response:
        return

    session = SessionLocal()
    inserted = 0

    try:
        for o in response["data"].get("orders", []):
            if session.get(OrderBook, o["orderid"]):
                logger.warning(f"Orderbook skip (exists): {o['orderid']}")
                continue

            ts = parse_timestamp(o["timestamp"])
            if not ts:
                continue

            session.add(OrderBook(
                orderid=o["orderid"],
                symbol=o["symbol"],
                action=o["action"],
                exchange=o["exchange"],
                product=o["product"],
                pricetype=o["pricetype"],
                price=float(o["price"]),
                trigger_price=float(o["trigger_price"]),
                quantity=int(o["quantity"]),
                order_status=o["order_status"],
                timestamp=ts
            ))
            inserted += 1

        session.commit()
        logger.info(f"Orderbook → inserted {inserted}")
    finally:
        session.close()

def sync_tradebook():
    response = retry_api(client.tradebook)
    if not response or "data" not in response:
        return

    session = SessionLocal()
    inserted, skipped = 0, 0

    try:
        for t in response["data"]:
            ts = parse_timestamp(t["timestamp"])
            if not ts:
                continue

            exists = session.query(TradeBook).filter(
                TradeBook.orderid == t["orderid"],
                TradeBook.timestamp == ts
            ).first()

            if exists:
                skipped += 1
                logger.info(
                    f"Tradebook exists, skip: {t['orderid']} @ {ts}"
                )
                continue

            session.add(TradeBook(
                orderid=t["orderid"],
                symbol=t["symbol"],
                action=t["action"],
                exchange=t["exchange"],
                product=t["product"],
                average_price=float(t["average_price"]),
                quantity=int(t["quantity"]),
                trade_value=float(t["trade_value"]),
                timestamp=ts
            ))
            inserted += 1

        session.commit()
        logger.info(f"Tradebook → inserted {inserted}")
    finally:
        session.close()

def sync_positionbook():
    response = retry_api(client.positionbook)
    if not response or "data" not in response:
        return

    session = SessionLocal()
    ts = datetime.now().replace(second=0, microsecond=0)
    inserted, skipped = 0, 0

    try:
        for p in response["data"]:

            exists = session.query(PositionBook).filter(
                PositionBook.symbol == p["symbol"],
                PositionBook.timestamp == ts
            ).first()

            if exists:
                skipped += 1
                logger.info(
                    f"Positionbook exists, skip: {p['symbol']} @ {ts}"
                )
                continue
            
            session.add(PositionBook(
                symbol=p["symbol"],
                exchange=p["exchange"],
                product=p["product"],
                quantity=int(p["quantity"]),
                average_price=float(p["average_price"]),
                ltp=float(p["ltp"]),
                pnl=float(p["pnl"]),
                timestamp=ts
            ))
            inserted += 1

        session.commit()
        logger.info(
            f"Positionbook → inserted {inserted}"
        )
    finally:
        session.close()

def log_final_counts():
    """
    Logs final row counts for EOD journaling validation (ORM-based).
    """
    session = SessionLocal()
    try:
        orderbook_count = session.query(func.count(OrderBook.orderid)).scalar()
        tradebook_count = session.query(func.count(TradeBook.id)).scalar()

        latest_ts = session.query(
            func.max(PositionBook.timestamp )
        ).scalar()

        positions_count = 0
        if latest_ts:
            positions_count = session.query(PositionBook)\
                .filter(PositionBook.timestamp  == latest_ts)\
                .count()

        logger.info("📊 EOD DATA SUMMARY")
        logger.info(f"Orderbook   rows : {orderbook_count}")
        logger.info(f"Tradebook   rows : {tradebook_count}")
        logger.info(f"Positionbook rows : {positions_count}")

    finally:
        session.close()

def display_journal_records(limit: int = 10):
    """
    Display latest orderbook, tradebook, and positionbook records
    in terminal using logs ONLY (read-only).
    """
    session = SessionLocal()

    try:
        logger.info("📒 JOURNAL RECORDS (READ ONLY)")
        logger.info("=" * 60)

        # -------------------------
        # ORDERBOOK (Last N)
        # -------------------------
        logger.info("📘 ORDERBOOK (Last %s records)", limit)

        orders = session.query(OrderBook)\
            .order_by(OrderBook.timestamp.desc())\
            .limit(limit)\
            .all()

        for o in orders:
            logger.info(
                f"ORDER | {o.orderid} | {o.symbol} | {o.action} | "
                f"qty={o.quantity} | price={o.price} | {o.order_status} | {o.timestamp}"
            )

        # -------------------------
        # TRADEBOOK (Last N)
        # -------------------------
        logger.info("📗 TRADEBOOK (Last %s records)", limit)

        trades = session.query(TradeBook)\
            .order_by(TradeBook.timestamp.desc())\
            .limit(limit)\
            .all()

        for t in trades:
            logger.info(
                f"TRADE | {t.orderid} | {t.symbol} | {t.action} | "
                f"qty={t.quantity} | avg={t.average_price} | "
                f"value={t.trade_value} | {t.timestamp}"
            )

        # -------------------------
        # POSITIONBOOK (Last N)
        # -------------------------
        logger.info("📙 POSITIONBOOK (Last %s records)", limit)

        positions = session.query(PositionBook)\
            .order_by(PositionBook.timestamp.desc())\
            .limit(limit)\
            .all()

        for p in positions:
            logger.info(
                f"POS | {p.symbol} | {p.product} | "
                f"qty={p.quantity} | avg={p.average_price} | "
                f"ltp={p.ltp} | pnl={p.pnl} | ts={p.timestamp}"
            )

        logger.info("=" * 60)
        logger.info("📒 END OF JOURNAL DISPLAY")
    finally:
        session.close()

def delete_orderbook_by_date(target_date: date = date.today()):
    session = SessionLocal()
    try:
        start_dt = datetime.combine(target_date, datetime.min.time())
        end_dt   = datetime.combine(target_date, datetime.max.time())

        deleted = session.query(OrderBook).filter(
            OrderBook.timestamp >= start_dt,
            OrderBook.timestamp <= end_dt
        ).delete(synchronize_session=False)

        session.commit()
        logger.info(f"🧹 Orderbook deleted rows for {target_date}: {deleted}")
    except Exception as e:
        session.rollback()
        logger.error(f"❌ Orderbook delete failed: {e}")
    finally:
        session.close()


def delete_tradebook_by_date(target_date: date = date.today()):
    session = SessionLocal()
    try:
        start_dt = datetime.combine(target_date, datetime.min.time())
        end_dt   = datetime.combine(target_date, datetime.max.time())

        deleted = session.query(TradeBook).filter(
            TradeBook.timestamp >= start_dt,
            TradeBook.timestamp <= end_dt
        ).delete(synchronize_session=False)

        session.commit()
        logger.info(f"🧹 Tradebook deleted rows for {target_date}: {deleted}")
    except Exception as e:
        session.rollback()
        logger.error(f"❌ Tradebook delete failed: {e}")
    finally:
        session.close()


def delete_positionbook_by_date(target_date: date = date.today()):
    session = SessionLocal()
    try:
        start_dt = datetime.combine(target_date, datetime.min.time())
        end_dt   = datetime.combine(target_date, datetime.max.time())

        deleted = session.query(PositionBook).filter(
            PositionBook.timestamp >= start_dt,
            PositionBook.timestamp <= end_dt
        ).delete(synchronize_session=False)

        session.commit()
        logger.info(f"🧹 Positionbook deleted rows for {target_date}: {deleted}")
    except Exception as e:
        session.rollback()
        logger.error(f"❌ Positionbook delete failed: {e}")
    finally:
        session.close()

def sync_all():
    logger.info("🔄 Starting broker sync")
    sync_orderbook()
    sync_tradebook()
    sync_positionbook()
    logger.info("✅ Broker sync completed")

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    logger.info("#---------Trade Journal Started---------#")
    sync_all()
    log_final_counts()
    display_journal_records()
    logger.info("#---------Trade Journal Update completed---------#")
    # delete_tradebook_by_date(), delete_positionbook_by_date(), delete_orderbook_by_date()

    # ----------------------------------------------------
    # GENERATE ANALYSIS REPORT
    # ----------------------------------------------------
    try:
        from trading_journal_analysis import generate_analysis_report
        logger.info(f"📊 Generating Trading Analysis Report (Brokerage: ₹{BROKERAGE_PER_ORDER}/order)...")
        generate_analysis_report(DB_PATH, brokerage_per_order=BROKERAGE_PER_ORDER)
    except Exception as e:
        logger.error(f"❌ Failed to generate analysis report: {e}")


