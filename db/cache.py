import os
import sqlite3
from typing import Optional

import pandas as pd


class MarketCache:
    """Simple SQLite-backed OHLCV cache.

    Stores per-ticker daily rows with primary key (ticker, date).
    Date is stored as TEXT in YYYY-MM-DD format.
    """

    def __init__(self, db_path: str = "db/market_cache.db"):
        # Ensure directory exists
        dirname = os.path.dirname(db_path)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname, exist_ok=True)

        self.db_path = db_path
        self._init_db()

    def _get_conn(self):
        return sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)

    def _init_db(self):
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS price_data (
                    ticker TEXT NOT NULL,
                    date TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    adj_close REAL,
                    volume INTEGER,
                    PRIMARY KEY(ticker, date)
                )
                """
            )
            conn.commit()

    def get_last_date(self, ticker: str) -> Optional[pd.Timestamp]:
        """Return the latest date (as pd.Timestamp) stored for ticker, or None."""
        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT MAX(date) FROM price_data WHERE ticker=?", (ticker,))
            r = cur.fetchone()
            if r and r[0]:
                return pd.to_datetime(r[0])
            return None

    def get_data(self, ticker: str) -> pd.DataFrame:
        """Return all cached rows for ticker as a DataFrame indexed by date (ascending).

        If no data, returns empty DataFrame.
        """
        with self._get_conn() as conn:
            df = pd.read_sql_query(
                "SELECT date, open, high, low, close, adj_close, volume FROM price_data WHERE ticker=? ORDER BY date",
                conn,
                params=(ticker,),
            )

        if df.empty:
            # empty DataFrame with expected columns
            return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Adj Close", "Volume"]).astype({
                "Open": float, "High": float, "Low": float, "Close": float, "Adj Close": float, "Volume": float
            })

        df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
        df = df.set_index("date")
        # rename columns to match yfinance/pandas expected names
        df = df.rename(columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "adj_close": "Adj Close",
            "volume": "Volume",
        })
        return df

    def upsert_data(self, ticker: str, df: pd.DataFrame):
        """Insert or replace rows from df for ticker.

        df may be either indexed by DatetimeIndex or have a 'Date'/'date' column. We convert index to date strings.
        """
        if df is None or df.empty:
            return

        # Ensure we have a datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            # try common column names
            if "Date" in df.columns:
                df = df.set_index(pd.to_datetime(df["Date"]))
            elif "date" in df.columns:
                df = df.set_index(pd.to_datetime(df["date"]))
            else:
                raise ValueError("DataFrame must have DatetimeIndex or a 'Date'/'date' column")

        # Standardize column names
        mapping = {
            "Open": "Open",
            "open": "Open",
            "High": "High",
            "high": "High",
            "Low": "Low",
            "low": "Low",
            "Close": "Close",
            "close": "Close",
            "Adj Close": "Adj Close",
            "AdjClose": "Adj Close",
            "adj_close": "Adj Close",
            "adj close": "Adj Close",
            "Volume": "Volume",
            "volume": "Volume",
        }
        df = df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})

        # Required columns
        for col in ["Open", "High", "Low", "Close"]:
            if col not in df.columns:
                df[col] = pd.NA

        # adj close and volume optional
        if "Adj Close" not in df.columns:
            df["Adj Close"] = df.get("Close")
        if "Volume" not in df.columns:
            df["Volume"] = df.get("volume", pd.NA)

        rows = []
        for idx, row in df.iterrows():
            date_str = pd.to_datetime(idx).strftime("%Y-%m-%d")
            rows.append((
                ticker,
                date_str,
                None if pd.isna(row.get("Open")) else float(row.get("Open")),
                None if pd.isna(row.get("High")) else float(row.get("High")),
                None if pd.isna(row.get("Low")) else float(row.get("Low")),
                None if pd.isna(row.get("Close")) else float(row.get("Close")),
                None if pd.isna(row.get("Adj Close")) else float(row.get("Adj Close")),
                None if pd.isna(row.get("Volume")) else int(row.get("Volume")),
            ))

        with self._get_conn() as conn:
            cur = conn.cursor()
            cur.executemany(
                """
                INSERT OR REPLACE INTO price_data (ticker, date, open, high, low, close, adj_close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()


__all__ = ["MarketCache"]
