import sqlite3
from pathlib import Path
from typing import Optional
import pandas as pd

from config.constants import DB_PATH
from utils.logger import get_logger

logger = get_logger(__name__)


class DBManager:
    """
    Manages all interactions with the SQLite database.
    """

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Open a connection to the SQLite database (creates file if absent)."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        logger.info(f"Connected to {self.db_path}")

    def disconnect(self) -> None:
        """Commit any pending transaction and close the connection."""
        if self.conn:
            self.conn.commit()
            self.conn.close()
            self.conn = None
            logger.info("Disconnected.")

    # ------------------------------------------------------------------
    # Execution (write / DDL)
    # ------------------------------------------------------------------

    def execute_file(self, sql_file_path: str, params: dict = {}) -> None:
        """Execute a .sql file with optional named parameters (:param_name syntax)."""
        self._require_connection()
        path = Path(sql_file_path)
        if not path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file_path}")
        self.conn.execute(path.read_text(), params)
        self.conn.commit()
        logger.info(f"Executed: {path} — {self.conn.total_changes} rows affected")

    def execute(self, sql: str, params: dict = {}) -> sqlite3.Cursor:
        """Execute a single SQL string (INSERT, UPDATE, DELETE, or DDL)."""
        self._require_connection()
        curr = self.conn.execute(sql, params)
        self.conn.commit()
        logger.info(f"Executed SQL — {self.conn.total_changes} rows affected")
        return curr

    def execute_many(self, sql: str, data: list[tuple]) -> None:
        """Bulk-insert a list of row tuples using a parameterized INSERT."""
        self._require_connection()
        self.conn.executemany(sql, data)
        self.conn.commit()
        logger.info(f"Executed bulk insert — {len(data)} rows")

    # ------------------------------------------------------------------
    # Querying (SELECT → DataFrame)
    # ------------------------------------------------------------------

    def query_file(self, sql_file_path: str, params: dict = {}) -> pd.DataFrame:
        """Read a .sql file and return results as a DataFrame."""
        self._require_connection()
        path = Path(sql_file_path)
        if not path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file_path}")
        return pd.read_sql_query(path.read_text(), self.conn, params=params)

    def query(self, sql: str, params: dict = {}) -> pd.DataFrame:
        """Run a SQL string and return results as a DataFrame."""
        self._require_connection()
        return pd.read_sql_query(sql, self.conn, params=params)

    # ------------------------------------------------------------------
    # Writing
    # ------------------------------------------------------------------

    def write_dataframe(self, df: pd.DataFrame, table: str, if_exists: str = "append") -> None:
        """Write a DataFrame to a table.
        if_exists: 'append' (default) | 'replace' | 'fail'"""
        self._require_connection()
        df.to_sql(table, self.conn, if_exists=if_exists, index=False)
        logger.info(f"Wrote {len(df)} rows to '{table}'.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _require_connection(self) -> None:
        if self.conn is None:
            raise RuntimeError("No active database connection. Call connect() first.")
