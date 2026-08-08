"""Audit logger — writes one record per query to a Databricks Delta table."""

import json
from datetime import datetime, timezone

from dotenv import load_dotenv

from config.settings import AUDIT_CATALOG, AUDIT_SCHEMA, AUDIT_TABLE
from db.connection import get_connection

load_dotenv()

_table_ensured = False


def add_tokens(current: dict, step: str, usage) -> dict:
    """Merge one LLM call's token counts into the running token_usage dict."""
    inp = getattr(usage, "input_tokens", 0)
    out = getattr(usage, "output_tokens", 0)
    return {
        "calls":        current.get("calls", []) + [{"step": step, "input": inp, "output": out}],
        "total_input":  current.get("total_input",  0) + inp,
        "total_output": current.get("total_output", 0) + out,
    }


def _ensure_table() -> None:
    """Create catalog, schema, and audit table if they do not already exist."""
    global _table_ensured
    if _table_ensured:
        return

    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE CATALOG IF NOT EXISTS {AUDIT_CATALOG}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_CATALOG}.{AUDIT_SCHEMA}")
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
                    ts                  TIMESTAMP,
                    question            STRING,
                    user_role           STRING,
                    intent              STRING,
                    eval_score          INT,
                    eval_notes          STRING,
                    elapsed_sec         DOUBLE,
                    error               STRING,
                    generated_sql       STRING,
                    answer_preview      STRING,
                    total_input_tokens  INT,
                    total_output_tokens INT,
                    token_calls         STRING
                )
            """)
    finally:
        conn.close()
    _table_ensured = True


def log_query(question: str, user_role: str, result: dict) -> None:
    """Insert one audit record into the Delta table. Fails silently to never block the UI."""
    try:
        _ensure_table()
        tu  = result.get("token_usage") or {}
        ts  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        conn = get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"INSERT INTO {AUDIT_TABLE} VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        ts,
                        question,
                        user_role,
                        result.get("intent", ""),
                        result.get("eval_score"),
                        json.dumps(result.get("eval_notes", [])),
                        result.get("elapsed"),
                        result.get("error", ""),
                        result.get("sql", ""),
                        (result.get("answer", "") or "")[:200],
                        tu.get("total_input",  0),
                        tu.get("total_output", 0),
                        json.dumps(tu.get("calls", [])),
                        None,
                        None,
                    ],
                )
        finally:
            conn.close()
    except Exception as e:
        print(f"[audit] Warning: could not log query to Databricks: {e}")
