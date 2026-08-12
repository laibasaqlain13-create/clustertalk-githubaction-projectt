# node/auth_store.py

"""Cluster-wide credential store used by every chat node.

Session/outbox state stays local to a node, but credentials must be shared or
a normal load-balancer failover becomes an authentication failure.
"""
from __future__ import annotations

import asyncio
import sqlite3
import time

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'client' CHECK(role IN ('client', 'admin')),
    created_at REAL NOT NULL
);
"""


class AuthStore:
    """Opens a short-lived SQLite connection per operation, safe across nodes."""

    def __init__(self, db_path: str):
        self._db_path = db_path

    def start(self) -> None:
        conn = sqlite3.connect(self._db_path, timeout=5)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=5000;")
            conn.executescript(_SCHEMA)
            columns = {row[1] for row in conn.execute("PRAGMA table_info(users)")}
            if "role" not in columns:
                conn.execute("ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'client'")
            conn.commit()
        finally:
            conn.close()

    async def get_user(self, username: str) -> dict | None:
        return await asyncio.to_thread(self._get_user, username)

    def _get_user(self, username: str) -> dict | None:
        conn = sqlite3.connect(self._db_path, timeout=5)
        try:
            conn.execute("PRAGMA busy_timeout=5000;")
            row = conn.execute("SELECT username, password_hash, role FROM users WHERE username = ?", (username,)).fetchone()
            return None if row is None else {"username": row[0], "password_hash": row[1], "role": row[2]}
        finally:
            conn.close()

    async def create_user(self, username: str, password_hash: str, role: str = "client") -> bool:
        return await asyncio.to_thread(self._create_user, username, password_hash, role)

    async def bootstrap_admin(self, username: str, password_hash: str) -> None:
        """Create or promote the administrator configured by the operator.

        This intentionally overwrites the configured account's password on
        startup. Environment configuration is the authority for a bootstrap
        admin, and it also repairs an account that was previously registered
        through the client portal with the same username.
        """
        await asyncio.to_thread(self._bootstrap_admin, username, password_hash)

    def _bootstrap_admin(self, username: str, password_hash: str) -> None:
        conn = sqlite3.connect(self._db_path, timeout=5)
        try:
            conn.execute("PRAGMA busy_timeout=5000;")
            conn.execute(
                """
                INSERT INTO users (username, password_hash, role, created_at)
                VALUES (?, ?, 'admin', ?)
                ON CONFLICT(username) DO UPDATE SET
                    password_hash = excluded.password_hash,
                    role = 'admin'
                """,
                (username, password_hash, time.time()),
            )
            conn.commit()
        finally:
            conn.close()

    def _create_user(self, username: str, password_hash: str, role: str) -> bool:
        conn = sqlite3.connect(self._db_path, timeout=5)
        try:
            conn.execute("PRAGMA busy_timeout=5000;")
            try:
                conn.execute("INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)", (username, password_hash, role, time.time()))
            except sqlite3.IntegrityError:
                return False
            conn.commit()
            return True
        finally:
            conn.close()
