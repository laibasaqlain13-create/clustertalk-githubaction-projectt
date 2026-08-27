"""Database path helpers for local development and Railway deployments."""
from __future__ import annotations

import os
from pathlib import Path


def default_database_path() -> str:
    """Return the persistent Railway path, or a writable local fallback."""
    railway_dir = "/app/data"
    local_dir = Path(__file__).resolve().parent.parent / "data"

    if os.name != "nt":
        try:
            os.makedirs(railway_dir, exist_ok=True)
            return os.path.join(railway_dir, "database.db")
        except OSError:
            pass

    os.makedirs(local_dir, exist_ok=True)
    return str(local_dir / "database.db")


def ensure_database_parent(db_path: str) -> str:
    """Create the parent directory for an explicitly supplied DB path."""
    parent = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(parent, exist_ok=True)
    return db_path