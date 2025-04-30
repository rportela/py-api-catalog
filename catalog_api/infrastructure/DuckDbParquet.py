"""High‑level repository for querying Parquet files with DuckDB.

Typical usage:

>>> repo = ParquetRepository(
...     parquet_locations=["s3://warehouse/sales_*.parquet"],
...     threads=4,
... )
>>> rows = repo.query("SELECT country, SUM(gross) FROM parquet GROUP BY country")
>>> repo.close()
"""

from __future__ import annotations

import importlib.metadata
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence, Union

import duckdb

__all__ = ["ParquetRepository"]

RemotePath = str  # e.g. "s3://bucket/key" or "https://host/object"
LocalPath = Union[str, Path]


class ParquetRepository:
    """Embed DuckDB and expose a simple query interface over Parquet files.

    Parameters
    ----------
    parquet_locations
        One or more glob patterns (local) *or* absolute URLs to Parquet files.
    threads
        Number of worker threads DuckDB should use. `None` → DuckDB default.
    read_only
        If *True*, the temporary DuckDB catalog will be deleted on close().
    duckdb_path
        Optional persistent DuckDB file; skips catalog rebuild between runs.
    duckdb_extensions
        Extra extensions to load (e.g. ["httpfs", "postgres_scanner"]).
    httpfs_headers
        Optional dict of HTTP headers to send for remote Parquet URLs
        (e.g. {"Authorization": "Bearer <JWT>"}).
    """

    def __init__(
        self,
        parquet_locations: Sequence[RemotePath | LocalPath],
        *,
        threads: int | None = None,
        read_only: bool = True,
        duckdb_path: LocalPath | None = None,
        duckdb_extensions: Iterable[str] = ("httpfs",),
        httpfs_headers: Mapping[str, str] | None = None,
    ) -> None:
        assert duckdb_path is not None, "duckdb_path must be set"
        self._con = duckdb.connect(duckdb_path, read_only=read_only)
        self._configure(threads, duckdb_extensions, httpfs_headers)
        self._attach_parquet(parquet_locations)

    # ---------------------------------------------------------------------
    # Public API
    # ---------------------------------------------------------------------
    def query(self, sql: str, *params) -> list[tuple[Any, ...]]:  # noqa: ANN401
        """Run *sql* and return a list of DuckDB `Row` objects."""
        return self._con.execute(sql, params).fetchall()

    def fetch_df(self, sql: str, *params):  # noqa: ANN001, ANN201
        """Run *sql* and return a **pandas** DataFrame (or Polars if installed)."""
        return self._con.execute(sql, params).fetch_df()

    def refresh(self) -> None:
        """Invalidate file metadata cache (call after upstream replaces files)."""
        # DuckDB 1.2 has `PRAGMA invalidate_cached_files`:
        self._con.execute("PRAGMA invalidate_cached_files")

    def close(self) -> None:  # noqa: D401
        """Close the underlying DuckDB connection."""
        self._con.close()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------
    def _configure(
        self,
        threads: int | None,
        duckdb_extensions: Iterable[str],
        httpfs_headers: Mapping[str, str] | None,
    ) -> None:
        if threads is not None:
            self._con.execute("PRAGMA threads = ?", [threads])
        for ext in duckdb_extensions:
            self._con.execute(f"INSTALL {ext}; LOAD {ext};")
        if httpfs_headers:
            hdrs = ",".join([f"{k}:{v}" for k, v in httpfs_headers.items()])
            self._con.execute("SET httpfs_headers=?", [hdrs])

    def _attach_parquet(self, parquet_locations):  # noqa: ANN001
        # Register each location as a *view* named after the stem, or "parquet"
        default_view = "parquet"
        for loc in parquet_locations:
            view_name = (
                Path(loc).stem.replace("*", "") or default_view  # local path
                if "//" not in str(loc)
                else default_view  # URL → generic
            )
            self._con.execute(
                f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet(?)",
                [str(loc)],
            )

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def __repr__(self):
        ver = importlib.metadata.version("duckdb")
        return f"<ParquetRepository duckdb={ver} attached={'TODO: list table names'}>"
