"""High‑level repository for querying Parquet files with DuckDB.

Typical usage:

>>> repo = ParquetRepository(
...     s3_bucket=my_s3_bucket_instance,
...     parquet_prefix="warehouse/sales_",
...     duckdb_path=":memory:",
... )
>>> rows = repo.query("SELECT country, SUM(gross) FROM sales GROUP BY country")
>>> repo.close()
"""

from __future__ import annotations

import importlib.metadata
from typing import Any, Iterable, Mapping

import duckdb

from .S3Bucket import S3Bucket

__all__ = ["DuckDbParquet"]


class DuckDbParquet:
    """Interface for querying Parquet files stored in S3 using DuckDB.

    Parameters
    ----------
    s3_bucket
        An instance of the S3Bucket class.
    parquet_prefix
        The S3 prefix where Parquet files are stored.
    duckdb_path
        Path to the DuckDB database file.
    duckdb_extensions
        Extra extensions to load (e.g. ["httpfs", "postgres_scanner"]).
    httpfs_headers
        Optional dict of HTTP headers to send for remote Parquet URLs
        (e.g. {"Authorization": "Bearer <JWT>"}).
    """

    def __init__(
        self,
        s3_bucket: S3Bucket,
        parquet_prefix: str,
        duckdb_path: str = ":memory:",
        duckdb_extensions: Iterable[str] = ("httpfs",),
        httpfs_headers: Mapping[str, str] | None = None,
    ) -> None:
        """
        Initialize the repository.

        Args:
            s3_bucket: An instance of the S3Bucket class.
            parquet_prefix: The S3 prefix where Parquet files are stored.
            duckdb_path: Path to the DuckDB database file. Defaults to in-memory (":memory:").
        Note:
            The DuckDB database is always in-memory by default. All Parquet files are attached from S3 storage as views.
        """
        self.s3_bucket = s3_bucket
        self.parquet_prefix = parquet_prefix
        self.duckdb_path = duckdb_path
        self._con = duckdb.connect(duckdb_path)
        self._configure(duckdb_extensions, httpfs_headers)
        self._attach_parquet_files()

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
        duckdb_extensions: Iterable[str],
        httpfs_headers: Mapping[str, str] | None,
    ) -> None:
        for ext in duckdb_extensions:
            self._con.execute(f"INSTALL {ext}; LOAD {ext};")
        if httpfs_headers:
            hdrs = ",".join([f"{k}:{v}" for k, v in httpfs_headers.items()])
            self._con.execute("SET httpfs_headers=?", [hdrs])

    def _attach_parquet_files(self) -> None:
        """Attach all Parquet files in the S3 prefix to the DuckDB instance."""
        parquet_files = [
            obj["key"]
            for obj in self.s3_bucket.list_objects(prefix=self.parquet_prefix)
            if obj["key"].endswith(".parquet")
        ]

        for file_key in parquet_files:
            s3_url = f"s3://{self.s3_bucket.bucket_name}/{file_key}"
            view_name = file_key.split("/")[-1].replace(".parquet", "")
            self._con.execute(
                f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet(?)",
                [s3_url],
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
