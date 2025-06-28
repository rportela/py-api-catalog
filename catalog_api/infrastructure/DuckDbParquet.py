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
import logging
from typing import Any, Iterable, Mapping

import duckdb

from .S3Bucket import S3Bucket

logger = logging.getLogger(__name__)

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

        # Note: We no longer set S3 credentials here since we use pre-signed URLs
        # which don't require credentials in DuckDB
        
        if httpfs_headers:
            hdrs = ",".join([f"{k}:{v}" for k, v in httpfs_headers.items()])
            self._con.execute("SET httpfs_headers=?", [hdrs])

    def _attach_parquet_files(self) -> None:
        """Attach all Parquet files in the S3 prefix to the DuckDB instance using glob pattern for partition pruning."""
        # Extract table name from the prefix for the view name
        table_name = self.parquet_prefix.rstrip('/').split('/')[-1]
        
        # Try glob pattern approach first (enables partition pruning)
        if self._try_attach_with_glob_pattern(table_name):
            return
        
        # Fallback to pre-signed URLs approach
        self._attach_with_presigned_urls(table_name)

    def _try_attach_with_glob_pattern(self, table_name: str) -> bool:
        """Try to attach files using S3 glob pattern for optimal partition pruning."""
        try:
            # Configure S3 access for DuckDB with correct region
            self._configure_s3_access()
            
            # Create glob pattern for all parquet files in the prefix
            s3_glob_pattern = f"s3://{self.s3_bucket.bucket_name}/{self.parquet_prefix}**/*.parquet"
            
            logger.debug(f"Attempting glob pattern: {s3_glob_pattern}")
            logger.debug(f"Using region: {self.s3_bucket.region_name}")
            
            # Create view using glob pattern - this enables DuckDB's partition pruning
            query = f"""
            CREATE OR REPLACE VIEW {table_name} AS 
            SELECT * FROM read_parquet('{s3_glob_pattern}', 
                                      hive_partitioning=true, 
                                      union_by_name=true)
            """
            
            self._con.execute(query)
            logger.debug(f"Successfully created view {table_name} with glob pattern for partition pruning")
            return True
            
        except Exception as e:
            logger.debug(f"Glob pattern approach failed: {e}")
            return False

    def _configure_s3_access(self) -> None:
        """Configure S3 access credentials for DuckDB."""
        try:
            # Set default region to sa-east-1 (where the bucket is located)
            self._con.execute("SET s3_region='sa-east-1'")
            
            # Try to get credentials from the S3 bucket instance
            if hasattr(self.s3_bucket, 'aws_access_key_id') and self.s3_bucket.aws_access_key_id:
                self._con.execute(f"SET s3_access_key_id='{self.s3_bucket.aws_access_key_id}'")
            
            if hasattr(self.s3_bucket, 'aws_secret_access_key') and self.s3_bucket.aws_secret_access_key:
                self._con.execute(f"SET s3_secret_access_key='{self.s3_bucket.aws_secret_access_key}'")
            
            # Override region if bucket has specific region
            if hasattr(self.s3_bucket, 'region_name') and self.s3_bucket.region_name:
                self._con.execute(f"SET s3_region='{self.s3_bucket.region_name}'")
            
            # Fallback: use environment variables
            import os
            if 'AWS_ACCESS_KEY_ID' in os.environ and not hasattr(self.s3_bucket, 'aws_access_key_id'):
                self._con.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}'")
            
            if 'AWS_SECRET_ACCESS_KEY' in os.environ and not hasattr(self.s3_bucket, 'aws_secret_access_key'):
                self._con.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}'")
            
            # Use AWS_DEFAULT_REGION only if no region is set
            if 'AWS_DEFAULT_REGION' in os.environ and not (hasattr(self.s3_bucket, 'region_name') and self.s3_bucket.region_name):
                self._con.execute(f"SET s3_region='{os.environ['AWS_DEFAULT_REGION']}'")
                    
        except Exception as e:
            logger.warning(f"Could not configure S3 credentials for DuckDB: {e}")

    def _attach_with_presigned_urls(self, table_name: str) -> None:
        """Fallback method using pre-signed URLs."""
        parquet_files = self._find_all_parquet_files(self.parquet_prefix)

        if not parquet_files:
            raise ValueError(f"No parquet files found in prefix: {self.parquet_prefix}")
        
        # For S3 keys with special characters (like = in Hive-style partitions),
        # use pre-signed URLs to avoid URL encoding issues in DuckDB
        presigned_urls = []
        for file_key in parquet_files:
            try:
                presigned_url = self.s3_bucket.s3_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': self.s3_bucket.bucket_name, 'Key': file_key},
                    ExpiresIn=3600  # 1 hour expiration
                )
                presigned_urls.append(presigned_url)
            except Exception as e:
                logger.debug(f"Failed to generate pre-signed URL for {file_key}: {e}")
                continue
        
        if not presigned_urls:
            raise ValueError("Failed to generate any pre-signed URLs for parquet files")
        
        # Create a unified view that reads from all parquet files using pre-signed URLs
        # Enable hive_partitioning to automatically detect partition columns
        # Enable union_by_name to handle schema differences between files
        if len(presigned_urls) == 1:
            # Single file case
            query = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{presigned_urls[0]}', hive_partitioning=true, union_by_name=true)"
        else:
            # Multiple files case - use array syntax for read_parquet
            urls_array = "[" + ", ".join([f"'{url}'" for url in presigned_urls]) + "]"
            query = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet({urls_array}, hive_partitioning=true, union_by_name=true)"
        
        try:
            self._con.execute(query)
            logger.debug(f"Successfully created view {table_name} with pre-signed URLs (no partition pruning)")
        except Exception as e:
            # Fallback: create individual views with safe names
            logger.debug(f"Unified view creation failed: {e}")
            logger.debug("Creating individual views...")
            
            view_names = []
            for i, url in enumerate(presigned_urls):
                safe_view_name = f"{table_name}_part_{i:04d}"
                try:
                    self._con.execute(
                        f"CREATE OR REPLACE VIEW {safe_view_name} AS SELECT * FROM read_parquet('{url}', hive_partitioning=true, union_by_name=true)"
                    )
                    view_names.append(safe_view_name)
                except Exception as view_error:
                    logger.debug(f"Failed to create view {safe_view_name}: {view_error}")
                    continue
            
            # Create a unified view that unions all partitions
            if view_names:
                union_query = " UNION ALL ".join([f"SELECT * FROM {view}" for view in view_names])
                try:
                    self._con.execute(
                        f"CREATE OR REPLACE VIEW {table_name} AS {union_query}"
                    )
                except Exception as union_error:
                    logger.debug(f"Failed to create unified view: {union_error}")
                    # At least we have the individual partition views

    def _find_all_parquet_files(self, prefix: str) -> list[str]:
        """Recursively find all parquet files under the given prefix."""
        parquet_files = []

        # List objects without delimiter to get a flat list of all files
        objects = self.s3_bucket.list_objects(prefix=prefix, delimiter="")

        for obj in objects:
            if obj["key"].endswith(".parquet"):
                parquet_files.append(obj["key"])

        return parquet_files

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
