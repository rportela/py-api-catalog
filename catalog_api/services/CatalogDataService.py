from datetime import datetime
from io import BytesIO
import logging
from typing import Any, Dict, Optional

import pandas as pd
from catalog_api.infrastructure.DuckDbParquet import DuckDbParquet
from catalog_api.infrastructure.S3Bucket import S3Bucket

logger = logging.getLogger(__name__)


class CatalogDataService:
    """
    CatalogDataService is a service class that provides methods to interact with the catalog data.
    """

    _bucket = S3Bucket(region_name="sa-east-1")

    def __init__(self):
        """
        Initializes the CatalogDataService with the provided catalog data.
        :param catalog_data: The catalog data to be used by the service.
        """
        self._bucket = S3Bucket(region_name="sa-east-1")

    def get_parquet_path(
        self,
        organization: str,
        dataset: str,
        table: str,
        partitions: Optional[dict] = None,
    ) -> str:
        """
        Returns the path to the data file in the S3 bucket.
        :param organization: The organization name.
        :param dataset: The dataset name.
        :param table: The table name.
        :param partitions: Optional dictionary of partitions.
        :return: The path to the data file.
        """
        parts = "/".join(
            [
                organization,
                dataset,
                table,
            ]
        )
        if partitions:
            parts += "/" + "/".join([f"{k}={v}" for k, v in partitions.items()])
        return "catalog-data/" + parts + "/data.parquet"

    def put_parquet_data(
        self,
        df: pd.DataFrame,
        organization: str,
        dataset: str,
        table: str,
        partitions: Optional[dict] = None,
    ) -> Dict[str, Any]:
        """
        Uploads a DataFrame to S3 as a Parquet file, partitioned if partitions are provided.
        :param df: The DataFrame to be uploaded.
        :param organization: The organization name.
        :param dataset: The dataset name.
        :param table: The table name.
        :param partitions: Optional dictionary of partitions.
        :return: The response from the S3 upload.
        """
        # Make a copy of the DataFrame to avoid modifying the original
        df_to_save = df.copy()
        
        if partitions:
            # Add partition columns to the DataFrame
            for partition_key, partition_value in partitions.items():
                df_to_save[partition_key] = partition_value
            
            # Write partitioned parquet file to the correct S3 path
            file_path = self.get_parquet_path(organization, dataset, table, partitions)
        else:
            file_path = self.get_parquet_path(organization, dataset, table)
        
        pq = BytesIO()
        df_to_save.to_parquet(pq, index=False, compression="snappy")
        pq.seek(0)
        return self._bucket.put_bytes(file_path, pq.getvalue())

    def _list_files(self, prefix: str) -> list:
        """
        List all files under a given prefix in the S3 bucket.
        :param prefix: The prefix to search under.
        :return: List of file paths.
        """
        objects = self._bucket.list_objects(prefix=prefix, delimiter="")
        return [obj["key"] for obj in objects if obj.get("type") == "file"]

    def list_partition_paths(
        self,
        organization: str,
        dataset: str,
        table: str,
    ) -> list:
        """
        List all partition paths for a given table in S3.
        :param organization: The organization name.
        :param dataset: The dataset name.
        :param table: The table name.
        :return: List of partition paths (relative to the table folder).
        """
        prefix = f"catalog-data/{organization}/{dataset}/{table}/"
        all_files = self._list_files(prefix)
        # Only keep paths ending with data.parquet
        partition_paths = [f for f in all_files if f.endswith("data.parquet")]
        return partition_paths

    def get_parquet_data(
        self,
        organization: str,
        dataset: str,
        table: str,
        partitions: Optional[dict] = None,
    ) -> pd.DataFrame:
        """
        Downloads Parquet file(s) from S3 and returns as a DataFrame. If partitions is None, reads all partitions.
        :param organization: The organization name.
        :param dataset: The dataset name.
        :param table: The table name.
        :param partitions: Optional dictionary of partitions.
        :return: The DataFrame containing the data.
        """
        if partitions:
            file_path = self.get_parquet_path(organization, dataset, table, partitions)
            data = self._bucket.get_bytes(file_path)
            return pd.read_parquet(BytesIO(data))
        else:
            # Read all partitioned parquet files for the table
            partition_paths = self.list_partition_paths(organization, dataset, table)
            dfs = []
            for path in partition_paths:
                data = self._bucket.get_bytes(path)
                dfs.append(pd.read_parquet(BytesIO(data)))
            if dfs:
                return pd.concat(dfs, ignore_index=True)
            else:
                return pd.DataFrame()

    def get_parquet_last_updated(
        self,
        organization: str,
        dataset: str,
        table: str,
        partitions: Optional[dict] = None,
    ) -> Optional[datetime]:
        """
        Returns the last updated timestamp of a Parquet file in S3.
        :param organization: The organization name.
        :param dataset: The dataset name.
        :param table: The table name.
        :param partitions: Optional dictionary of partitions.
        :return: The last updated timestamp as a string or None if not found.
        """
        file_path = self.get_parquet_path(organization, dataset, table, partitions)
        return self._bucket.get_last_updated_date_of_file(file_path)

    def get_duckdb_path(self, organization: str) -> str:
        """
        Returns the path to the DuckDB instance for the given organization.
        :param organization: The organization name.
        :return: The path to the DuckDB instance.
        """
        return f"catalog-data/{organization}/_duckdb"

    def ensure_duckdb_instance(self, organization: str) -> None:
        """
        Ensures the existence of a DuckDB instance for the given organization.
        If the instance does not exist, it initializes a valid DuckDB database.
        :param organization: The organization name.
        """
        import duckdb
        from io import BytesIO

        duckdb_path = self.get_duckdb_path(organization)
        if not self._bucket.file_exists(duckdb_path):
            # Create a new DuckDB database in memory
            conn = duckdb.connect(database=":memory:")
            conn.execute("CREATE TABLE IF NOT EXISTS dummy_table (id INTEGER);")

            # Save the database to a file-like object
            db_bytes = BytesIO()
            conn.execute("EXPORT DATABASE 'dummy_export';")
            conn.close()

            # Upload the database to S3
            db_bytes.seek(0)
            self._bucket.put_bytes(duckdb_path, db_bytes.getvalue())

    def get_duckdb(self, organization: str, dataset: str, table: str) -> DuckDbParquet:
        """
        Returns a DuckDbParquet instance for querying all Parquet files (all partitions) in S3 for a table.
        :param organization: The organization name.
        :param dataset: The dataset name.
        :param table: The table name.
        :return: A DuckDbParquet instance.
        """
        parquet_prefix = f"catalog-data/{organization}/{dataset}/{table}/"
        return DuckDbParquet(s3_bucket=self._bucket, parquet_prefix=parquet_prefix)

    def get_duckdb_for_partitions(self, organization: str, dataset: str, table: str, partitions: dict[str, str]) -> DuckDbParquet:
        """
        Returns a DuckDbParquet instance for querying specific partitions only.
        This enables true partition pruning by only loading files from the specified partitions.
        
        :param organization: The organization name.
        :param dataset: The dataset name.
        :param table: The table name.
        :param partitions: Dictionary of partition key-value pairs (e.g., {"year": "2025", "month": "01"})
        :return: A DuckDbParquet instance configured for the specific partitions.
        """
        # Build partition path
        partition_path = ""
        for key, value in partitions.items():
            partition_path += f"{key}={value}/"
        
        # Create prefix for the specific partitions
        parquet_prefix = f"catalog-data/{organization}/{dataset}/{table}/{partition_path}"
        
        return DuckDbParquet(s3_bucket=self._bucket, parquet_prefix=parquet_prefix)
