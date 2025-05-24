from datetime import datetime
from io import BytesIO
from typing import Any, Dict, Optional

import pandas as pd
from catalog_api.infrastructure.S3Bucket import S3Bucket


class CatalogDataService:
    """
    CatalogDataService is a service class that provides methods to interact with the catalog data.
    """

    _bucket = S3Bucket()

    def __init__(self):
        """
        Initializes the CatalogDataService with the provided catalog data.
        :param catalog_data: The catalog data to be used by the service.
        """
        self._bucket = S3Bucket()

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
        orgainzation: str,
        dataset: str,
        table: str,
        partitions: Optional[dict] = None,
    ) -> Dict[str, Any]:
        """
        Uploads a DataFrame to S3 as a Parquet file.
        :param df: The DataFrame to be uploaded.
        :param orgainzation: The organization name.
        :param dataset: The dataset name.
        :param table: The table name.
        :param partitions: Optional dictionary of partitions.
        :return: The response from the S3 upload.
        """
        file_path = self.get_parquet_path(orgainzation, dataset, table, partitions)
        pq = BytesIO()
        df.to_parquet(pq, index=False, compression="snappy")
        pq.seek(0)
        return self._bucket.put_bytes(file_path, pq.getvalue())

    def get_parquet_data(
        self,
        orgainzation: str,
        dataset: str,
        table: str,
        partitions: Optional[dict] = None,
    ) -> pd.DataFrame:
        """
        Downloads a Parquet file from S3 and returns it as a DataFrame.
        :param orgainzation: The organization name.
        :param dataset: The dataset name.
        :param table: The table name.
        :param partitions: Optional dictionary of partitions.
        :return: The DataFrame containing the data.
        """
        file_path = self.get_parquet_path(orgainzation, dataset, table, partitions)
        data = self._bucket.get_bytes(file_path)
        return pd.read_parquet(BytesIO(data))

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
            conn = duckdb.connect(database=':memory:')
            conn.execute("CREATE TABLE IF NOT EXISTS dummy_table (id INTEGER);")

            # Save the database to a file-like object
            db_bytes = BytesIO()
            conn.execute("EXPORT DATABASE 'dummy_export';")
            conn.close()

            # Upload the database to S3
            db_bytes.seek(0)
            self._bucket.put_bytes(duckdb_path, db_bytes.getvalue())
