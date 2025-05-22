from catalog_api.models.CatalogModels import CatalogTable, CatalogColumn
from catalog_api.repository.BaseRepository import BaseRepository

class DataTableService:
    def __init__(self):
        self.table_repository = BaseRepository(CatalogTable)
        self.column_repository = BaseRepository(CatalogColumn)

    def get_table_by_id(self, table_id: str) -> CatalogTable:
        """
        Get a CKAN table by its ID.
        """
        assert table_id is not None, "Table ID cannot be None"
        tables = self.table_repository.query(
            filter_fn=lambda q: q.filter(CatalogTable.id == table_id)
        )
        return tables[0] if len(tables) > 0 else None

    def get_column_by_id(self, column_id: str) -> CatalogColumn:
        """
        Get a CKAN column by its ID.
        """
        assert column_id is not None, "Column ID cannot be None"
        columns = self.column_repository.query(
            filter_fn=lambda q: q.filter(CatalogColumn.id == column_id)
        )
        return columns[0] if len(columns) > 0 else None