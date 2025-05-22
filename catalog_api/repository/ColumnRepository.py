from catalog_api.models.CatalogModels import CatalogColumn
from catalog_api.repository.BaseRepository import BaseRepository


class ColumnRepository(BaseRepository[CatalogColumn]):
    """
    Column repository for managing column records in the database.
    """

    def __init__(self):
        super().__init__(CatalogColumn)
