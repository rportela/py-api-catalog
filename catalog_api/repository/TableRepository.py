from catalog_api.models.CatalogModels import CatalogTable
from catalog_api.repository.BaseRepository import BaseRepository


class TableRepository(BaseRepository[CatalogTable]):
    """
    Table repository for managing table records in the database.
    """

    def __init__(self):
        super().__init__(CatalogTable)
