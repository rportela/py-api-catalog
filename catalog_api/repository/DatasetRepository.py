from catalog_api.models.CatalogModels import CatalogDataset
from catalog_api.repository.BaseRepository import BaseRepository


class DatasetRepository(BaseRepository[CatalogDataset]):
    """
    Dataset repository for managing dataset records in the database.
    """

    def __init__(self):
        super().__init__(CatalogDataset)
