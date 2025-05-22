from catalog_api.models.CatalogModels import CatalogDatasetTag
from catalog_api.repository.BaseRepository import BaseRepository


class DatasetTagRepository(BaseRepository[CatalogDatasetTag]):
    """
    DatasetTag repository for managing dataset tag records in the database.
    """

    def __init__(self):
        super().__init__(CatalogDatasetTag)
