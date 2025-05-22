from catalog_api.models.CatalogModels import CatalogDatasetExtra
from catalog_api.repository.BaseRepository import BaseRepository


class DatasetExtraRepository(BaseRepository[CatalogDatasetExtra]):
    """
    DatasetExtra repository for managing dataset extra records in the database.
    """

    def __init__(self):
        super().__init__(CatalogDatasetExtra)
