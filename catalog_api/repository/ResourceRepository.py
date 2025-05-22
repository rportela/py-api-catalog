from catalog_api.models.CatalogModels import CatalogResource
from catalog_api.repository.BaseRepository import BaseRepository


class ResourceRepository(BaseRepository[CatalogResource]):
    """
    Resource repository for managing resource records in the database.
    """

    def __init__(self):
        super().__init__(CatalogResource)
