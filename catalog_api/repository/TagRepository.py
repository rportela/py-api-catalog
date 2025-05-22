from catalog_api.models.CatalogModels import CatalogTag
from catalog_api.repository.BaseRepository import BaseRepository


class TagRepository(BaseRepository[CatalogTag]):
    """
    Tag repository for managing tag records in the database.
    """

    def __init__(self):
        super().__init__(CatalogTag)
