from catalog_api.models.CatalogModels import CatalogGroup
from catalog_api.repository.BaseRepository import BaseRepository


class GroupRepository(BaseRepository[CatalogGroup]):
    """
    Group repository for managing group records in the database.
    """

    def __init__(self):
        super().__init__(CatalogGroup)
