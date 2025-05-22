from catalog_api.models.CatalogModels import CatalogGroupExtra
from catalog_api.repository.BaseRepository import BaseRepository


class GroupExtraRepository(BaseRepository[CatalogGroupExtra]):
    """
    GroupExtra repository for managing group extra records in the database.
    """

    def __init__(self):
        super().__init__(CatalogGroupExtra)
