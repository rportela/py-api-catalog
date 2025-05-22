from catalog_api.models.CatalogModels import CatalogLicense
from catalog_api.repository.BaseRepository import BaseRepository


class LicenseRepository(BaseRepository[CatalogLicense]):
    """
    License repository for managing license records in the database.
    """

    def __init__(self):
        super().__init__(CatalogLicense)
