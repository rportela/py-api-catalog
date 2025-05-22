from catalog_api.models.CatalogModels import CatalogMember
from catalog_api.repository.BaseRepository import BaseRepository


class MemberRepository(BaseRepository[CatalogMember]):
    """
    Member repository for managing member records in the database.
    """

    def __init__(self):
        super().__init__(CatalogMember)
