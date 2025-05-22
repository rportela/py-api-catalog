from catalog_api.models.CatalogModels import CatalogDatasetRelationship
from catalog_api.repository.BaseRepository import BaseRepository


class RelationshipRepository(BaseRepository[CatalogDatasetRelationship]):
    """
    Relationship repository for managing dataset relationship records in the database.
    """

    def __init__(self):
        super().__init__(CatalogDatasetRelationship)
