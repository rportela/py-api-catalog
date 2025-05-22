from catalog_api.models.CatalogModels import CatalogDatasetRelationship
from catalog_api.repository.RelationshipRepository import RelationshipRepository


class RelationshipService:
    """
    Service class for managing CatalogDatasetRelationship operations.
    """

    def __init__(self):
        self.relationship_repository = RelationshipRepository()

    def get_by_id(self, relationship_id):
        return self.relationship_repository.get(relationship_id)

    def query(self, query=None):
        return self.relationship_repository.query(filter_fn=lambda q: q.filter(query) if query else None)

    def delete_by_id(self, relationship_id):
        return self.relationship_repository.delete(relationship_id)

    def save(self, relationship: CatalogDatasetRelationship):
        return self.relationship_repository.create(relationship)
