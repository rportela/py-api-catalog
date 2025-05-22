from catalog_api.models.CatalogModels import CatalogResource
from catalog_api.repository.ResourceRepository import ResourceRepository


class ResourceService:
    """
    Service class for managing CatalogResource operations.
    """

    def __init__(self):
        self.resource_repository = ResourceRepository()

    def get_by_id(self, resource_id):
        return self.resource_repository.get(resource_id)

    def query(self, query=None):
        return self.resource_repository.query(filter_fn=lambda q: q.filter(query) if query else None)

    def delete_by_id(self, resource_id):
        return self.resource_repository.delete(resource_id)

    def save(self, resource: CatalogResource):
        return self.resource_repository.create(resource)
