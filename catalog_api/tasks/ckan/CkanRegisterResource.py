from catalog_api.services.CatalogService import CatalogService
from catalog_api.infrastructure.CkanApi import CkanResource
from catalog_api.models.CatalogModels import CatalogDataset
from datetime import datetime

class CkanRegisterResource:
    def __init__(self, service: CatalogService):
        self.service = service

    def execute(self, dataset: CatalogDataset, resource: CkanResource):
        res = self.service.query_resources_by_id(resource.id)
        if not res:
            res = self.service.create_resource(
                {
                    "id": resource.id,
                    "package_id": dataset.id,
                    "url": resource.url,
                    "format": resource.format,
                    "description": resource.description,
                    "name": resource.name,
                    "resource_type": resource.resource_type,
                    "mimetype": resource.mimetype,
                    "mimetype_inner": resource.mimetype_inner,
                    "size": resource.size,
                    "hash": resource.hash,
                    "last_modified": (
                        datetime.fromisoformat(resource.last_modified)
                        if resource.last_modified
                        else None
                    ),
                    "cache_url": resource.cache_url,
                    "cache_last_updated": (
                        datetime.fromisoformat(resource.cache_last_updated)
                        if resource.cache_last_updated
                        else None
                    ),
                    "created": datetime.fromisoformat(resource.created),
                    "url_type": resource.url_type,
                    "state": resource.state,
                    "position": resource.position,
                }
            )
        return res