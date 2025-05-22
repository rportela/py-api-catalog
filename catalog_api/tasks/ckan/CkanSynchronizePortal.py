from catalog_api.services.CatalogService import CatalogService
from catalog_api.infrastructure.CkanApi import CkanApi

class CkanSynchronizePortal:
    """
    This class synchronizes a CKAN data portal to the catalog objects and stores them.
    """
    _portal_url: str

    def __init__(self, service: CatalogService, api: CkanApi):
        self.service = service
        self.api = api

    def synchronize(self):
        packages = self.api.list_packages() or []
        for package_id in packages:
            package = self.api.get_package(package_id)

            organization_data = {
                "name": package.organization.name,
                "title": package.organization.title,
                "description": package.organization.description,
                "created": package.organization.created,
                "state": package.organization.state,
                "type": package.organization.type,
                "approval_status": package.organization.approval_status,
                "image_url": package.organization.image_url,
                "is_organization": package.organization.is_organization,
            }
            organization = self.service.get_or_create_organization(organization_data)

            dataset_data = {
                "name": package.name,
                "title": package.title,
                "version": package.version,
                "url": package.url,
                "notes": package.notes,
                "license_id": package.license_id,
                "author": package.author,
                "author_email": package.author_email,
                "maintainer": package.maintainer,
                "maintainer_email": package.maintainer_email,
                "state": package.state,
                "type": package.type,
                "private": package.private,
                "owner_org": organization.id,
                "metadata_created": package.metadata_created,
                "metadata_modified": package.metadata_modified,
            }
            dataset = self.service.get_or_create_dataset(organization, dataset_data)

            for resource in package.resources or []:
                resource_data = {
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
                    "last_modified": resource.last_modified,
                    "cache_url": resource.cache_url,
                    "cache_last_updated": resource.cache_last_updated,
                    "created": resource.created,
                    "url_type": resource.url_type,
                    "state": resource.state,
                    "position": resource.position,
                }
                self.service.get_or_create_resource(dataset, resource_data)
