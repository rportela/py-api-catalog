from catalog_api.infrastructure.CkanApi import CkanPackage
from catalog_api.models.CatalogModels import CatalogGroup, CatalogDataset
from datetime import datetime

from catalog_api.services.DatasetService import DatasetService
from catalog_api.tasks.ckan.CkanRegisterLicense import CkanRegisterLicense


class CkanRegisterDataset:
    def __init__(self):
        self._register_license = CkanRegisterLicense()
        self._service = DatasetService()
        
    def execute(self, org: CatalogGroup, pkg: CkanPackage) -> CatalogDataset:
        catalog_name = pkg.name.lower().replace(" ", "_")
        datasets = self._service.query_datasets_by_name_and_org(catalog_name, str(org.id))
        dataset = datasets[0] if len(datasets) > 0 else None
        if dataset:
            return dataset
        license = self._register_license.register_license(license_id=pkg.license_id, title=pkg.license_title, url=pkg.license_url)
        dataset = self._service.create_dataset(
            {
                "name": catalog_name,
                "title": pkg.title,
                "version": pkg.version,
                "url": pkg.url,
                "notes": pkg.notes,
                "license_id": pkg.license_id,
                "author": pkg.author,
                "author_email": pkg.author_email,
                "maintainer": pkg.maintainer,
                "maintainer_email": pkg.maintainer_email,
                "state": pkg.state,
                "type": pkg.type,
                "private": pkg.private,
                "owner_org": org.id,
                "metadata_created": datetime.fromisoformat(pkg.metadata_created),
                "metadata_modified": (
                    datetime.fromisoformat(pkg.metadata_modified)
                    if pkg.metadata_modified
                    else None
                ),
            }
        )
        return dataset
