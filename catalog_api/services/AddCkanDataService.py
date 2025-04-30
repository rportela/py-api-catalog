from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field
from sqlalchemy import UUID

from portela_dev.infrastructure.Ckan import CkanOrganization, CkanPackage, CkanResource
from portela_dev.repo.CatalogModels import (
    CatalogDataset,
    CatalogGroup,
    CatalogLicense,
    CatalogResource,
    CatalogTable,
    CatalogColumn,
)
from portela_dev.repo.CatalogRepo import BaseRepository
from sqlalchemy.orm import Session


class AddCkanDataService:

    _organization_repo: BaseRepository[CatalogGroup]
    _dataset_repo: BaseRepository[CatalogDataset]
    _table_repo: BaseRepository[CatalogTable]
    _column_repo: BaseRepository[CatalogColumn]
    _resource_repo: BaseRepository[CatalogResource]
    _license_repo: BaseRepository[CatalogLicense]

    def __init__(self, db: Session):
        self._organization_repo = BaseRepository(db, CatalogGroup)
        self._dataset_repo = BaseRepository(db, CatalogDataset)
        self._table_repo = BaseRepository(db, CatalogTable)
        self._column_repo = BaseRepository(db, CatalogColumn)
        self._resource_repo = BaseRepository(db, CatalogResource)
        self._license_repo = BaseRepository(db, CatalogLicense)

    def register_organization(
        self, country_code: str, org_type: str, org: CkanOrganization
    ) -> CatalogGroup:
        """
        Register a CKAN organization.
        """
        assert org is not None, "Organization cannot be None"
        catalog_name = f"{country_code}_{org_type}_{org.name.lower().replace(' ', '_')}"
        groups = self._organization_repo.query(
            filter_fn=lambda q: q.filter(CatalogGroup.name == catalog_name)
        )
        group = groups[0] if len(groups) > 0 else None
        if group:
            return group
        group = self._organization_repo.create(
            {
                "name": catalog_name,
                "title": org.title,
                "description": org.description,
                "created": datetime.fromisoformat(org.created),
                "state": org.state,
                "type": org.type,
                "approval_status": org.approval_status,
                "image_url": org.image_url,
                "is_organization": org.is_organization,
            }
        )
        return group

    def register_license(self, pkg: CkanPackage):
        """
        Register a CKAN license.
        """
        assert pkg is not None, "Package cannot be None"
        license = self._license_repo.get(pkg.license_id)
        if not license:
            self._license_repo.create(
                {
                    "id": pkg.license_id,
                    "title": pkg.license_title,
                    "url": pkg.license_url,
                }
            )

    def register_dataset(self, org: CatalogGroup, pkg: CkanPackage):
        catalog_name = pkg.name.lower().replace(" ", "_")
        datasets = self._dataset_repo.query(
            filter_fn=lambda q: q.filter(
                CatalogDataset.owner_org == org.id, CatalogDataset.name == catalog_name
            )
        )
        dataset = datasets[0] if len(datasets) > 0 else None
        if dataset:
            return dataset
        self.register_license(pkg)
        dataset = self._dataset_repo.create(
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

    def register_resource(self, dataset: CatalogDataset, resource: CkanResource):
        res = self._resource_repo.get(resource.id)
        if not res:
            res = self._resource_repo.create(
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

    def register_package(
        self, country_code: str, org_type: str, pkg: CkanPackage
    ) -> tuple[CatalogGroup, CatalogDataset | None, List[CatalogResource]]:
        org = self.register_organization(country_code, org_type, pkg.organization)
        dataset = self.register_dataset(org, pkg)
        pkgs: List[CatalogResource] = []
        if pkg.resources:
            for resource in pkg.resources:
                res = self.register_resource(dataset, resource)
                pkgs.append(res)
        return org, dataset, pkgs

    def register_table(self, table: CatalogTable) -> CatalogTable:
        """
        Register a CKAN table.
        """
        assert table is not None, "Table cannot be None"
        tables = self._table_repo.query(
            filter_fn=lambda q: q.filter(
                CatalogDataset.id == table.dataset_id, CatalogTable.name == table.name
            )
        )
        tbl = tables[0] if len(tables) > 0 else None
        if tbl:
            return tbl
        tbl = self._table_repo.create(
            {
                "name": table.name,
                "description": table.description,
                "created": table.created,
                "state": table.state,
                "dataset_id": table.dataset_id,
            }
        )
        return tbl

    def register_column(self, column: CatalogColumn) -> CatalogColumn:
        """
        Register a CKAN column.
        """
        assert column is not None, "Column cannot be None"
        columns = self._column_repo.query(
            filter_fn=lambda q: q.filter(
                CatalogColumn.table_id == column.table_id,
                CatalogColumn.name == column.name,
            )
        )
        col = columns[0] if len(columns) > 0 else None
        if col:
            return col
        col = self._column_repo.create(
            {
                "name": column.name,
                "data_type": column.data_type,
                "description": column.description,
                "created": column.created,
                "table_id": column.table_id,
                "precision": column.precision,
                "scale": column.scale,
                "state": "active",
                "is_nullable": column.is_nullable,
                "is_primary_key": column.is_primary_key,
                "is_unique": column.is_unique,
            }
        )
        return col

    def get_meta_resources(self):
        """
        Get all CKAN meta resources.
        """
        resources = self._resource_repo.query(
            filter_fn=lambda q: q.filter(CatalogResource.url.like("%/meta_%"))
        )
        return resources
