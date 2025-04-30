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


class CatalogService:

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

    def get_organization_by_id(self, org_id: str) -> CatalogGroup:
        """
        Get a CKAN organization by its ID.
        """
        assert org_id is not None, "Organization ID cannot be None"
        groups = self._organization_repo.query(
            filter_fn=lambda q: q.filter(CatalogGroup.id == org_id)
        )
        return groups[0] if len(groups) > 0 else None

    def get_dataset_by_id(self, dataset_id: str) -> CatalogDataset:
        """
        Get a CKAN dataset by its ID.
        """
        assert dataset_id is not None, "Dataset ID cannot be None"
        datasets = self._dataset_repo.query(
            filter_fn=lambda q: q.filter(CatalogDataset.id == dataset_id)
        )
        return datasets[0] if len(datasets) > 0 else None

    def get_table_by_id(self, table_id: str) -> CatalogTable:
        """
        Get a CKAN table by its ID.
        """
        assert table_id is not None, "Table ID cannot be None"
        tables = self._table_repo.query(
            filter_fn=lambda q: q.filter(CatalogTable.id == table_id)
        )
        return tables[0] if len(tables) > 0 else None

    def get_column_by_id(self, column_id: str) -> CatalogColumn:
        """
        Get a CKAN column by its ID.
        """
        assert column_id is not None, "Column ID cannot be None"
        columns = self._column_repo.query(
            filter_fn=lambda q: q.filter(CatalogColumn.id == column_id)
        )
        return columns[0] if len(columns) > 0 else None

    def get_resource_by_id(self, resource_id: str) -> CatalogResource:
        """
        Get a CKAN resource by its ID.
        """
        assert resource_id is not None, "Resource ID cannot be None"
        resources = self._resource_repo.query(
            filter_fn=lambda q: q.filter(CatalogResource.id == resource_id)
        )
        return resources[0] if len(resources) > 0 else None

    def get_license_by_id(self, license_id: str) -> CatalogLicense:
        """
        Get a CKAN license by its ID.
        """
        assert license_id is not None, "License ID cannot be None"
        licenses = self._license_repo.query(
            filter_fn=lambda q: q.filter(CatalogLicense.id == license_id)
        )
        return licenses[0] if len(licenses) > 0 else None
