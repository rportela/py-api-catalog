from catalog_api.models.CatalogModels import CatalogGroup
from catalog_api.repository.BaseRepository import BaseRepository
from sqlalchemy.orm import Session

class CatalogGroupService:
    def __init__(self, db: Session):
        self._group_repo = BaseRepository(CatalogGroup)

    def query_groups_by_name(self, name: str):
        return self._group_repo.query(
            filter_fn=lambda q: q.filter(CatalogGroup.name == name)
        )

    def create_group(self, group_data: dict):
        return self._group_repo.create(group_data)

    def get_organization_by_id(self, org_id: str) -> CatalogGroup:
        """
        Get a CKAN organization by its ID.
        """
        assert org_id is not None, "Organization ID cannot be None"
        groups = self._group_repo.query(
            filter_fn=lambda q: q.filter(CatalogGroup.id == org_id)
        )
        return groups[0] if len(groups) > 0 else None

    def get_or_create_organization(self, org_data: dict):
        org = self._group_repo.query(
            filter_fn=lambda q: q.filter(CatalogGroup.name == org_data["name"])
        )
        if org:
            return org[0]
        return self._group_repo.create(org_data)