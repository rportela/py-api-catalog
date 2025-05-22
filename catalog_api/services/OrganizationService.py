from catalog_api.models.CatalogModels import CatalogGroup
from catalog_api.repository.BaseRepository import BaseRepository

class OrganizationService:
    def __init__(self):
        self.organization_repository = BaseRepository(CatalogGroup)

    def get_organization_by_id(self, org_id: str) -> CatalogGroup:
        """
        Get a CKAN organization by its ID.
        """
        assert org_id is not None, "Organization ID cannot be None"
        groups = self.organization_repository.query(
            filter_fn=lambda q: q.filter(CatalogGroup.id == org_id)
        )
        return groups[0] if len(groups) > 0 else None

    def get_or_create_organization(self, org_data: dict):
        org = self.organization_repository.query(
            filter_fn=lambda q: q.filter(CatalogGroup.name == org_data["name"])
        )
        if org:
            return org[0]
        return self.organization_repository.create(org_data)