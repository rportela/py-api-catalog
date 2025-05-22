from catalog_api.services.CatalogGroupService import CatalogGroupService
from catalog_api.infrastructure.CkanApi import CkanOrganization
from catalog_api.models.CatalogModels import CatalogGroup
from datetime import datetime


class CkanRegisterGroup:
    def __init__(self, service: CatalogGroupService):
        self.service = service

    def execute(self, country_code: str, org_type: str, org: CkanOrganization) -> CatalogGroup:
        catalog_name = f"{country_code}_{org_type}_{org.name.lower().replace(' ', '_')}"
        groups = self.service.query_groups_by_name(catalog_name)
        group = groups[0] if len(groups) > 0 else None
        if group:
            return group
        group = self.service.create_group(
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