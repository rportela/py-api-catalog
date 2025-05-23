from typing import List
from catalog_api.models.CatalogModels import CatalogGroup
from catalog_api.repository.GroupRepository import GroupRepository


class GroupService:
    """
    Service class for managing CatalogGroup operations.
    """

    def __init__(self):
        self.group_repository = GroupRepository()

    def get_by_id(self, group_id) -> CatalogGroup | None:
        return self.group_repository.get(group_id)

    def query(self, query=None) -> List[CatalogGroup]:
        return self.group_repository.query(
            filter_fn=lambda q: q.filter(query) if query else None
        )

    def delete_by_id(self, group_id) -> bool:
        return self.group_repository.delete(group_id)

    def create(self, group):
        """
        Create a new Group object.
        """
        return self.group_repository.create(group)

    def update(self, group) -> CatalogGroup | None:
        """
        Update an existing Group object.
        """
        updates = group.to_dict()  # Assuming a to_dict method exists
        return self.group_repository.update(group.id, updates)

    def save(self, group) -> CatalogGroup | None:
        """
        Save a Group object. If the object has an ID, it updates the object; otherwise, it creates a new one.
        """
        if group.id:
            return self.update(group)
        return self.create(group)
