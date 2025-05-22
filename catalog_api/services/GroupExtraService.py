from catalog_api.models.CatalogModels import CatalogGroupExtra
from catalog_api.repository.GroupExtraRepository import GroupExtraRepository


class GroupExtraService:
    """
    Service class for managing CatalogGroupExtra operations.
    """

    def __init__(self):
        self.group_extra_repository = GroupExtraRepository()

    def get_by_id(self, extra_id):
        return self.group_extra_repository.get(extra_id)

    def query(self, query=None):
        return self.group_extra_repository.query(filter_fn=lambda q: q.filter(query) if query else None)

    def delete_by_id(self, extra_id):
        return self.group_extra_repository.delete(extra_id)

    def create(self, group_extra: CatalogGroupExtra):
        """
        Create a new GroupExtra object.
        """
        return self.group_extra_repository.create(group_extra)

    def update(self, group_extra: CatalogGroupExtra):
        """
        Update an existing GroupExtra object.
        """
        updates = group_extra.to_dict()  # Assuming a to_dict method exists
        return self.group_extra_repository.update(group_extra.id, updates)

    def save(self, group_extra: CatalogGroupExtra):
        """
        Save a GroupExtra object. If the object has an ID, it updates the object; otherwise, it creates a new one.
        """
        if group_extra.id is not None:
            return self.update(group_extra)
        return self.create(group_extra)
