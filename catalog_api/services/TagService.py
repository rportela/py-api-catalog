from catalog_api.models.CatalogModels import CatalogTag
from catalog_api.repository.TagRepository import TagRepository


class TagService:
    """
    Service class for managing CatalogTag operations.
    """

    def __init__(self):
        self.tag_repository = TagRepository()

    def get_by_id(self, tag_id):
        return self.tag_repository.get(tag_id)

    def query(self, query=None):
        return self.tag_repository.query(filter_fn=lambda q: q.filter(query) if query else None)

    def delete_by_id(self, tag_id):
        return self.tag_repository.delete(tag_id)

    def save(self, tag: CatalogTag):
        return self.tag_repository.create(tag)
