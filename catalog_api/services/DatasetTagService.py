from catalog_api.models.CatalogModels import CatalogDatasetTag
from catalog_api.repository.DatasetTagRepository import DatasetTagRepository


class DatasetTagService:
    """
    Service class for managing CatalogDatasetTag operations.
    """

    def __init__(self):
        self.dataset_tag_repository = DatasetTagRepository()

    def get_by_id(self, tag_id):
        return self.dataset_tag_repository.get(tag_id)

    def query(self, query=None):
        return self.dataset_tag_repository.query(filter_fn=lambda q: q.filter(query) if query else None)

    def delete_by_id(self, tag_id):
        return self.dataset_tag_repository.delete(tag_id)

    def save(self, dataset_tag: CatalogDatasetTag):
        return self.dataset_tag_repository.create(dataset_tag)
