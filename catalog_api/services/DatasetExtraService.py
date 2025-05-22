from catalog_api.models.CatalogModels import CatalogDatasetExtra
from catalog_api.repository.DatasetExtraRepository import DatasetExtraRepository


class DatasetExtraService:
    """
    Service class for managing CatalogDatasetExtra operations.
    """

    def __init__(self):
        self.dataset_extra_repository = DatasetExtraRepository()

    def get_by_id(self, extra_id):
        return self.dataset_extra_repository.get(extra_id)

    def query(self, query=None):
        return self.dataset_extra_repository.query(filter_fn=lambda q: q.filter(query) if query else None)

    def delete_by_id(self, extra_id):
        return self.dataset_extra_repository.delete(extra_id)

    def save(self, dataset_extra: CatalogDatasetExtra):
        return self.dataset_extra_repository.create(dataset_extra)
