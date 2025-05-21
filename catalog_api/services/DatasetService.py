from typing import List, Optional

from catalog_api.models.CatalogModels import CatalogDataset
from catalog_api.repository.DatasetRepository import DatasetRepository


class DatasetService:
    _repo: DatasetRepository
    
    def __init__(self):
        self.dataset_repository = None

    def get_by_id(self, dataset_id):
        return self.dataset_repository.get_by_id(dataset_id)

    def query(self, query: Optional[DataQuery] = None) -> List[CatalogDataset]:
        """
        Query the dataset repository with a DataQuery object.
        """
        return self.dataset_repository.query(query)

    def query_json(self, query: Optional[str] = None) -> List[CatalogDataset]:
        """
        Query the dataset repository with a JSON query string.
        """
        return self.dataset_repository.query_json(query)

    def delete_by_id(self, dataset_id: str | int):
        return self.dataset_repository.delete(dataset_id)

    def save(self, dataset: CatalogDataset) -> CatalogDataset:
        """
        Save a new dataset.
        """
        return self.dataset_repository.save(dataset)
