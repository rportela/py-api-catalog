from typing import List, Optional

from catalog_api.models.CatalogModels import CatalogDataset, CatalogGroup
from catalog_api.repository.DatasetRepository import DatasetRepository


class DatasetService:
    _repo: DatasetRepository
    
    def __init__(self):
        self.dataset_repository = DatasetRepository()

    def get_by_id(self, dataset_id):
        return self.dataset_repository.get(dataset_id)

    def query(self, query: Optional[str] = None) -> List[CatalogDataset]:
        """
        Query the dataset repository with a JSON query string.
        """
        return self.dataset_repository.query(
            filter_fn=lambda q: q.filter(query) if query else None
        )

    def query_json(self, query: Optional[str] = None) -> List[CatalogDataset]:
        """
        Query the dataset repository with a JSON query string.
        """
        return self.query(query)

    def delete_by_id(self, dataset_id: str | int):
        return self.dataset_repository.delete(dataset_id)

    def save(self, dataset: CatalogDataset) -> CatalogDataset:
        """
        Save a new dataset.
        """
        return self.dataset_repository.create(dataset)

    def get_dataset_by_id(self, dataset_id: str) -> CatalogDataset:
        """
        Get a CKAN dataset by its ID.
        """
        assert dataset_id is not None, "Dataset ID cannot be None"
        return self.dataset_repository.get(dataset_id)

    def query_datasets_by_name_and_org(self, name: str, org_id: str):
        return self.dataset_repository.query(
            filter_fn=lambda q: q.filter(
                CatalogDataset.name == name, CatalogDataset.owner_org == org_id
            )
        )

    def create_dataset(self, dataset_data: dict):
        return self.dataset_repository.create(dataset_data)

    def get_or_create_dataset(self, org: CatalogGroup, pkg: dict):
        datasets = self.query_datasets_by_name_and_org(pkg["name"], str(org.id))
        if datasets:
            return datasets[0]
        return self.create_dataset(pkg)
