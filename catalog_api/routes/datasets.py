from typing import List, Optional
from fastapi import APIRouter, HTTPException

from catalog_api.models.CatalogModels import CatalogDataset
from catalog_api.services.DatasetService import DatasetService


router = APIRouter()
dataset_service = DatasetService()


@router.get("/v1/datasets", tags=["Datasets"])
def get_dataset_query(query: Optional[str] = None) -> List[CatalogDataset]:
    """
    Gets the ticker info
    """
    return dataset_service.query_json(query)


@router.post("/v1/datasets", tags=["Datasets"])
def save_dataset(dataset: CatalogDataset) -> CatalogDataset:
    """
    Save a new dataset.
    """
    return dataset_service.save(dataset)


@router.get("/v1/datasets/{dataset_id}", tags=["Datasets"])
def get_dataset_by_id(dataset_id: str) -> CatalogDataset:
    """
    Get a dataset by its ID.
    """
    ds = dataset_service.get_by_id(dataset_id)
    if ds is None:
        raise HTTPException(
            status_code=404, detail="Dataset not found with id " + dataset_id
        )
    return ds


@router.delete("/v1/datasets/{dataset_id}", tags=["Datasets"])
def delete_dataset_by_id(dataset_id: str) -> None:
    """
    Delete a dataset by its ID.
    """
    return dataset_service.delete_by_id(dataset_id)
