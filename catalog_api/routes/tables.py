from typing import List, Optional
from fastapi import APIRouter, HTTPException

from catalog_api.models.CatalogModels import CatalogTable
from catalog_api.services.DataTableService import TableService


router = APIRouter()
table_service = TableService()


@router.get("/v1/tables", tags=["Tables"])
def get_table_query(query: Optional[str] = None) -> List[CatalogTable]:
    """
    Gets the ticker info
    """
    return table_service.query_json(query)


@router.post("/v1/tables", tags=["Tables"])
def save_table(table: CatalogTable) -> CatalogTable:
    """
    Save a new table.
    """
    return table_service.save(table)


@router.get("/v1/tables/{table_id}", tags=["Tables"])
def get_table_by_id(table_id: str) -> CatalogTable:
    """
    Get a table by its ID.
    """
    ds = table_service.get_by_id(table_id)
    if ds is None:
        raise HTTPException(
            status_code=404, detail="Table not found with id " + table_id
        )
    return ds


@router.delete("/v1/tables/{table_id}", tags=["Tables"])
def delete_table_by_id(table_id: str) -> None:
    """
    Delete a table by its ID.
    """
    return table_service.delete_by_id(table_id)
