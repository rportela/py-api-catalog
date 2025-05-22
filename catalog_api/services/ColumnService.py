from catalog_api.models.CatalogModels import CatalogColumn
from catalog_api.repository.ColumnRepository import ColumnRepository


class ColumnService:
    """
    Service class for managing CatalogColumn operations.
    """

    def __init__(self):
        self.column_repository = ColumnRepository()

    def get_by_id(self, column_id):
        return self.column_repository.get(column_id)

    def query(self, query=None):
        return self.column_repository.query(filter_fn=lambda q: q.filter(query) if query else None)

    def delete_by_id(self, column_id):
        return self.column_repository.delete(column_id)

    def create(self, column: CatalogColumn):
        """
        Create a new CatalogColumn object.
        """
        return self.column_repository.create(column)

    def update(self, column: CatalogColumn):
        """
        Update an existing CatalogColumn object.
        """
        updates = column.to_dict()  # Assuming a to_dict method exists
        return self.column_repository.update(column.id, updates)

    def save(self, column: CatalogColumn):
        """
        Save a CatalogColumn object. If the object has an ID, it updates the object; otherwise, it creates a new one.
        """
        if column.id is not None:
            return self.update(column)
        return self.create(column)
