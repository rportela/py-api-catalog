from catalog_api.models.CatalogModels import CatalogTable
from catalog_api.repository.TableRepository import TableRepository


class TableService:
    """
    Service class for managing CatalogTable operations.
    """

    def __init__(self):
        self.table_repository = TableRepository()

    def get_by_id(self, table_id):
        return self.table_repository.get(table_id)

    def query(self, query=None):
        return self.table_repository.query(filter_fn=lambda q: q.filter(query) if query else None)

    def delete_by_id(self, table_id):
        return self.table_repository.delete(table_id)

    def create(self, table):
        """
        Create a new Table object.
        """
        return self.table_repository.create(table)

    def update(self, table):
        """
        Update an existing Table object.
        """
        updates = table.to_dict()  # Assuming a to_dict method exists
        return self.table_repository.update(table.id, updates)

    def save(self, table):
        """
        Save a Table object. If the object has an ID, it updates the object; otherwise, it creates a new one.
        """
        if table.id:
            return self.update(table)
        return self.create(table)
