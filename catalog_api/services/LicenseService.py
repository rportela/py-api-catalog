from catalog_api.models.CatalogModels import CatalogLicense
from catalog_api.repository.LicenseRepository import LicenseRepository


class LicenseService:
    """
    Service class for managing CatalogLicense operations.
    """

    def __init__(self):
        self.license_repository = LicenseRepository()

    def get_by_id(self, license_id):
        return self.license_repository.get(license_id)

    def query(self, query=None):
        return self.license_repository.query(filter_fn=lambda q: q.filter(query) if query else None)

    def delete_by_id(self, license_id):
        return self.license_repository.delete(license_id)

    def create(self, license: CatalogLicense):
        return self.license_repository.create(license)
