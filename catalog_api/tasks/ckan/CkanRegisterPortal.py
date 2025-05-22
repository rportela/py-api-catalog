from catalog_api.services.CkanService import AddCkanDataService
from catalog_api.infrastructure.CkanApi import CkanApi

class CkanRegisterPortal:
    def __init__(self, service: AddCkanDataService, api: CkanApi):
        self.service = service
        self.api = api

    def execute(self):
        groups = self.api.list_groups()
        for group in groups:
            self.service.register_organization("country_code", "org_type", group)

        packages = self.api.list_packages()
        for package_id in packages:
            package = self.api.get_package(package_id)
            self.service.register_dataset(package.organization, package)