from catalog_api.infrastructure.Ckan import CkanPackage
from .CkanRegisterGroup import CkanRegisterGroup


class CkanRegisterDataset:

    _register_group: CkanRegisterGroup

    def __init__(self):
        self._register_group = CkanRegisterGroup()

    def execute(self, package: CkanPackage):
        package.organization
        pass
