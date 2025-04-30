from portela_dev.infrastructure.Ckan import (
    CkanOrganization,
    CkanPackage,
    CkanPortal,
    CkanResource,
)

class CkanPortalWrapper:
    _portal: CkanPortal

    def __init__(self, base_url: str):
        assert base_url is not None, "Portal base_url cannot be None"
        self._portal = CkanPortal(base_url)

    def _register_organization(self, org: CkanOrganization):
        """
        Register a CKAN organization.
        """
        assert org is not None, "Organization cannot be None"
        if not self._portal.organization_exists(org.name):
            self._portal.create_organization(org)
        else:
            print(f"Organization {org.name} already exists.")

    def _register_dataset(self, pkg: CkanPackage):
        """
        Register a CKAN package.
        """
        assert pkg is not None, "Package cannot be None"
        if not self._portal.package_exists(pkg.name):
            pass

    def _register_resource(self, resource: CkanResource):
        """
        Register a CKAN resource.
        """
        pass

    def find_updatable_resources(self, package_name: str):
        """
        Find updatable resources in a CKAN package.
        """
        assert package_name is not None, "Package name cannot be None"
        package = self._portal.get_package(package_name)
        resources = package.resources or []
        updatable_resources = []
        for resource in resources:
            if resource.updatable:
                updatable_resources.append(resource)
        return updatable_resources
