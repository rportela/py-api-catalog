from catalog_api.services.LicenseService import LicenseService

class CkanRegisterLicense:
    """
    Task class for registering a license in CKAN.
    """

    def __init__(self):
        self.license_service = LicenseService()

    def register_license(self, license_id, title, is_okd_compliant=False, is_generic=False, url=None, home_url=None):
        """
        Check if a license exists by its ID and create it if it does not.

        :param license_id: The unique ID of the license (e.g., 'cc-by-4.0').
        :param title: The full name of the license.
        :param is_okd_compliant: Whether the license is OKD compliant.
        :param is_generic: Whether the license is generic.
        :param url: The URL to the license text.
        :param home_url: The optional homepage URL of the license.
        :return: The registered or existing license.
        """
        # Check if the license already exists
        existing_license = self.license_service.query({"id": license_id})
        if existing_license:
            return existing_license[0]  # Return the first matching license

        # Create a new license if it does not exist
        new_license_data = {
            "id": license_id,
            "title": title,
            "is_okd_compliant": is_okd_compliant,
            "is_generic": is_generic,
            "url": url,
            "home_url": home_url,
        }
        new_license = self.license_service.license_repository.create(new_license_data)
        return new_license