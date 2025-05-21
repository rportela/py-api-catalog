class CkanSynchronizePortal:
    """
    This class synchronizes a CKAN data portal to the catalog objects and stores them.
    """
    _portal_url: str

    def __init__(self, portal_url: str):
        self._portal_url = portal_url
