from typing import Dict, List, Optional
from pydantic import BaseModel
import requests

# Define models for CKAN entities using Pydantic for validation and type safety
class CkanResource(BaseModel):
    cache_last_updated: Optional[str] = None
    cache_url: Optional[str] = None
    created: str
    description: Optional[str] = None
    format: Optional[str] = None
    hash: Optional[str] = None
    id: str
    last_modified: Optional[str] = None
    metadata_modified: str
    mimetype: Optional[str] = None
    mimetype_inner: Optional[str] = None
    name: str
    package_id: str
    position: int
    resource_type: Optional[str] = None
    size: Optional[str] = None
    state: Optional[str] = None
    url: str
    url_type: Optional[str] = None


class CkanExtra(BaseModel):
    key: str
    value: str


class CkanOrganization(BaseModel):
    id: str
    name: str
    title: str
    type: str
    description: str
    image_url: str
    created: str
    is_organization: bool
    approval_status: str
    state: str


class CkanGroup(BaseModel):
    description: str
    display_name: str
    id: str
    image_display_url: str
    name: str
    title: str


class CkanTag(BaseModel):
    display_name: str
    id: str
    name: str
    state: str
    vocabulary_id: Optional[str] = None


class CkanPackage(BaseModel):
    author: str
    author_email: str
    creator_user_id: str
    id: str
    isopen: bool
    license_id: str
    license_title: str
    license_url: str
    maintainer: str
    maintainer_email: str
    metadata_created: str
    metadata_modified: Optional[str] = None
    name: str
    notes: str
    num_resources: int
    num_tags: int
    organization: CkanOrganization
    owner_org: str
    private: bool
    state: str
    title: str
    type: str
    url: str
    version: str
    extras: Optional[List[CkanExtra]] = None
    groups: Optional[List[CkanGroup]] = None
    resources: Optional[List[CkanResource]] = None
    tags: Optional[List[CkanTag]] = None
    relationships_as_subject: Optional[List[Dict]] = None
    relationships_as_object: Optional[List[Dict]] = None


# CKAN API client for interacting with CKAN instances
class CkanApi:
    base_url: str  # Base URL of the CKAN instance
    api_url: str  # API endpoint URL

    def __init__(self, base_url: str):
        """
        Initialize the CKAN API client.

        :param base_url: The base URL of the CKAN instance.
        """
        self.base_url = base_url
        self.api_url = f"{base_url}/api/3/action"

    def get_package(self, package_id: str) -> CkanPackage:
        """
        Retrieve a CKAN package by its ID.

        :param package_id: The ID of the package to retrieve.
        :return: A CkanPackage object representing the package.
        """
        url = f"{self.api_url}/package_show?id={package_id}"
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        result = data.get("result")
        if not result:
            raise ValueError("No result found in the response.")
        return CkanPackage.model_validate(result)

    def list_packages(self) -> List[str]:
        """
        List all package IDs available in the CKAN instance.

        :return: A list of package IDs.
        """
        url = f"{self.api_url}/package_list"
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        package_ids = data.get("result", [])
        if not isinstance(package_ids, list):
            raise TypeError("Expected a list of package IDs in the response.")
        return package_ids
