from typing import Dict, List, Optional
from pydantic import BaseModel
import requests


class CkanResource(BaseModel):
    cache_last_updated: str | None
    cache_url: str | None
    created: str
    description: str | None
    format: str | None
    hash: str | None
    id: str
    last_modified: str | None
    metadata_modified: str
    mimetype: str | None
    mimetype_inner: str | None
    name: str
    package_id: str
    position: int
    resource_type: str | None
    size: str | None
    state: str | None
    url: str
    url_type: str | None


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
    vocabulary_id: str | None


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
    metadata_modified: str | None
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


class CkanPortal:
    base_url: str
    api_url: str

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.api_url = f"{base_url}/api/3/action"

    def get_package(self, package_id: str) -> CkanPackage:
        url = f"{self.api_url}/package_show?id={package_id}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        result = data.get("result")
        return CkanPackage.model_validate(result)

    def list_packages(self) -> List[str]:
        url = f"{self.api_url}/package_list"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        package_ids = data.get("result", [])
        return package_ids
