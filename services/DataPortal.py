from typing import List, Optional
from pydantic import BaseModel


class DataPortal(BaseModel):
    name: str
    title: str
    url: str
    author: str
    publisher: str
    issued: Optional[str] = None
    publisher_classification: Optional[str] = None
    description: str
    tags: Optional[List[str]] = None
    license_id: Optional[str] = None
    license_url: Optional[str] = None
    license_notes: Optional[str] = None
    place: Optional[str] = None
    location: Optional[str] = None
    country: Optional[str] = None
    language: Optional[str] = None
    status: Optional[str] = None
    metadatacreated: Optional[str] = None
    generator: Optional[str] = None
    api_endpoint: Optional[str] = None
    api_type: Optional[str] = None
    full_metadata_download: Optional[str] = None
    id: str
    description_html: str
    groups: []


class DataPortalService:
    pass
