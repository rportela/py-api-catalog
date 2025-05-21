from sqlalchemy import (
    Column,
    String,
    Text,
    Boolean,
    DateTime,
    Integer,
    ForeignKey,
    BigInteger,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from . import Base
import uuid


class CatalogGroup(Base):
    """Represents a CKAN group or organization."""

    __tablename__ = "catalog_group"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False
    )
    name = Column(String(100), unique=True, nullable=False)
    title = Column(String(200), nullable=True)  # title of group/organization
    description = Column(Text, nullable=True)  # detailed description
    created = Column(DateTime, nullable=False)  # timestamp when created
    state = Column(
        String(10), default="active", nullable=False
    )  # 'active' or 'deleted'
    type = Column(
        String(50), default="group", nullable=False
    )  # e.g. 'group' or 'organization'
    approval_status = Column(String(20), default="approved", nullable=False)
    image_url = Column(Text, nullable=True)  # URL of group/org image (if any)
    is_organization = Column(Boolean, default=False, nullable=False)

    # Relationships:
    extras = relationship(
        "CatalogGroupExtra", back_populates="group", cascade="all, delete-orphan"
    )
    memberships = relationship(
        "CatalogMember", back_populates="group", cascade="all, delete-orphan"
    )
    # If this group is an organization, it can own many datasets (one-to-many via dataset.owner_org)
    datasets = relationship(
        "CatalogDataset",
        back_populates="organization",
        foreign_keys="CatalogDataset.owner_org",
    )


class CatalogGroupExtra(Base):
    """Additional metadata key-value pairs for a group/organization (CKAN group_extra)."""

    __tablename__ = "catalog_group_extra"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False
    )
    group_id = Column(
        UUID(as_uuid=True),
        ForeignKey("catalog_group.id", ondelete="CASCADE"),
        nullable=False,
    )
    key = Column(String(100), nullable=False)  # extra field name
    value = Column(Text, nullable=True)  # extra field value
    state = Column(String(10), default="active", nullable=False)

    # Relationship:
    group = relationship("CatalogGroup", back_populates="extras")


class CatalogMember(Base):
    """Linking table for membership of objects (users or datasets) in groups/organizations."""

    __tablename__ = "catalog_member"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False
    )
    group_id = Column(
        UUID(as_uuid=True),
        ForeignKey("catalog_group.id", ondelete="CASCADE"),
        nullable=False,
    )
    table_id = Column(UUID(as_uuid=True), nullable=False)
    table_name = Column(
        String(50), nullable=False
    )  # 'user' or 'package' (dataset) indicating the type of member
    capacity = Column(
        String(20), nullable=False
    )  # role or capacity (e.g. 'admin', 'editor', 'member')
    state = Column(String(10), default="active", nullable=False)

    # Relationship:
    group = relationship("CatalogGroup", back_populates="memberships")


class CatalogLicense(Base):
    """Represents an available license for datasets."""

    __tablename__ = "catalog_license"

    # Using short text codes (e.g., 'cc-by-4.0') as primary key for licenses
    id = Column(String(50), primary_key=True, nullable=False)
    title = Column(String(200), nullable=False)  # full name of the license
    is_okd_compliant = Column(Boolean, default=False, nullable=False)
    is_generic = Column(Boolean, default=False, nullable=False)
    url = Column(Text, nullable=True)  # link to license text
    home_url = Column(Text, nullable=True)  # optional homepage URL
    status = Column(
        String(10), default="active", nullable=False
    )  # e.g. 'active' or 'deleted'

    # Relationship: back-reference to datasets that use this license
    datasets = relationship("CatalogDataset", back_populates="license")


class CatalogDataset(Base):
    """Represents a CKAN dataset (package) and its core metadata."""

    __tablename__ = "catalog_dataset"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False
    )
    name = Column(
        String(100), unique=True, nullable=False
    )  # unique dataset name (for URLs)
    title = Column(String(200), nullable=True)  # human-readable title
    version = Column(String(100), nullable=True)  # version identifier of the dataset
    url = Column(Text, nullable=True)  # optional URL (e.g., homepage of dataset)
    notes = Column(Text, nullable=True)  # description/notes for the dataset
    license_id = Column(String(50), ForeignKey("catalog_license.id"), nullable=True)
    author = Column(String(100), nullable=True)
    author_email = Column(String(100), nullable=True)
    maintainer = Column(String(100), nullable=True)
    maintainer_email = Column(String(100), nullable=True)
    state = Column(
        String(10), default="active", nullable=False
    )  # 'active' or 'deleted'
    type = Column(
        String(50), default="dataset", nullable=False
    )  # dataset type (for extensibility)
    private = Column(
        Boolean, default=False, nullable=False
    )  # if True, dataset is private
    owner_org = Column(
        UUID(as_uuid=True), ForeignKey("catalog_group.id"), nullable=True
    )
    metadata_created = Column(
        DateTime, nullable=False
    )  # timestamp when dataset was created
    metadata_modified = Column(
        DateTime, nullable=False
    )  # timestamp when dataset was last modified

    # Relationships:
    organization = relationship(
        "CatalogGroup", back_populates="datasets", foreign_keys=[owner_org]
    )
    license = relationship("CatalogLicense", back_populates="datasets")
    resources = relationship(
        "CatalogResource", back_populates="dataset", cascade="all, delete-orphan"
    )
    extras = relationship(
        "CatalogDatasetExtra", back_populates="dataset", cascade="all, delete-orphan"
    )
    tag_associations = relationship(
        "CatalogDatasetTag", back_populates="dataset", cascade="all, delete-orphan"
    )
    relationships_as_subject = relationship(
        "CatalogDatasetRelationship",
        back_populates="subject_dataset",
        foreign_keys="CatalogDatasetRelationship.subject_package_id",
        cascade="all, delete-orphan",
    )
    relationships_as_object = relationship(
        "CatalogDatasetRelationship",
        back_populates="object_dataset",
        foreign_keys="CatalogDatasetRelationship.object_package_id",
        cascade="all, delete-orphan",
    )
    tables = relationship(
        "CatalogTable", back_populates="dataset", cascade="all, delete-orphan"
    )

class CatalogDatasetExtra(Base):
    """Additional metadata key-value pairs for a dataset (CKAN package_extra)."""

    __tablename__ = "catalog_dataset_extra"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False
    )
    dataset_id = Column(
        UUID(as_uuid=True),
        ForeignKey("catalog_dataset.id", ondelete="CASCADE"),
        nullable=False,
    )
    key = Column(String(100), nullable=False)  # extra field name
    value = Column(Text, nullable=True)  # extra field value
    state = Column(String(10), default="active", nullable=False)

    # Relationship:
    dataset = relationship("CatalogDataset", back_populates="extras")


class CatalogResource(Base):
    """Represents a resource (data file or API) that is part of a dataset."""

    __tablename__ = "catalog_resource"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False
    )
    package_id = Column(
        UUID(as_uuid=True),
        ForeignKey("catalog_dataset.id", ondelete="CASCADE"),
        nullable=False,
    )  # the parent dataset ID
    url = Column(Text, nullable=False)  # URL or link to the resource (required)
    format = Column(
        String(50), nullable=True
    )  # format of the resource (e.g. "CSV", "API")
    description = Column(Text, nullable=True)  # brief description of the resource
    name = Column(
        String(100), nullable=True
    )  # name of the resource (optional, for UI/URL)
    resource_type = Column(
        String(20), nullable=True
    )  # e.g. 'file', 'file.upload', 'api', etc.
    mimetype = Column(String(100), nullable=True)  # MIME type of the resource
    mimetype_inner = Column(
        String(100), nullable=True
    )  # MIME type of inner content (if compressed)
    size = Column(BigInteger, nullable=True)  # size of the resource in bytes
    hash = Column(String(100), nullable=True)  # hash (md5/sha1) of the resource content
    last_modified = Column(
        DateTime, nullable=True
    )  # last modified datetime of the resource
    cache_url = Column(Text, nullable=True)  # URL of cached copy (if any)
    cache_last_updated = Column(DateTime, nullable=True)
    webstore_url = Column(Text, nullable=True)  # URL of webstore (if any)
    webstore_last_updated = Column(DateTime, nullable=True)
    created = Column(DateTime, nullable=False)  # timestamp when resource was created
    url_type = Column(
        String(20), nullable=True
    )  # type of URL (e.g., 'upload' for uploaded file)
    state = Column(String(10), default="active", nullable=False)
    extras = Column(JSONB, nullable=True)  # additional resource info (JSON field)
    position = Column(
        Integer, nullable=True
    )  # ordering position within dataset's resources

    # Relationship:
    dataset = relationship("CatalogDataset", back_populates="resources")


class CatalogDatasetRelationship(Base):
    """Represents a relationship between two datasets (CKAN package_relationship)."""

    __tablename__ = "catalog_dataset_relationship"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False
    )
    subject_package_id = Column(
        UUID(as_uuid=True),
        ForeignKey("catalog_dataset.id", ondelete="CASCADE"),
        nullable=False,
    )
    object_package_id = Column(
        UUID(as_uuid=True),
        ForeignKey("catalog_dataset.id", ondelete="CASCADE"),
        nullable=False,
    )
    type = Column(
        String(50), nullable=False
    )  # relationship type (e.g. 'depends_on', 'derived_from')
    comment = Column(
        Text, nullable=True
    )  # optional comment describing the relationship
    state = Column(String(10), default="active", nullable=False)

    # Relationships:
    subject_dataset = relationship(
        "CatalogDataset",
        back_populates="relationships_as_subject",
        foreign_keys=[subject_package_id],
    )
    object_dataset = relationship(
        "CatalogDataset",
        back_populates="relationships_as_object",
        foreign_keys=[object_package_id],
    )


class CatalogVocabulary(Base):
    """Represents a tag vocabulary (collection of tags)."""

    __tablename__ = "catalog_vocabulary"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False
    )
    name = Column(
        String(100), unique=True, nullable=False
    )  # name of the vocabulary (e.g., "Genre")

    # Relationship: a vocabulary can have many tags
    tags = relationship(
        "CatalogTag", back_populates="vocabulary", cascade="all, delete-orphan"
    )


class CatalogTag(Base):
    """Represents a tag for classifying datasets (may belong to a vocabulary or be free-form)."""

    __tablename__ = "catalog_tag"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False
    )
    name = Column(String(100), nullable=False)  # tag name
    vocabulary_id = Column(
        UUID(as_uuid=True), ForeignKey("catalog_vocabulary.id"), nullable=True
    )

    # Relationships:
    vocabulary = relationship("CatalogVocabulary", back_populates="tags")
    dataset_associations = relationship(
        "CatalogDatasetTag", back_populates="tag", cascade="all, delete-orphan"
    )


class CatalogDatasetTag(Base):
    """Association table linking datasets and tags (CKAN package_tag)."""

    __tablename__ = "catalog_dataset_tag"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False
    )
    dataset_id = Column(
        UUID(as_uuid=True),
        ForeignKey("catalog_dataset.id", ondelete="CASCADE"),
        nullable=False,
    )
    tag_id = Column(
        UUID(as_uuid=True),
        ForeignKey("catalog_tag.id", ondelete="CASCADE"),
        nullable=False,
    )
    state = Column(
        String(10), default="active", nullable=False
    )  # state of the association

    # Relationships:
    dataset = relationship("CatalogDataset", back_populates="tag_associations")
    tag = relationship("CatalogTag", back_populates="dataset_associations")

class CatalogTable(Base):
    """Represents a table in a dataset."""

    __tablename__ = "catalog_table"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False
    )
    dataset_id = Column(
        UUID(as_uuid=True),
        ForeignKey("catalog_dataset.id", ondelete="CASCADE"),
        nullable=False,
    )
    name = Column(String(100), nullable=False)  # name of the table
    description = Column(Text, nullable=True)  # description of the table
    created = Column(DateTime, nullable=False)  # timestamp when table was created
    state = Column(String(10), default="active", nullable=False)

    # Relationships:
    dataset = relationship("CatalogDataset", back_populates="tables")
    columns = relationship(
        "CatalogColumn", back_populates="table", cascade="all, delete-orphan"
    )

class CatalogColumn(Base):
    """Represents a column in a table."""

    __tablename__ = "catalog_column"

    id = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, nullable=False
    )
    table_id = Column(
        UUID(as_uuid=True),
        ForeignKey("catalog_table.id", ondelete="CASCADE"),
        nullable=False,
    )
    name = Column(String(100), nullable=False)  # name of the column
    data_type = Column(String(50), nullable=False)  # data type of the column
    description = Column(Text, nullable=True)  # description of the column
    created = Column(DateTime, nullable=False)  # timestamp when column was created
    state = Column(String(10), default="active", nullable=False)
    precision = Column(Integer, nullable=True)  # precision for numeric types
    scale = Column(Integer, nullable=True)  # scale for numeric types
    is_primary_key = Column(Boolean, default=False, nullable=False)  # is it a primary key?
    is_nullable = Column(Boolean, default=True, nullable=False)  # is it nullable?
    is_unique = Column(Boolean, default=False, nullable=False)  # is it unique?

    # Relationships:
    table = relationship("CatalogTable", back_populates="columns")