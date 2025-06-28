# Python Development Standards - High Cohesion & Low Coupling

## Core Principles

### High Cohesion
- **Single Responsibility**: Each module, class, and function should have ONE well-defined purpose
- **Related Elements Together**: Group closely related functionality within the same module/class
- **Clear Purpose**: Every component should have a clear, easily describable responsibility
- **Minimal Dependencies**: Each module should depend on as few external components as possible

### Low Coupling
- **Interface-Based Design**: Depend on abstractions (interfaces/protocols) rather than concrete implementations
- **Dependency Injection**: Pass dependencies as parameters rather than creating them internally
- **Event-Driven Architecture**: Use events/messages for communication between loosely related components
- **Configuration External**: Keep configuration separate from business logic

## Architecture Standards

### Directory Structure
```
catalog_api/
├── models/          # Data models and schemas (high cohesion)
├── repository/      # Data access layer (abstracted interfaces)
├── services/        # Business logic (single responsibility)
├── infrastructure/  # External integrations (loosely coupled)
├── routes/          # HTTP endpoints (thin controllers)
├── helpers/         # Utility functions (pure functions)
└── utils/           # Cross-cutting concerns
```

### Dependency Management
- Use dependency injection containers or factory patterns
- Avoid circular imports at all costs
- Keep imports at the top of files, grouped by: standard library, third-party, local
- Use `from typing import Protocol` for defining interfaces

### Code Organization

#### Models & Schemas
```python
# High cohesion: All related data structures together
# Low coupling: Use Pydantic for validation, avoid business logic
from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class DatasetModel(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    created_at: datetime
    
    class Config:
        from_attributes = True
```

#### Repository Pattern
```python
# High cohesion: All data access for one entity
# Low coupling: Abstract interface, implementation can be swapped
from abc import ABC, abstractmethod
from typing import List, Optional

class DatasetRepositoryProtocol(Protocol):
    async def get_by_id(self, dataset_id: str) -> Optional[DatasetModel]:
        ...
    
    async def list_all(self) -> List[DatasetModel]:
        ...
```

#### Service Layer
```python
# High cohesion: All business logic for one domain
# Low coupling: Depends on abstractions, not concrete classes
class DatasetService:
    def __init__(self, repository: DatasetRepositoryProtocol):
        self._repository = repository
    
    async def get_dataset(self, dataset_id: str) -> DatasetModel:
        dataset = await self._repository.get_by_id(dataset_id)
        if not dataset:
            raise DatasetNotFoundError(f"Dataset {dataset_id} not found")
        return dataset
```

## Code Quality Standards

### Type Hints
- Use type hints for ALL function parameters and return values
- Use `typing.Protocol` for interface definitions
- Use `typing.Union` or `|` for Python 3.10+ union types
- Use `Optional[T]` or `T | None` for nullable types

### Error Handling
```python
# Custom exceptions for domain-specific errors
class CatalogError(Exception):
    """Base exception for catalog domain"""
    pass

class DatasetNotFoundError(CatalogError):
    """Raised when a dataset cannot be found"""
    pass

# Use specific exception types, not generic Exception
async def get_dataset(dataset_id: str) -> DatasetModel:
    try:
        return await repository.get_by_id(dataset_id)
    except DatabaseConnectionError as e:
        logger.error(f"Database error retrieving dataset {dataset_id}: {e}")
        raise CatalogServiceUnavailableError("Service temporarily unavailable")
```

### Logging
```python
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Structured logging with context
def log_operation(operation: str, **context: Any) -> None:
    logger.info(f"Operation: {operation}", extra=context)
```

### Testing Standards
```python
# High cohesion: Test one component thoroughly
# Low coupling: Use mocks for dependencies
import pytest
from unittest.mock import Mock, AsyncMock

@pytest.fixture
def mock_repository():
    return Mock(spec=DatasetRepositoryProtocol)

@pytest.mark.asyncio
async def test_get_dataset_success(mock_repository):
    # Arrange
    dataset = DatasetModel(id="123", name="Test Dataset")
    mock_repository.get_by_id.return_value = dataset
    service = DatasetService(mock_repository)
    
    # Act
    result = await service.get_dataset("123")
    
    # Assert
    assert result == dataset
    mock_repository.get_by_id.assert_called_once_with("123")
```

## FastAPI Best Practices

### Route Organization
```python
# High cohesion: Group related endpoints
# Low coupling: Thin controllers, delegate to services
from fastapi import APIRouter, Depends, HTTPException
from catalog_api.services import DatasetService

router = APIRouter(prefix="/datasets", tags=["datasets"])

@router.get("/{dataset_id}")
async def get_dataset(
    dataset_id: str,
    dataset_service: DatasetService = Depends(get_dataset_service)
) -> DatasetModel:
    try:
        return await dataset_service.get_dataset(dataset_id)
    except DatasetNotFoundError:
        raise HTTPException(status_code=404, detail="Dataset not found")
```

### Dependency Injection
```python
# Container for managing dependencies
from functools import lru_cache
from catalog_api.infrastructure import DatabaseConnection

@lru_cache()
def get_database() -> DatabaseConnection:
    return DatabaseConnection(connection_string=settings.database_url)

def get_dataset_repository(
    db: DatabaseConnection = Depends(get_database)
) -> DatasetRepository:
    return SqlAlchemyDatasetRepository(db)

def get_dataset_service(
    repository: DatasetRepositoryProtocol = Depends(get_dataset_repository)
) -> DatasetService:
    return DatasetService(repository)
```

## Data Access Patterns

### Repository Implementation
```python
# High cohesion: All database operations for one entity
# Low coupling: Implements protocol, can be easily replaced
from sqlalchemy.orm import Session
from sqlalchemy import select

class SqlAlchemyDatasetRepository:
    def __init__(self, session: Session):
        self._session = session
    
    async def get_by_id(self, dataset_id: str) -> Optional[DatasetModel]:
        stmt = select(DatasetEntity).where(DatasetEntity.id == dataset_id)
        result = await self._session.execute(stmt)
        entity = result.scalar_one_or_none()
        return DatasetModel.from_orm(entity) if entity else None
```

## Configuration Management

### Environment-Based Config
```python
from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    # Database
    database_url: str
    database_pool_size: int = 10
    
    # API
    api_title: str = "Catalog API"
    api_version: str = "1.0.0"
    
    # External Services
    s3_bucket_name: Optional[str] = None
    ckan_api_url: Optional[str] = None
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()
```

## Performance & Scalability

### Async/Await Best Practices
- Use `async`/`await` consistently throughout the stack
- Don't mix sync and async code unnecessarily
- Use connection pooling for database operations
- Implement proper timeout handling

### Caching Strategy
```python
from functools import lru_cache
from typing import Dict, Any
import asyncio

class CacheService:
    def __init__(self):
        self._cache: Dict[str, Any] = {}
        self._lock = asyncio.Lock()
    
    async def get_or_set(self, key: str, factory_func, ttl: int = 300):
        async with self._lock:
            if key in self._cache:
                return self._cache[key]
            
            value = await factory_func()
            self._cache[key] = value
            return value
```

## Code Review Checklist

### High Cohesion Checks
- [ ] Does each class/module have a single, clear responsibility?
- [ ] Are related functions grouped together?
- [ ] Are data and the operations on that data co-located?
- [ ] Can you easily describe what each component does in one sentence?

### Low Coupling Checks
- [ ] Does the component depend on abstractions rather than concrete classes?
- [ ] Can dependencies be easily mocked for testing?
- [ ] Are there any circular dependencies?
- [ ] Can components be used independently in different contexts?

### Code Quality Checks
- [ ] Are all functions and methods type-hinted?
- [ ] Are errors handled appropriately with specific exception types?
- [ ] Is logging structured and meaningful?
- [ ] Are tests comprehensive and independent?
- [ ] Is configuration externalized from business logic?

## Refactoring Guidelines

### When to Extract a Service
- Function/method is getting too long (>20 lines)
- Multiple responsibilities are being handled
- Complex business logic is mixed with infrastructure concerns

### When to Create an Interface
- Multiple implementations are likely
- Testing requires mocking
- Component will be used by multiple clients
- Clear boundary between layers exists

### When to Split a Module
- File is getting too large (>500 lines)
- Multiple unrelated concepts are present
- Import dependencies are getting complex
- Team members frequently conflict on the same file

Remember: **High cohesion and low coupling are not just patterns—they're fundamental principles that make code maintainable, testable, and scalable.**