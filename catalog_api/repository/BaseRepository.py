import os
from typing import Type, TypeVar, Generic, Optional, List, Callable, Any
from sqlalchemy import create_engine, asc, desc
from sqlalchemy.orm import sessionmaker, Session
from catalog_api.models import Base
from contextlib import contextmanager


def create_sqlalchemy():
    ALCHEMY_URL = os.getenv("ALCHEMY_URL")
    assert ALCHEMY_URL is not None, "Database URL cannot be None"
    return create_engine(ALCHEMY_URL)


SQLENGINE = create_sqlalchemy()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=SQLENGINE)
db_session: Session | None = None


def get_session() -> Session:
    """
    Dependency to get a database session.
    """
    global db_session
    if db_session is None:
        db_session = SessionLocal()
    return db_session


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


# Define a generic type variable for the model class
T = TypeVar("T")


class BaseRepository(Generic[T]):
    """
    Base repository class for SQLAlchemy models.

    Provides common CRUD operations and a custom query method.
    """

    _session: Session  # SQLAlchemy session instance
    _model: Type[T]  # SQLAlchemy model class

    def __init__(self, model: Type[T]):
        """
        Initialize the repository with a specific model class.

        Args:
            model (Type[T]): The SQLAlchemy model class to be used by the repository.
        """
        self._session = get_session()
        self._model = model

    def get(self, obj_id: Any) -> Optional[T]:
        """
        Retrieve an object by its primary key.

        Args:
            obj_id (Any): The primary key of the object to retrieve.

        Returns:
            Optional[T]: The retrieved object or None if not found.
        """
        return self._session.get(self._model, obj_id)

    def list(self, skip: int = 0, limit: int = 100) -> List[T]:
        """
        Retrieve a list of objects with optional pagination.

        Args:
            skip (int): Number of records to skip. Defaults to 0.
            limit (int): Maximum number of records to return. Defaults to 100.

        Returns:
            List[T]: A list of retrieved objects.
        """
        return self._session.query(self._model).offset(skip).limit(limit).all()

    def create(self, data: dict) -> T:
        """
        Create a new object in the database.

        Args:
            data (dict): A dictionary of data to initialize the object.

        Returns:
            T: The created object.
        """
        with session_scope() as session:
            obj = self._model(**data)
            session.add(obj)
            session.commit()
            session.refresh(obj)
            return obj

    def update(self, obj_id: Any, updates: dict) -> Optional[T]:
        """
        Update an existing object in the database.

        Args:
            obj_id (Any): The primary key of the object to update.
            updates (dict): A dictionary of fields to update.

        Returns:
            Optional[T]: The updated object or None if not found.
        """
        with session_scope() as session:
            obj = session.get(self._model, obj_id)
            if not obj:
                return None
            for key, value in updates.items():
                setattr(obj, key, value)
            session.commit()
            session.refresh(obj)
            return obj

    def delete(self, obj_id: Any) -> bool:
        """
        Delete an object from the database.

        Args:
            obj_id (Any): The primary key of the object to delete.

        Returns:
            bool: True if the object was deleted, False if not found.
        """
        obj = self.get(obj_id)
        if not obj:
            return False
        self._session.delete(obj)
        self._session.commit()
        return True

    def query(
        self,
        filter_fn: Optional[Callable[[Any], Any]] = None,
        sort_by: Optional[str] = None,
        sort_desc: bool = False,
        skip: int = 0,
        limit: int = 100,
    ) -> List[T]:
        """
        Perform a custom query with filtering, sorting, offset, and limit.

        Args:
            filter_fn (Optional[Callable[[Any], Any]]): A function to apply filters to the query.
            sort_by (Optional[str]): The field to sort by.
            sort_desc (bool): Whether to sort in descending order. Defaults to False.
            skip (int): Number of records to skip. Defaults to 0.
            limit (int): Maximum number of records to return. Defaults to 100.

        Returns:
            List[T]: A list of retrieved objects.
        """
        query = self._session.query(self._model)

        if filter_fn:
            query = filter_fn(query)

        if sort_by:
            sort_column = getattr(self._model, sort_by, None)
            if sort_column is not None:
                query = query.order_by(
                    desc(sort_column) if sort_desc else asc(sort_column)
                )

        return query.offset(skip).limit(limit).all()


# Example usage:
if __name__ == "__main__":
    Base.metadata.create_all(bind=SQLENGINE)  # Create tables if they don't exist
    print("SQL ALCHEMY tables created")
