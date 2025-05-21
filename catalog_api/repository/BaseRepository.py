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


T = TypeVar("T")


class BaseRepository(Generic[T]):

    _session: Session
    _model: Type[T]

    def __init__(self, model: Type[T]):
        self._session = get_session()
        self._model = model

    def get(self, obj_id) -> Optional[T]:
        return self._session.get(self._model, obj_id)

    def list(self, skip: int = 0, limit: int = 100) -> List[T]:
        return self._session.query(self._model).offset(skip).limit(limit).all()

    def create(self, data: dict) -> T:
        with session_scope() as session:
            obj = self._model(**data)
            session.add(obj)
            session.commit()
            session.refresh(obj)
            return obj

    def update(self, obj_id, updates: dict) -> Optional[T]:
        with session_scope() as session:
            obj = session.get(self._model, obj_id)
            if not obj:
                return None
            for key, value in updates.items():
                setattr(obj, key, value)
            session.commit()
            session.refresh(obj)
            return obj

    def delete(self, obj_id) -> bool:
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
        Custom query method with filtering, sorting, offset and limit.

        Example usage:
            repo.query(
                filter_fn=lambda session: session.query(Model).filter(Model.name == 'something'),
                sort_by="created",
                sort_desc=True,
                skip=10,
                limit=20
            )
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
