import os
from typing import Type, TypeVar, Generic, Optional, List, Callable, Any
from sqlalchemy import create_engine, asc, desc
from sqlalchemy.orm import sessionmaker, Session
from portela_dev.repo.CatalogModels import Base  # assumes your models are in models.py


def create_sqlalchemy():
    DATABASE_URL = os.getenv("ALCHEMY_URL")
    assert DATABASE_URL is not None, "Database URL cannot be None"
    return create_engine(DATABASE_URL)


SQLENGINE = create_sqlalchemy()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=SQLENGINE)

T = TypeVar("T")


class BaseRepository(Generic[T]):
    def __init__(self, db: Session, model: Type[T]):
        self.db = db
        self.model = model

    def get(self, obj_id) -> Optional[T]:
        return self.db.get(self.model, obj_id)

    def list(self, skip: int = 0, limit: int = 100) -> List[T]:
        return self.db.query(self.model).offset(skip).limit(limit).all()

    def create(self, data: dict) -> T:
        obj = self.model(**data)
        self.db.add(obj)
        self.db.commit()
        self.db.refresh(obj)
        return obj

    def update(self, obj_id, updates: dict) -> Optional[T]:
        obj = self.get(obj_id)
        if not obj:
            return None
        for key, value in updates.items():
            setattr(obj, key, value)
        self.db.commit()
        self.db.refresh(obj)
        return obj

    def delete(self, obj_id) -> bool:
        obj = self.get(obj_id)
        if not obj:
            return False
        self.db.delete(obj)
        self.db.commit()
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
        query = self.db.query(self.model)

        if filter_fn:
            query = filter_fn(query)

        if sort_by:
            sort_column = getattr(self.model, sort_by, None)
            if sort_column is not None:
                query = query.order_by(
                    desc(sort_column) if sort_desc else asc(sort_column)
                )

        return query.offset(skip).limit(limit).all()


# Example usage:
if __name__ == "__main__":
    Base.metadata.create_all(bind=SQLENGINE)  # Create tables if they don't exist
    print("SQL ALCHEMY tables created")
