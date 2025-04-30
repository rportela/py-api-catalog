from typing import Optional, List
import pandas as pd
from pydantic import BaseModel, Field


class CatalogColumnHelper(BaseModel):
    """
    Helper class for CatalogColumn
    """

    # ────────── core ──────────
    name: str
    type: str  # e.g. varchar, int, numeric
    length: Optional[int] = None  # e.g. varchar(255)  -> length = 255
    precision: Optional[int] = None  # e.g. numeric(12,2) -> precision = 12
    scale: Optional[int] = None  # e.g. numeric(12,2) -> scale = 2
    nullable: bool
    description: Optional[str] = None  # e.g. "Nome do cliente"

    def adjust_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adjust the dataframe to match the column type
        """
        if self.type == "varchar" or self.type == "char":
            df[self.name] = df[self.name].astype(str)
        elif (
            self.type == "int"
            or self.type == "integer"
            or self.type == "bigint"
            or self.type == "tinyint"
        ):
            df[self.name] = df[self.name].astype(int)
        elif self.type == "numeric":
            if self.scale:
                df[self.name] = df[self.name].astype(float).round(self.scale)
        elif self.type == "date":
            df[self.name] = pd.to_datetime(df[self.name], errors="coerce").dt.date
        elif self.type == "float":
            df[self.name] = df[self.name].astype(float)
        elif self.type == "boolean":
            df[self.name] = df[self.name].astype(bool)
        elif self.type == "timestamp":
            df[self.name] = pd.to_datetime(df[self.name], errors="coerce")
        return df


class CatalogTableHelper(BaseModel):
    """
    Helper class for CatalogTable
    """

    name: str
    columns: List[CatalogColumnHelper]
    description: str | None = None
    is_view: bool = False

    def get_column(self, column_name: str) -> Optional[CatalogColumnHelper]:
        """
        Get a column by name
        """
        for column in self.columns:
            if column.name == column_name:
                return column
        return None


class SchemaHelper(BaseModel):
    """
    Helper class for Schema
    """

    tables: List[CatalogTableHelper] = Field(default_factory=list)
    description: Optional[str] = None
    is_public: bool = False

    def to_dataframe(self):
        rows = []
        for table in self.tables:
            for column in table.columns:
                rows.append(
                    {
                        "table_name": table.name,
                        "column_name": column.name,
                        "type": column.type,
                        "length": column.length,
                        "precision": column.precision,
                        "scale": column.scale,
                        "nullable": column.nullable,
                        "description": column.description,
                    }
                )
        return pd.DataFrame(rows)

    def get_table(self, file_name: str) -> Optional[CatalogTableHelper]:
        """
        Get a table by name
        """
        table_name = file_name.split(".")[0]
        if table_name.endswith("_view"):
            table_name = table_name[:-5]
        name_parts = table_name.split("_")
        if name_parts[-1].isnumeric():
            name_parts = name_parts[:-1]
        file_name = "_".join(name_parts)
        for table in self.tables:
            if table.name == file_name:
                return table
        return None
