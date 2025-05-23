from typing import Any, Literal, Optional, Self
from pydantic import BaseModel

# Define type aliases using Literal for better type safety and readability
DataComparison = Literal[
    "=",
    "<",
    ">",
    "<=",
    ">=",
    "!=",
    "IN",
    "NOT IN",
    "LIKE",
    "NOT LIKE",
    "BETWEEN",
    "IS NULL",
    "IS NOT NULL",
]
DataOperation = Literal["AND", "OR"]
DataFilterType = Literal["Term", "Expression"]
DataSortDirecion = Literal["ASC", "DESC"]


class DataFilter(BaseModel):
    """Abstract base class for data filters."""

    filter_type: DataFilterType

    def __init__(self, filter_type: DataFilterType):
        # Initialize the filter type
        self.filter_type = filter_type


class DataFilterTerm(DataFilter):
    """Represents a single filter condition."""

    field: str
    comparison: DataComparison
    value: Any

    def __init__(
        self, field: str, comparison: DataComparison, value: Optional[Any] = None
    ):
        super().__init__("Term")
        # Assign field, comparison operator, and value
        self.field = field
        self.comparison = comparison
        self.value = value


class DataFilterNode(BaseModel):
    """Node in a linked list representing a chain of filters."""

    filter: DataFilter
    operation: DataOperation
    next: Optional["DataFilterNode"]

    def __init__(
        self,
        filter: DataFilter,
        operation: DataOperation = "AND",
        next: Optional["DataFilterNode"] = None,
    ):
        # Initialize the filter, operation, and next node
        self.filter = filter
        self.operation = operation
        self.next = next


class DataFilterExpression(DataFilter):
    """Represents a complex filter expression composed of multiple filters."""

    _first: DataFilterNode
    _last: DataFilterNode

    def __init__(self, filter: DataFilter):
        super().__init__("Expression")
        # Initialize the first and last nodes of the filter chain
        self._first = DataFilterNode(filter)
        self._last = self._first

    def append(self, operation: DataOperation, filter: DataFilter) -> Self:
        """Append a new filter to the expression."""
        # Update the last node's operation and link a new node
        self._last.operation = operation
        self._last.next = DataFilterNode(filter, operation)
        self._last = self._last.next
        return self


class DataSortTerm(BaseModel):
    """Class representing a sort term."""

    field: str
    direction: DataSortDirecion

    def __init__(self, field: str, direction: DataSortDirecion = "ASC"):
        # Initialize the field and sort direction
        self.field = field
        self.direction = direction


class DataQuery:
    """Class representing a data query."""

    filter: Optional[DataFilter]
    sort: Optional[DataSortTerm]
    offset: int
    limit: int

    def __init__(
        self,
        filter: Optional[DataFilter] = None,
        sort: Optional[DataSortTerm] = None,
        offset: int = 0,
        limit: int = -1,
    ):
        # Initialize query parameters with default values
        self.filter = filter
        self.sort = sort
        self.offset = offset
        self.limit = limit
