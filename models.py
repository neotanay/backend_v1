"""
Request models for /filter_multiple_values.

Column names are resolved through `column_registry.REGISTRY` at validation
time, so an unknown column is rejected with a 422 before it ever reaches
athena_filter.
"""
import os
from typing import Any, List, Optional

from pydantic import BaseModel, Field, field_validator

from column_registry import REGISTRY

MAX_ROWS_DEFAULT = int(os.getenv("SV_MAX_ROWS_DEFAULT", "200"))
MAX_ROWS_CAP = int(os.getenv("SV_MAX_ROWS_CAP", "1000"))
if not 1 <= MAX_ROWS_DEFAULT <= MAX_ROWS_CAP:
    raise RuntimeError(
        f"SV_MAX_ROWS_DEFAULT ({MAX_ROWS_DEFAULT}) must be between 1 and "
        f"SV_MAX_ROWS_CAP ({MAX_ROWS_CAP})"
    )


class PreviousFilter(BaseModel):
    column_name: str
    values: List[str] = Field(..., min_length=1, max_length=MAX_ROWS_CAP)

    @field_validator("column_name")
    @classmethod
    def _known_column(cls, value: str) -> str:
        # Raises UnknownColumn -> surfaces as a 422 from FastAPI.
        return REGISTRY.resolve(value)


class FilterValuesRequest(BaseModel):
    table_name: Optional[str] = None

    current_column_name: str = Field(
        ..., description="Column whose distinct values we want, e.g. FAMILY_NAME"
    )
    previous_filters: List[PreviousFilter] = Field(
        default_factory=list,
        description=(
            "Optional. Columns already selected earlier, e.g. COUNTRY=[INDIA]. "
            "Omit it, send null, or send [] to get the unrestricted values of "
            "current_column_name."
        ),
    )

    @field_validator("previous_filters", mode="before")
    @classmethod
    def _optional_previous_filters(cls, value: Any) -> List[Any]:
        """Normalise the field so callers can leave it out entirely.

        Runs before per-item validation, so entries that carry no values are
        dropped here rather than failing PreviousFilter.values (min_length=1).
        An empty `values` list cannot become SQL anyway -- `IN ()` is a syntax
        error -- so "filter on nothing" has to mean "do not filter".
        """
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("previous_filters must be a list when provided")

        kept: List[Any] = []
        for item in value:
            if item is None:
                continue
            if isinstance(item, dict):
                column = item.get("column_name") or item.get("column")
                values = item.get("values")
                if not column or not values:
                    continue
                # Accept `column` as an alias, as the frontend contract documents.
                kept.append({"column_name": column, "values": values})
            else:
                if not getattr(item, "values", None):
                    continue
                kept.append(item)
        return kept

    q: Optional[str] = Field(
        default=None, description="Optional search term for high-cardinality columns"
    )
    limit: int = Field(MAX_ROWS_DEFAULT, ge=1, le=MAX_ROWS_CAP)
    offset: int = Field(default=0, ge=0)

    @field_validator("current_column_name")
    @classmethod
    def _known_column(cls, value: str) -> str:
        return REGISTRY.resolve(value)
