"""
CSV validation and loading module for e-Stat Downloader
"""

import re
from enum import Enum
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import pandas as pd
from pydantic import (
    BaseModel,
    Field,
    HttpUrl,
    ValidationError,
    field_validator,
)
from rich.console import Console

console = Console()


class FileFormat(str, Enum):
    CSV = "CSV"
    XLS = "XLS"
    DB = "DB"


class URLEntry(BaseModel):
    url: HttpUrl = Field(description="URL of the statistical data")
    format: FileFormat = Field(description="File format of the data")
    stats_data_id: str = Field(description="Unique identifier for the statistical data")
    title: Optional[str] = Field(
        default=None, description="Title of the statistical data"
    )
    description: Optional[str] = None

    @field_validator("url")
    def validate_estat_url(cls, v: HttpUrl) -> HttpUrl:
        """Validate that the URL is from e-Stat domain"""
        parsed = urlparse(str(v))
        if not parsed.netloc.endswith("e-stat.go.jp"):
            raise ValueError("URL must be from e-stat.go.jp domain")
        return v

    @field_validator("stats_data_id")
    def validate_stats_data_id(cls, v: str) -> str:
        """Validate stats_data_id format"""
        if not v:
            raise ValueError("stats_data_id cannot be empty")

        v = v.strip()
        if not re.match(r"^\d{12}$", v):
            raise ValueError("stats_data_id must be a 12-digit number")

        return v

    def get_filename(self) -> str:
        """Generate filename based on stats_data_id and format"""
        extension = {
            FileFormat.CSV: ".csv",
            FileFormat.XLS: ".xlsx",
            FileFormat.DB: ".json",
        }[self.format]
        return f"{self.stats_data_id}{extension}"


class ValidationResult(BaseModel):
    valid_entries: list[URLEntry]
    invalid_rows: list[tuple[int, str]]  # (row_index, error_message)


def load_and_validate_csv(file_path: Path) -> ValidationResult:
    """
    Load and validate CSV file containing URLs.

    Args:
        file_path: Path to the CSV file

    Returns:
        ValidationResult containing valid entries and validation errors
    """
    try:
        df = pd.read_csv(file_path, dtype=str)
        required_columns = {"url", "format", "stats_data_id"}
        if not required_columns.issubset(df.columns):
            missing = required_columns - set(df.columns)
            raise ValueError(f"Missing required columns: {missing}")

        valid_entries: list[URLEntry] = []
        invalid_rows: list[tuple[int, str]] = []

        for idx, row in df.iterrows():
            try:
                entry = URLEntry.model_validate(row.to_dict())
                valid_entries.append(entry)
            except ValidationError as e:
                error_msgs = "; ".join(str(err["msg"]) for err in e.errors())
                invalid_rows.append((idx + 1, error_msgs))

        return ValidationResult(valid_entries=valid_entries, invalid_rows=invalid_rows)

    except Exception as e:
        raise ValueError(f"Failed to load CSV file: {e}") from e


def display_validation_result(result: ValidationResult) -> None:
    """Display validation results using rich"""
    console.print(f"Valid entries: {len(result.valid_entries)}", style="green")

    if result.invalid_rows:
        console.print("\nInvalid entries:", style="red")
        for row_num, error in result.invalid_rows:
            console.print(f"Row {row_num}: {error}", style="red")
