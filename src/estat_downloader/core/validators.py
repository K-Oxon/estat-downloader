"""
CSV validation and loading module for e-Stat Downloader
"""

import re
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Optional, Union
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


class BaseEntry(BaseModel, ABC):
    """Base class for all entry types"""

    format: FileFormat = Field(description="ファイル形式 DB or CSV or XLS")
    stats_data_id: str = Field(description="統計表ID")
    title: Optional[str] = Field(default=None, description="統計表のタイトル")
    description: Optional[str] = None

    @field_validator("stats_data_id")
    def validate_stats_data_id(cls, v: str) -> str:
        """Validate stats_data_id format"""
        if not v:
            raise ValueError("stats_data_id cannot be empty")

        v = v.strip()
        if not re.match(r"^\d{10}$", v):
            raise ValueError("stats_data_id must be a 10-digit number")

        return v

    @abstractmethod
    def get_filename(self) -> str:
        """Generate filename for the entry"""
        pass


class URLEntry(BaseEntry):
    """Entry for downloading files directly from e-Stat"""

    format: FileFormat = Field(description="File format (CSV or EXCEL)")
    url: HttpUrl = Field(description="URL of the statistical data")

    @field_validator("format")
    def validate_format(cls, v: FileFormat) -> FileFormat:
        """Validate that format is either CSV or EXCEL"""
        if v == FileFormat.DB:
            raise ValueError("URLEntry cannot have DB format")
        return v

    @field_validator("url")
    def validate_estat_url(cls, v: HttpUrl) -> HttpUrl:
        """Validate that the URL is from e-Stat domain"""
        parsed = urlparse(str(v))
        if not parsed.netloc.endswith("e-stat.go.jp"):
            raise ValueError("URL must be from e-stat.go.jp domain")
        return v

    def get_filename(self) -> str:
        """Generate filename based on stats_data_id and format"""
        extension = ".csv" if self.format == FileFormat.CSV else ".xlsx"
        return f"{self.stats_data_id}{extension}"


class DBEntry(BaseEntry):
    """Entry for downloading metadata from e-Stat API"""

    format: FileFormat = Field(
        default=FileFormat.DB, description="Always DB for metadata entries"
    )
    url: str = Field(
        description="10-digit identifier for the statistical data",
        min_length=10,
        max_length=10,
        pattern=r"^\d{10}$",
    )

    def get_filename(self) -> str:
        """Generate filename for metadata"""
        return f"{self.stats_data_id}.meta.json"


class ValidationResult(BaseModel):
    """Result of CSV validation"""

    url_entries: list[URLEntry]
    db_entries: list[DBEntry]
    invalid_rows: list[tuple[int, str]]  # (row_index, error_message)


def load_and_validate_csv(file_path: Path) -> ValidationResult:
    """
    Load and validate CSV file containing statistical data entries.

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

        url_entries: list[URLEntry] = []
        db_entries: list[DBEntry] = []
        invalid_rows: list[tuple[int, str]] = []

        for idx, row in df.iterrows():
            try:
                data = row.to_dict()
                if data["format"] == FileFormat.DB:
                    # DBエントリーとして検証
                    entry = DBEntry.model_validate(data)
                    db_entries.append(entry)
                else:
                    # URLエントリーとして検証
                    if "url" not in data:
                        raise ValueError("URL column is required for non-DB entries")
                    entry = URLEntry.model_validate(data)
                    url_entries.append(entry)

            except ValidationError as e:
                error_msgs = "; ".join(str(err["msg"]) for err in e.errors())
                invalid_rows.append((idx + 1, error_msgs))

        return ValidationResult(
            url_entries=url_entries, db_entries=db_entries, invalid_rows=invalid_rows
        )

    except Exception as e:
        raise ValueError(f"Failed to load CSV file: {e}") from e


# Type alias for convenience
Entry = Union[URLEntry, DBEntry]


def display_validation_result(result: ValidationResult) -> None:
    """Display validation results using rich"""
    console.print(
        f"Valid entries: {len(result.url_entries + result.db_entries)}", style="green"
    )

    if result.invalid_rows:
        console.print("\nInvalid entries:", style="red")
        for row_num, error in result.invalid_rows:
            console.print(f"Row {row_num}: {error}", style="red")
