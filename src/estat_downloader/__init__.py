"""
e-Stat Downloader
A CLI tool for downloading statistical data from e-Stat
"""

__version__ = "0.1.4"

from estat_downloader.api import (
    DownloadResult,
    URLEntry,
    ValidationResult,
    download_stats,
    validate_url_list,
)

__all__ = [
    "download_stats",
    "validate_url_list",
    "URLEntry",
    "ValidationResult",
    "DownloadResult",
]
