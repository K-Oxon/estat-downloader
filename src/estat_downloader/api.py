# src/estat_downloader/api.py
"""
Public API for e-Stat Downloader library
"""

from pathlib import Path
from typing import List, Optional, Union

from estat_downloader.core.downloader import DownloadManager, DownloadResult
from estat_downloader.core.validators import (
    URLEntry,
    ValidationResult,
    load_and_validate_csv,
)


async def download_stats(
    url_list: Union[Path, List[URLEntry]],
    output_dir: Optional[Path] = None,
    max_concurrent: int = 3,
) -> DownloadResult:
    """
    Download statistical data from e-Stat.

    Args:
        url_list: Either a Path to CSV file or a list of URLEntry objects
        output_dir: Directory to save downloaded files (default: ./tmp_dl)
        max_concurrent: Maximum number of concurrent downloads (default: 3)

    Returns:
        DownloadResult containing successful and failed downloads

    Examples:
        Using with CSV file:
        >>> import asyncio
        >>> from estat_downloader.api import download_stats
        >>> result = asyncio.run(download_stats("urls.csv"))

        Using with URLEntry objects:
        >>> from estat_downloader.api import download_stats, URLEntry
        >>> entries = [
        ...     URLEntry(
        ...         url="https://www.e-stat.go.jp/...",
        ...         format="CSV",
        ...         stats_data_id="000031234567",
        ...     )
        ... ]
        >>> result = asyncio.run(download_stats(entries))
    """
    # 出力ディレクトリのデフォルト値設定
    if output_dir is None:
        output_dir = Path("tmp_dl")

    # URLEntryのリストを取得
    entries: List[URLEntry]
    if isinstance(url_list, Path):
        validation_result = load_and_validate_csv(url_list)
        if not validation_result.valid_entries:
            raise ValueError("No valid entries found in CSV file")
        entries = validation_result.valid_entries
    else:
        entries = url_list

    # ダウンローダーの初期化と実行
    downloader = DownloadManager(
        output_dir=output_dir,
        max_concurrent=max_concurrent,
    )

    # CSVファイルから読み込んだ場合はそのファイル名を使用、
    # そうでない場合はデフォルトのディレクトリ名を使用
    subdir_name = url_list.name if isinstance(url_list, Path) else "estat_data"

    return await downloader.download_all(entries, subdir_name)


def validate_url_list(csv_path: Path) -> ValidationResult:
    """
    Validate a CSV file containing e-Stat URLs.

    Args:
        csv_path: Path to CSV file

    Returns:
        ValidationResult containing valid entries and validation errors

    Example:
        >>> from estat_downloader.api import validate_url_list
        >>> result = validate_url_list("urls.csv")
        >>> print(f"Valid entries: {len(result.valid_entries)}")
        >>> print(f"Invalid entries: {len(result.invalid_rows)}")
    """
    return load_and_validate_csv(csv_path)


# Re-export important types for library users
__all__ = [
    "download_stats",
    "validate_url_list",
    "URLEntry",
    "ValidationResult",
    "DownloadResult",
]
