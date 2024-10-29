"""
Metadata download functionality for e-Stat Downloader
"""

import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import httpx
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
)

from .validators import DBEntry, FileFormat

console = Console()


@dataclass
class MetadataError:
    """Metadata download error information"""

    stats_data_id: str
    status_code: Optional[int]
    error_message: str


@dataclass
class MetadataResult:
    """Metadata download operation result"""

    successful: list[Path]
    failed: list[MetadataError]


class MetadataDownloader:
    def __init__(
        self,
        output_dir: Path,
        max_concurrent: int = 5,
        timeout: float = 30.0,
    ):
        """
        Initialize metadata downloader.

        Args:
            output_dir: Base directory for downloads
            max_concurrent: Maximum number of concurrent downloads
            timeout: Timeout for each download in seconds
        """
        self.output_dir = output_dir
        self.max_concurrent = max_concurrent
        self.timeout = timeout
        self.successful: list[Path] = []
        self.failed: list[MetadataError] = []

        # 環境変数からAPIキーを取得
        self.api_key = os.environ.get("ESTAT_API_KEY")
        if not self.api_key:
            raise ValueError("ESTAT_API_KEY environment variable is not set")

    async def download_metadata(
        self,
        entry: DBEntry,
        subdir: Path,
        progress: Progress,
        task_id: TaskID,
    ) -> None:
        """
        Download metadata for a single entry.

        Args:
            entry: Validated DB entry
            subdir: Subdirectory for this download
            progress: Progress bar instance
            task_id: Task ID for progress tracking
        """
        # メタデータ用のファイル名を生成
        output_path = subdir / f"{entry.stats_data_id}.meta.json"

        try:
            # APIエンドポイントとパラメータの設定
            api_url = "https://api.e-stat.go.jp/rest/3.0/app/json/getMetaInfo"
            params = {
                "appId": self.api_key,
                "statsDataId": entry.stats_data_id,
            }

            async with httpx.AsyncClient() as client:
                response = await client.get(
                    api_url, params=params, timeout=self.timeout
                )
                response.raise_for_status()

                # レスポンスをJSONとしてパース
                data = response.json()

                # ディレクトリが存在しない場合は作成
                subdir.mkdir(parents=True, exist_ok=True)

                # 日本語を適切にエンコードしてJSONを保存
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

                self.successful.append(output_path)
                progress.update(task_id, advance=1)

        except httpx.RequestError as e:
            self.failed.append(
                MetadataError(
                    stats_data_id=entry.stats_data_id,
                    status_code=None,
                    error_message=f"Request failed: {str(e)}",
                )
            )
        except httpx.HTTPStatusError as e:
            self.failed.append(
                MetadataError(
                    stats_data_id=entry.stats_data_id,
                    status_code=e.response.status_code,
                    error_message=f"HTTP error: {e.response.reason_phrase}",
                )
            )
        except Exception as e:
            self.failed.append(
                MetadataError(
                    stats_data_id=entry.stats_data_id,
                    status_code=None,
                    error_message=f"Unexpected error: {str(e)}",
                )
            )

    async def download_all(
        self, entries: List[DBEntry], csv_name: str
    ) -> MetadataResult:
        """
        Download metadata for all entries.
        """
        # DB形式のエントリーのみを処理
        metadata_entries = [entry for entry in entries if entry.format == FileFormat.DB]

        if not metadata_entries:
            return MetadataResult(successful=[], failed=[])

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            # サブディレクトリの作成
            subdir = self.output_dir / Path(csv_name).stem

            # 各エントリーに対するダウンロードタスクの作成
            tasks = []
            for entry in metadata_entries:
                task_id = progress.add_task(
                    description=f"Downloading metadata for {entry.stats_data_id}",
                    total=1,
                )
                tasks.append(self.download_metadata(entry, subdir, progress, task_id))

            # 非同期ダウンロードの実行（同時実行数を制限）
            semaphore = asyncio.Semaphore(self.max_concurrent)

            async def download_with_semaphore(task):
                async with semaphore:
                    await task

            await asyncio.gather(*(download_with_semaphore(task) for task in tasks))

        return MetadataResult(successful=self.successful, failed=self.failed)


def display_metadata_result(result: MetadataResult) -> None:
    """Display metadata download results using rich"""
    if result.successful:
        console.print(
            f"\nSuccessfully downloaded {len(result.successful)} metadata files:",
            style="green",
        )
        for path in result.successful:
            console.print(f"  ✓ {path}", style="green")

    if result.failed:
        console.print(
            f"\nFailed to download {len(result.failed)} metadata files:", style="red"
        )
        for error in result.failed:
            status = f" (Status: {error.status_code})" if error.status_code else ""
            console.print(
                f"  ✗ {error.stats_data_id}{status}: {error.error_message}", style="red"
            )
