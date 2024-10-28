"""
Core download functionality for e-Stat Downloader
"""
import asyncio
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import chardet
import httpx
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
)

from .validators import FileFormat, URLEntry

console = Console()


@dataclass
class DownloadError:
    """Download error information"""

    url: str
    status_code: Optional[int]
    error_message: str


@dataclass
class DownloadResult:
    """Download operation result"""

    successful: list[Path]
    failed: list[DownloadError]


class DownloadManager:
    def __init__(
        self,
        output_dir: Path,
        max_concurrent: int = 8,
        timeout: float = 30.0,
    ):
        """
        Initialize download manager.

        Args:
            output_dir: Base directory for downloads
            max_concurrent: Maximum number of concurrent downloads
            timeout: Timeout for each download in seconds
        """
        self.output_dir = output_dir
        self.max_concurrent = max_concurrent
        self.timeout = timeout
        self.successful: list[Path] = []
        self.failed: list[DownloadError] = []

    def _detect_encoding_from_headers(self, headers: httpx.Headers) -> Optional[str]:
        """
        Extract encoding from Content-Type header.

        Args:
            headers: Response headers

        Returns:
            Encoding if found in headers, None otherwise
        """
        content_type = headers.get("content-type", "")
        if "charset=" in content_type.lower():
            match = re.search(r"charset=([^\s;]+)", content_type, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    def _detect_encoding_from_content(self, content: bytes) -> str:
        """
        Detect encoding from content using chardet.

        Args:
            content: File content as bytes

        Returns:
            Detected encoding or 'utf-8' as fallback
        """
        # chardetで文字コードを推測
        result = chardet.detect(content)
        if result["confidence"] > 0.7:  # 確信度が70%以上の場合のみ採用
            return result["encoding"]

        # e-Statの場合、確信度が低い場合はCP932(Shift-JIS)の可能性が高い
        try:
            content.decode("cp932")
            return "cp932"
        except UnicodeDecodeError:
            pass

        # それでもだめな場合はUTF-8を試す
        try:
            content.decode("utf-8")
            return "utf-8"
        except UnicodeDecodeError:
            pass

        raise ValueError("Could not detect file encoding")

    async def _convert_encoding(
        self, content: bytes, headers: httpx.Headers, target_encoding: str = "utf-8"
    ) -> str:
        """
        Convert content to target encoding.

        Args:
            content: File content as bytes
            headers: Response headers
            target_encoding: Target encoding (default: utf-8)

        Returns:
            Content converted to target encoding
        """
        # 1. まずヘッダーからエンコーディングを確認
        source_encoding = self._detect_encoding_from_headers(headers)

        # 2. ヘッダーから取得できない場合は内容から推測
        if not source_encoding:
            source_encoding = self._detect_encoding_from_content(content)

        # 3. 変換処理
        try:
            text = content.decode(source_encoding)
            return text
        except UnicodeDecodeError as e:
            raise ValueError(f"Failed to decode content using {source_encoding}: {e}")

    async def download_file(
        self,
        entry: URLEntry,
        subdir: Path,
        progress: Progress,
        task_id: int,
    ) -> None:
        """
        Download a single file.

        Args:
            entry: Validated URL entry
            subdir: Subdirectory for this download
            progress: Progress bar instance
            task_id: Task ID for progress tracking
        """
        # stats_data_idとフォーマットに基づいてファイル名を生成
        output_path = subdir / entry.get_filename()

        try:
            async with httpx.AsyncClient() as client:
                async with client.stream(
                    "GET", str(entry.url), timeout=self.timeout
                ) as response:
                    response.raise_for_status()
                    total_size = int(response.headers.get("content-length", 0))
                    progress.update(task_id, total=total_size)

                    # ディレクトリが存在しない場合は作成
                    subdir.mkdir(parents=True, exist_ok=True)

                    # コンテンツの取得
                    content = b""
                    with open(output_path, "wb") as f:
                        async for chunk in response.aiter_bytes():
                            content += chunk
                            f.write(chunk)
                            progress.update(task_id, advance=len(chunk))

                    # CSVの場合、エンコーディング変換
                    if entry.format == FileFormat.CSV:
                        try:
                            text = await self._convert_encoding(
                                content, response.headers
                            )
                            output_path.write_text(text, encoding="utf-8")
                        except ValueError as e:
                            self.failed.append(
                                DownloadError(
                                    url=str(entry.url),
                                    status_code=response.status_code,
                                    error_message=f"Encoding conversion failed: {e}",
                                )
                            )
                            return

                    self.successful.append(output_path)

        except httpx.RequestError as e:
            self.failed.append(
                DownloadError(
                    url=str(entry.url),
                    status_code=None,
                    error_message=f"Request failed: {str(e)}",
                )
            )
        except httpx.HTTPStatusError as e:
            self.failed.append(
                DownloadError(
                    url=str(entry.url),
                    status_code=e.response.status_code,
                    error_message=f"HTTP error: {e.response.reason_phrase}",
                )
            )
        except Exception as e:
            self.failed.append(
                DownloadError(
                    url=str(entry.url),
                    status_code=None,
                    error_message=f"Unexpected error: {str(e)}",
                )
            )

    async def download_all(
        self, entries: list[URLEntry], csv_name: str
    ) -> DownloadResult:
        """
        Download all files from the provided entries.

        Args:
            entries: List of validated URL entries
            csv_name: Name of the source CSV file (used for subdirectory name)

        Returns:
            DownloadResult containing successful and failed downloads
        """
        # 進捗バーの設定
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            # サブディレクトリの作成（CSVファイル名をベースに）
            subdir = self.output_dir / Path(csv_name).stem

            # 各URLに対するダウンロードタスクの作成
            tasks = []
            for entry in entries:
                task_id = progress.add_task(
                    description=f"Downloading {Path(entry.url.path).name}",
                    total=None,  # コンテンツサイズが分かるまではNone
                )
                tasks.append(self.download_file(entry, subdir, progress, task_id))

            # 非同期ダウンロードの実行（同時実行数を制限）
            semaphore = asyncio.Semaphore(self.max_concurrent)

            async def download_with_semaphore(task):
                async with semaphore:
                    await task

            await asyncio.gather(*(download_with_semaphore(task) for task in tasks))

        return DownloadResult(successful=self.successful, failed=self.failed)


def display_download_result(result: DownloadResult) -> None:
    """Display download results using rich"""
    if result.successful:
        console.print(
            f"\nSuccessfully downloaded {len(result.successful)} files:", style="green"
        )
        for path in result.successful:
            console.print(f"  ✓ {path}", style="green")

    if result.failed:
        console.print(f"\nFailed to download {len(result.failed)} files:", style="red")
        for error in result.failed:
            status = f" (Status: {error.status_code})" if error.status_code else ""
            console.print(
                f"  ✗ {error.url}{status}: {error.error_message}", style="red"
            )