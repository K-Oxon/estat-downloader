"""
CLI entry point for e-Stat Downloader
"""

import asyncio
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from estat_downloader import __version__
from estat_downloader.core.downloader import DownloadManager
from estat_downloader.core.metadata_downloader import (
    MetadataDownloader,
    display_metadata_result,
)
from estat_downloader.core.validators import (
    display_validation_result,
    load_and_validate_csv,
)

console = Console()
app = typer.Typer(
    name="estat-downloader",
    help="Download statistical data from e-Stat",
    no_args_is_help=True,
)


@app.command()
def download(
    url_list: Annotated[
        Path,
        typer.Argument(
            help="Path to CSV file containing URLs to download",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
        ),
    ],
    output_dir: Annotated[
        Optional[Path],
        typer.Option(
            "--output-dir",
            "-o",
            help="Directory to save downloaded files",
        ),
    ] = Path("tmp_dl"),
    max_concurrent: Annotated[
        int,
        typer.Option(
            "--max-concurrent",
            "-c",
            help="Maximum number of concurrent downloads",
        ),
    ] = 4,
) -> None:
    """Download statistical data files from e-Stat based on URL list."""
    if not output_dir:
        output_dir = Path("tmp_dl")

    table = Table(title="Download Parameters")
    table.add_column("Parameter", style="cyan")
    table.add_column("Value", style="green")

    table.add_row("URL List Path", str(url_list.absolute()))
    table.add_row("Output Directory", str(output_dir.absolute()))

    console.print(table)

    try:
        # URLリストの検証
        console.print(Panel("Validating URL list...", title="Step 1"))
        validation_result = load_and_validate_csv(url_list)
        display_validation_result(validation_result)

        if validation_result.invalid_rows:
            if not typer.confirm(
                "\nSome entries are invalid. Continue with valid entries?"
            ):
                raise typer.Exit(1)

        if not validation_result.url_entries:
            console.print("No valid entries found.", style="red")
            raise typer.Exit(1)

        # ダウンロードの実行
        console.print(Panel("Starting download...", title="Step 2"))
        downloader = DownloadManager(
            output_dir=output_dir,
            max_concurrent=max_concurrent,
        )

        # 非同期ダウンロードの実行
        download_result = asyncio.run(
            downloader.download_all(validation_result.url_entries, url_list.name)
        )

        # 結果の表示
        console.print(Panel("Download completed", title="Step 3"))

        if download_result.successful:
            console.print(
                f"\nSuccessfully downloaded {len(download_result.successful)} files:",
                style="green",
            )
            for path in download_result.successful:
                console.print(f"  ✓ {path}", style="green")

        if download_result.failed:
            console.print(
                f"\nFailed to download {len(download_result.failed)} files:",
                style="red",
            )
            for error in download_result.failed:
                status = f" (Status: {error.status_code})" if error.status_code else ""
                console.print(
                    f"  ✗ {error.url}{status}: {error.error_message}", style="red"
                )
            raise typer.Exit(1)

    except Exception as e:
        console.print(f"Error: {e}", style="red")
        raise typer.Exit(1)


@app.command()
def metadata(
    url_list: Annotated[
        Path,
        typer.Argument(
            help="Path to CSV file containing stats data IDs",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
        ),
    ],
    output_dir: Annotated[
        Optional[Path],
        typer.Option(
            "--output-dir",
            "-o",
            help="Directory to save metadata files",
        ),
    ] = Path("tmp_dl"),
    max_concurrent: Annotated[
        int,
        typer.Option(
            "--max-concurrent",
            "-c",
            help="Maximum number of concurrent downloads",
        ),
    ] = 3,
) -> None:
    """Download metadata for statistical data from e-Stat."""
    if not output_dir:
        output_dir = Path("tmp_dl")

    try:
        # URLリストの検証
        console.print(Panel("Validating stats data IDs...", title="Step 1"))
        validation_result = load_and_validate_csv(url_list)
        display_validation_result(validation_result)

        if validation_result.invalid_rows:
            if not typer.confirm(
                "\nSome entries are invalid. Continue with valid entries?"
            ):
                raise typer.Exit(1)

        if not validation_result.db_entries:
            console.print("No valid entries found.", style="red")
            raise typer.Exit(1)

        # メタデータダウンローダーの初期化と実行
        console.print(Panel("Starting metadata download...", title="Step 2"))
        try:
            downloader = MetadataDownloader(
                output_dir=output_dir,
                max_concurrent=max_concurrent,
            )
        except ValueError as e:
            console.print(f"Error: {e}", style="red")
            console.print(
                "Please set the ESTAT_API_KEY environment variable.", style="yellow"
            )
            raise typer.Exit(1)

        # 非同期ダウンロードの実行
        download_result = asyncio.run(
            downloader.download_all(validation_result.db_entries, url_list.name)
        )

        # 結果の表示
        console.print(Panel("Metadata download completed", title="Step 3"))
        display_metadata_result(download_result)

        if download_result.failed:
            raise typer.Exit(1)

    except Exception as e:
        console.print(f"Error: {e}", style="red")
        raise typer.Exit(1)


def version_callback(value: bool) -> None:
    """Display version information."""
    if value:
        console.print(f"estat-downloader version {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-v",
            help="Show version information",
            callback=version_callback,
        ),
    ] = False,
) -> None:
    """
    A CLI tool for downloading statistical data from e-Stat.
    """
    pass


if __name__ == "__main__":
    app()
