import asyncio
from pathlib import Path
from unittest.mock import patch, AsyncMock, MagicMock

import pytest
import httpx
from pydantic import HttpUrl

from estat_downloader.core.downloader import DownloadManager
from estat_downloader.core.validators import URLEntry, FileFormat


@pytest.fixture
def estat_url():
    """URL for the target e-Stat file"""
    return "https://www.e-stat.go.jp/stat-search/file-download?&statInfId=000040228829&fileKind=1"


@pytest.fixture
def shift_jis_content():
    """Shift-JIS encoded sample content for testing"""
    # This is a simple content encoded in Shift-JIS
    # "テスト,データ,123" encoded in Shift-JIS
    return b"\x83\x65\x83\x58\x83\x67,\x83\x66\x81\x5b\x83\x5e,123"


@pytest.fixture
def url_entry(estat_url):
    """Create a URL entry for testing"""
    return URLEntry(
        url=HttpUrl(estat_url),
        format=FileFormat.CSV,
        stats_data_id="0000401234",
        title="Test Data",
    )


@pytest.mark.asyncio
async def test_convert_encoding_ignores_headers():
    """
    Test that _convert_encoding ignores headers and uses Shift-JIS by default.

    This specifically verifies:
    1. Even with UTF-8 in headers, the method detects Shift-JIS from content
    2. The final content is correctly converted to UTF-8
    """
    # Create a download manager
    manager = DownloadManager(output_dir=Path("tmp_dl"))

    # Create sample shift-jis content
    shift_jis_content = b"\x83\x65\x83\x58\x83\x67,\x83\x66\x81\x5b\x83\x5e,123"  # "テスト,データ,123" in Shift-JIS

    # Create headers with misleading UTF-8 information
    headers = httpx.Headers({"content-type": "text/csv; charset=utf-8"})

    # Call the method under test
    result = await manager._convert_encoding(
        content=shift_jis_content, headers=headers, target_encoding="utf-8"
    )

    # Verify the result is properly decoded
    assert "テスト,データ,123" in result

    # Directly test the encoding detection from content
    detected_encoding = manager._detect_encoding_from_content(shift_jis_content)
    assert (
        detected_encoding.lower() == "cp932"
    ), f"Expected cp932, got {detected_encoding}"


@pytest.mark.asyncio
async def test_detect_encoding_from_content_prioritizes_shift_jis():
    """
    Test that _detect_encoding_from_content prioritizes Shift-JIS detection.
    """
    # Create a download manager
    manager = DownloadManager(output_dir=Path("tmp_dl"))

    # Create sample shift-jis content
    shift_jis_content = b"\x83\x65\x83\x58\x83\x67,\x83\x66\x81\x5b\x83\x5e,123"  # "テスト,データ,123" in Shift-JIS

    # Test direct encoding detection
    detected_encoding = manager._detect_encoding_from_content(shift_jis_content)

    # Verify it detects Shift-JIS
    assert (
        detected_encoding.lower() == "cp932"
    ), f"Expected cp932, got {detected_encoding}"

    # Create content that could be decoded as either UTF-8 or Shift-JIS
    # This is ASCII content which is valid in many encodings
    ascii_content = b"test,data,123"

    # Even with content that could be decoded in multiple ways, we expect cp932 first
    # because the method should try cp932 before UTF-8
    detected_encoding = manager._detect_encoding_from_content(ascii_content)
    assert (
        detected_encoding.lower() == "cp932"
    ), f"Should prefer cp932 for ambiguous content, got {detected_encoding}"


@pytest.mark.asyncio
async def test_end_to_end_encoding_with_file_write(tmp_path):
    """
    Test for conversion from Shift-JIS to UTF-8 with file writing,
    focusing only on the encoding aspect.
    """
    # Create a download manager
    download_dir = tmp_path / "downloads"
    download_dir.mkdir()
    manager = DownloadManager(output_dir=download_dir)

    # Create sample shift-jis content
    shift_jis_content = b"\x83\x65\x83\x58\x83\x67,\x83\x66\x81\x5b\x83\x5e,123"  # "テスト,データ,123" in Shift-JIS

    # Create a test file and write the shift-jis content
    test_file = download_dir / "test.csv"
    test_file.write_bytes(shift_jis_content)

    # Read the content and convert with our method
    content = test_file.read_bytes()
    headers = httpx.Headers({"content-type": "text/csv"})

    # Convert the encoding
    converted_text = await manager._convert_encoding(content, headers)

    # Write back as UTF-8
    output_file = download_dir / "converted.csv"
    output_file.write_text(converted_text, encoding="utf-8")

    # Verify the resulting file exists and has correct content
    assert output_file.exists()
    content = output_file.read_text(encoding="utf-8")
    assert "テスト,データ,123" in content

    # Verify the encoding detection was correct
    detected_encoding = manager._detect_encoding_from_content(shift_jis_content)
    assert (
        detected_encoding.lower() == "cp932"
    ), f"Expected cp932, got {detected_encoding}"


@pytest.mark.asyncio
async def test_real_estat_url_encoding_conversion():
    """
    Test with the actual URL to verify encoding detection.
    This is a slower integration test that makes actual network requests.
    Skip with -k "not integration" when running pytest for faster tests.
    """
    # pytest.skip("Integration test - uncomment to run with actual network requests")

    # Create a download manager
    download_dir = Path("tmp_dl") / "test_encoding"
    download_dir.mkdir(parents=True, exist_ok=True)
    manager = DownloadManager(output_dir=download_dir)

    # Create URLEntry for the real e-Stat URL
    entry = URLEntry(
        url=HttpUrl(
            "https://www.e-stat.go.jp/stat-search/file-download?&statInfId=000040228829&fileKind=1"
        ),
        format=FileFormat.CSV,
        stats_data_id="0000401234",
        title="Real Test",
    )

    # Create a simple progress for tracking
    progress = MagicMock()
    task_id = MagicMock()

    # Download the file
    await manager.download_file(entry, download_dir, progress, task_id)

    # Verify the output file exists
    output_file = download_dir / f"{entry.stats_data_id}.csv"
    assert output_file.exists()

    # Check if the file is in the successful list
    assert len(manager.successful) > 0

    # There should be no errors
    assert len(manager.failed) == 0
