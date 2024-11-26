import pytest

from estat_downloader.core.validators import ValidationResult, load_and_validate_csv


def test_load_and_validate_csv_with_valid_file(sample_csv_path):
    """Test loading and validating a valid CSV file."""
    result = load_and_validate_csv(sample_csv_path)
    assert isinstance(result, ValidationResult)
    assert len(result.url_entries) == 2
    assert len(result.invalid_rows) == 0

    # 各エントリーの検証
    entry = result.url_entries[1]
    assert entry.stats_data_id == "000010340063"
    assert entry.format == "CSV"
    assert (
        str(entry.url)
        == "https://www.e-stat.go.jp/stat-search/file-download?&statInfId=000040171707&fileKind=1"
    )


def test_load_and_validate_csv_with_missing_columns(invalid_csv_path):
    """Test loading CSV with missing required columns."""
    with pytest.raises(ValueError) as exc_info:
        load_and_validate_csv(invalid_csv_path)
    assert "Missing required columns" in str(exc_info.value)


def test_load_and_validate_csv_with_malformed_data(malformed_csv_path):
    """Test loading CSV with malformed data."""
    result = load_and_validate_csv(malformed_csv_path)
    assert len(result.url_entries) == 0
    assert len(result.invalid_rows) == 2
    # URLフォーマットエラーの確認
    assert "URL must be from e-stat.go.jp domain" in result.invalid_rows[0][1]
    # 空のstats_data_idエラーの確認
    assert "stats_data_id must be a 10 or 12-digit number" in result.invalid_rows[1][1]
