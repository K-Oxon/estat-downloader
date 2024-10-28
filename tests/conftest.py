import shutil
from pathlib import Path

import pytest

# fixturesディレクトリのパスを取得
FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_csv_path(tmp_path) -> Path:
    """Valid CSV file with required columns"""
    csv_content = """url,format,stats_data_id,title
https://www.e-stat.go.jp/stat-search/file-download?&statInfId=000040171707&fileKind=0,XLS,000010340062,7_歳出内訳及び財源内訳（その１）_1
https://www.e-stat.go.jp/stat-search/file-download?&statInfId=000040171707&fileKind=1,CSV,000010340063,7_歳出内訳及び財源内訳（その１）_1"""

    csv_path = tmp_path / "valid_urls.csv"
    csv_path.write_text(csv_content)
    return csv_path


@pytest.fixture
def invalid_csv_path(tmp_path) -> Path:
    """Invalid CSV file missing required columns"""
    csv_content = """url,title
https://www.e-stat.go.jp/data/000001,Sample CSV"""

    csv_path = tmp_path / "invalid_urls.csv"
    csv_path.write_text(csv_content)
    return csv_path


@pytest.fixture
def malformed_csv_path(tmp_path) -> Path:
    """CSV file with malformed data"""
    csv_content = """url,format,stats_data_id,title
https://invalid-url,INVALID,000001,Invalid Format
https://www.e-stat.go.jp/data/000002,CSV,aaa,Empty ID"""

    csv_path = tmp_path / "malformed_urls.csv"
    csv_path.write_text(csv_content)
    return csv_path


@pytest.fixture
def sample_files(tmp_path) -> dict[str, Path]:
    """Sample downloaded files for comparison"""
    # サンプルCSVファイル
    # xh -b get "https://www.e-stat.go.jp/stat-search/file-download?&statInfId=000040171707&fileKind=1" | iconv -f SHIFT_JIS -t UTF-8 | head -n 4
    csv_content = """決算年度,業務コード,団体コード ,県名,団体名,団体区分,表番号,表名称,行番号,行名称,001:議会費,002:総務費・総額,003:総務費・総務管理費,004:総務費・徴税費,005:総務費・戸籍・住民基本台帳費,006:総務費・選挙費,007:総務費・統計調査費,008:総務費・監査委員費
2022,61,11002,北海道,札幌市,1,7,歳出内訳及び財源内訳（その1）,1,人件費,1422205,24562722,15914353,4528488,3060653,687242,101495,270491
2022,61,12025,北海道,函館市,8,7,歳出内訳及び財源内訳（その1）,1,人件費,403046,4499278,3023486,824727,406721,131280,28249,84815
2022,61,12033,北海道,小樽市,3,7,歳出内訳及び財源内訳（その1）,1,人件費,303305,2604383,1830111,475929,181858,55557,23420,37508
    """

    csv_path = tmp_path / "sample.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    # サンプルExcelファイル（バイナリデータ）
    excel_src = FIXTURES_DIR / "sample_000010340062.xlsx"
    excel_dst = tmp_path / "sample.xlsx"
    shutil.copy(excel_src, excel_dst)

    return {"csv": csv_path, "excel": excel_dst}
