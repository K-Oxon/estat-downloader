# e-Stat downloader

e-Statから指定された統計表IDのファイルをダウンロードするCLIツール/ライブラリです。

## 背景

WIP

## 機能

- CSVファイルに記載された統計表IDに基づく一括ダウンロード
- CSVはUTF-8に変換
- 並行ダウンロードによる高速化
- CLIツールとライブラリの両方の形式で利用可能

## 使用方法


### インストール

```bash
pip install git+https://github.com/K-Oxon/estat-downloader.git
```

### CLIとして使用

1. URLリストのCSVファイルを準備（例: `sample/test_local_finance_expenditure_and_revenue_breakdown.csv`）
   1. CSVファイルには url, format, stats_data_id の3つの列が必須です
      1. `dataset__title__survey_date`の列があった場合、その内容(通常は調査年・年月)でサブディレクトリを作成します
   2. [e-Stat APIのデータカタログ情報取得API](https://www.e-stat.go.jp/api/api-info/e-stat-manual3-0#api_4_7) で取得できる情報を加工することを想定しています
2. コマンドを実行

```bash
# 基本的な使用方法
estat-downloader download urls.csv

# 出力ディレクトリを指定
estat-downloader download urls.csv --output-dir ./downloads

# 並行ダウンロード数を指定
estat-downloader download urls.csv --max-concurrent 5

# ヘルプの表示
estat-downloader --help
estat-downloader download --help
```

### ライブラリとして使用

```python
"""csvファイルを指定"""
import asyncio
from pathlib import Path
from estat_downloader import download_stats, validate_url_list

# CSVファイルの検証
result = validate_url_list(Path("urls.csv"))
print(f"Valid entries: {len(result.valid_entries)}")

# ダウンロードの実行
async def main():
    result = await download_stats("urls.csv", output_dir=Path("downloads"))
    print(f"Successfully downloaded: {len(result.successful)}")

asyncio.run(main())
```

```python
"""プログラムから直接URLを指定する場合:
"""
import asyncio
from estat_downloader import download_stats, URLEntry

async def main():
    entries = [
        URLEntry(
            url="https://www.e-stat.go.jp/stat-search/file-download?statInfId=000031234567",
            format="CSV",
            stats_data_id="000031234567",
            title="Population Statistics 2020"
        )
    ]
    
    result = await download_stats(entries, output_dir=Path("downloads"))
    for path in result.successful:
        print(f"Downloaded: {path}")

asyncio.run(main())
```

## その他機能

### メタデータ一括ダウンロード

```bash
export ESTAT_API_KEY="your-api-key-here"

# 基本的な使用方法
estat-downloader metadata urls.csv

# 出力ディレクトリを指定
estat-downloader metadata urls.csv -o ./metadata

# 並行ダウンロード数を指定
estat-downloader metadata urls.csv -c 5
```

## dev

```bash
uv sync
```

```bash
uv run python -c "import estat_downloader; estat_downloader.main()"
# or
uv run estat-downloader
```

test

```bash
uv run pytest
```

## LICENSE

This project is licensed under the MIT License, see the [LICENSE](/LICENSE) file for details
