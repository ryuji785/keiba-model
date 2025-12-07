# バッチ取得〜ETL 運用手順

## 1. HTML 取得
- 月単位: `python scripts/fetch_month_htmls.py 2024 05 --fail-log fail_202405.txt`
- 年単位: `python scripts/fetch_year_htmls.py 2024 --fail-log fail_2024.txt`
- まとめ取得（2024済み前提で 2021/2022/2023/2025 を回す）: `python scripts/fetch_missing_years.py`
- 上書きしたい場合は `--overwrite` を付ける。
- 失敗ログ: `--fail-log fail_log.txt` を付けると失敗した CNAME を記録。再取得時はログを見て必要に応じて個別 fetch。

### 文字化け対策
- 取得時は Shift_JIS/CP932 を優先してデコード済み（common_fetch/parse_jra_race）。既存 HTML に文字化けがある場合は `data/raw/jra` を退避して再取得してから ETL を実行する。

## 2. ETL の実行
- 一括（取得→ETL→前走リンク→特徴量ビューまで）: `python scripts/fetch_missing_years.py --db data/keiba.db`
- 全HTMLを一括ETL: `python scripts/etl_all_htmls.py --db data/keiba.db`
- 個別ETL: `python scripts/etl_one_race_v4.py <race_id> --db data/keiba.db`

## 3. 品質チェック
- サマリ: `python scripts/report_race_quality.py --db data/keiba.db --summary`
- 欠損多レース: `python scripts/report_race_quality.py --db data/keiba.db`
- class/age/sex 埋まり: `python scripts/class_age_sex_report.py --db data/keiba.db`

## 4. 前走リンク付け (Step5)
- `python scripts/prev_race_link.py --db data/keiba.db`

## 5. 特徴量ビュー作成
- `python scripts/create_feature_views.py --db data/keiba.db`
- 定義は `scripts/feature_views.sql` を参照。

## 6. 再試行フロー
- fetch 失敗は `--fail-log` で記録し、必要に応じて再取得。
- ETL 失敗レースはログの race_id を個別に `etl_one_race_v4.py` で再実行。
