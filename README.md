# 🐎 keiba-model  
**JRA公式レース結果ページのみを一次ソースにした 競馬予測AI / ETL & ML Pipeline**

このリポジトリは、  
**JRA公式レース結果ページ（1ページのみ）をデータソースにした完全自動 ETL パイプラインと、  
機械学習モデルによる競馬予測（P(in_top3)）構築プロジェクト** です。

---

# 🔥 Features（特徴）

## ✔ 1. **JRA公式サイトだけで完結する ETL**
- データソースは **JRA 結果ページ 1 枚のみ**  
- オッズ / タイム / 馬体重 / 騎手・調教師 / 上がり / コーナー通過 etc  
- 外部サイト（netkeiba等）には一切依存しない構造  
- 生 HTML を `data/raw/jra/` に保存して再現性を確保

## ✔ 2. **v4 スキーマによる再現性のある DB 設計**
docs 内に以下を格納：

docs/db/data_sources.md ← データソース仕様（v4）
docs/db/table_definitions.md ← テーブル定義（v4）
docs/db/etl_plan.md ← ETL設計書（v4）

markdown
コードをコピーする

スキーマは  
**「Must/Should のうち JRA 結果ページで取得可能な生データのみ」**  
を中心に構成。

## ✔ 3. **段階的 ETL 開発（Step 1→5 のロードマップ）**
ETL は5段階に分割されたフェーズで実装：

1. 必須カラム（馬番, 枠番, オッズ等）
2. タイム/上がり/着差処理
3. レース条件（クラス, 年齢, 性別）
4. コースマスタ（course_id）導入
5. 前走情報 prev_* 自動リンク

## ✔ 4. **ML Notebook による特徴量チェック & 予測モデル**
notebooks/
├─check_features.ipynb
├─inspect_html.ipynb
└─first_model.ipynb

yaml
コードをコピーする

---

# 📁 Directory Structure（構成）

keiba-model/
│
├── data/
│ ├── keiba.db # 実験用DB（v3 → v4 へ移行予定）
│ ├── master/ # コース等の静的マスタ
│ └── raw/
│ ├── jra/ # 保存されたJRA HTML（一次ソース）
│ └── netkeiba/ # 現状は非使用（将来削除予定）
│
├── docs/
│ └── db/
│ ├── data_sources.md
│ ├── etl_plan.md
│ └── table_definitions.md
│
├── notebooks/
│ ├── check_features.ipynb
│ ├── inspect_html.ipynb
│ └── first_model.ipynb
│
├── scripts/
│ ├── fetch_jra_html.py # Extract
│ ├── etl_one_race.py # Transform + Load (v3 → v4移行中)
│ ├── create_db_v3.py
│ ├── create_view_v_race_features.py
│ └── ...（各種補助スクリプト）
│
├── src/
│ ├── jra_parser.py # HTML → dict 変換（v4 対応予定）
│ ├── test_db.py
│ └── test_db2.py
│
└── README.md

yaml
コードをコピーする

---

# 🚀 Quick Start（使い方）

## 1. 仮想環境 & 依存パッケージ

```bash
pip install -r requirements.txt
※ requirements.txt は必要に応じて自動生成可能（→ 下部参照）

2. JRA HTML を取得（Extract）
bash
コードをコピーする
python scripts/fetch_jra_html.py --race_id 202405020411
保存先：

bash
コードをコピーする
data/raw/jra/race_202405020411.html
3. ETL（Transform → Load）
bash
コードをコピーする
python scripts/etl_one_race.py --race_id 202405020411
結果は SQLite DB（data/keiba.db または v4 DB）へ挿入されます。

4. Notebook で解析（ML / Feature Engineering）
bash
コードをコピーする
jupyter lab
🔧 Development（開発者向け）
v4 ETL 開発のロードマップ
（詳細 → docs/db/etl_plan.md）

Step	内容
1	race_results / races の最小カラムで end-to-end
2	タイム・上がり・着差処理
3	レース条件（class, age_cond, sex_cond）
4	course_id マッピング
5	prev_*（前走リンク）を自動付与

🤖 Auto-Generate（Codex / GitHub Actions 連携）
この README は Codex / ChatGPT Projects によって
以下を自動生成／更新できる構造になっています：

新しいスクリプトが追加された時の項目追加

ディレクトリ構造ブロックのアップデート

テーブル定義変更（v5 など）に伴うドキュメント更新

requirements.txt の自動生成

notebook の説明追加

Codex 自動更新案（将来実装例）
bash
コードをコピーする
python tools/generate_readme.py \
  --scan scripts/ src/ docs/db \
  --output README.md
README.md の維持を半自動化して技術負債をゼロにします。

📄 License
This project is for personal research & educational use.
JRA公式サイトの利用規約を遵守して運用してください。

👤 Author
ryuji785

個人開発 / 競馬予測AIプロジェクト

ChatGPTによるプロンプト・コード生成も併用
