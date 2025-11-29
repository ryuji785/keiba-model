# ETL計画書 v4（Extract / Transform / Load）

競馬予測モデル用データベース（v4スキーマ）に対して  
**JRA公式レース結果ページだけ** を一次ソースとして  
完全自動 ETL（Extract → Transform → Load）を実現するための計画書。

---

# 1. ゴール

1. JRA結果ページの HTML を取得し、保存する（Extract）
2. HTML を Python で解析し、v4 スキーマの dict へ変換（Transform）
3. SQLite DB（v4スキーマ）へ安全に挿入する（Load）
4. 1レース単位で再現性のあるデータ投入ができる構造と運用を作る

---

# 2. ディレクトリ構成（v4）

project/
data/
raw/
jra/
race_202405020411.html
docs/
data_sources_v4.md
etl_plan_v4.md
table_definitions_v4.md
scripts/
fetch_jra_html.py # Extract
parse_jra_race.py # Transform
load_to_sqlite_v4.py # Load
db/
keiba_v4.db
notebooks/
first_model.ipynb

yaml
コードをコピーする

---

# 3. ETL フェーズ

---

## ◆ Phase 1：Extract（HTML 取得）

### ● 目的

- JRA レース結果ページの HTML を取得し、  
  `data/raw/jra/` 以下に保存する。

### ● 処理フロー

1. race_id を指定（例：202405020411）
2. JRA公式 URL を組み立てる
3. `requests.get()` で取得
4. HTML を下記に保存：

data/raw/jra/race_<race_id>.html

yaml
コードをコピーする

### ● 注意点

- 1レース1回のみ取得  
  → 同じレースを何度も取得しない
- エラー時は `race_<id>_error.html` として保存

---

## ◆ Phase 2：Transform（HTML → Python dict）

### ● 目的
HTML から v4 スキーマの以下 2 種類の辞書を生成：

- `race_dict`（races テーブル 1件）
- `results_list`（race_results テーブル 複数件）

### ● パース対象

#### 1）レース概要エリア
- 開催日
- レース名
- 距離・芝/ダート
- 天候
- 馬場状態
- クラス / 年齢条件 / 性別条件

#### 2）着順表
- 着順
- 枠番・馬番
- 馬名
- 騎手（URLから ID 抽出）
- 調教師（URLから ID 抽出）
- タイム
- 上がり3F
- 着差
- 単勝オッズ・人気
- 馬体重
- コーナー通過順位

#### 3）前走情報（prev_*）
- ETL 直後は **NULL のまま**  
- 別の “集計バッチ” で後からリンク付けする（Phase 5 で記述）

---

## ◆ Phase 3：Load（dict → SQLite）

### ● 目的
`race_dict` / `results_list` を v4 スキーマへ挿入。

### ● ロジック

- races：INSERT OR REPLACE
- race_results：PRIMARY KEY（race_id, horse_id）で UPSERT
- horses / jockeys / trainers：  
  - IDが存在しなければ INSERT  
  - あればスキップ（上書き不要）

### ● トランザクション
1レース分は **1トランザクション** とし、失敗時はロールバック。

---

# 4. 段階的実装ロードマップ（重要）

v4 スキーマの必須カラムを **5 段階** で実装する。

---

## Step 1（MVP）：最低限 end-to-end

- race_results：  
  - race_id / horse_id / bracket_no / horse_no  
  - finish_rank / finish_status  
  - odds / popularity  
  - weight / body_weight / weight_diff  

- races：  
  - race_id / date / race_no / distance / surface / going / weather / num_runners

- horses / jockeys / trainers：  
  - ID＋名前のみ

目的：  
**1レース投げて→DBに保存までが通る状態を最速で作る**

---

## Step 2：タイム・上がり・着差

- finish_time_sec
- last_3f
- margin_sec（文字列→秒換算）

---

## Step 3：コーナー＆条件

- corner_pass_order
- class / age_cond / sex_cond / race_name

---

## Step 4：course_id 正規化

- course_id を courses テーブルにマッピング
- races.course_id を正式IDへ変換

---

## Step 5：前走リンク（prev_*）

race_results の以下項目を埋めるバッチを別途作成：

- prev_race_id
- prev_finish_rank
- prev_margin_sec
- prev_time_sec
- prev_last_3f
- days_since_last

Transform では埋めず、  
**全レース投入後に別スクリプトで再計算する** のが最も安全。

---

# 5. ログ運用

logs/etl.log

yaml
コードをコピーする

- race_id
- フェーズ（Extract / Transform / Load）
- 成否
- エラーメッセージ

---

# 6. 今後の拡張

- race_laps テーブル（ラップタイム抽出）
- horses_stats / jockeys_stats / trainers_stats の自動集計
- dataset versioning（MLflow / DVC）

---

# 7. まとめ

- **JRA結果ページ1枚で ETL 完結**  
- **v4 スキーマ対応の ETL を段階的に構築**  
- **前走リンクは後フェーズで集計する設計**  
- **courses は手動マスタ、他は自動抽出**