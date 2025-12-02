# JRAデータスクレイピング実装方針 & タスク計画（v4）
**Based on:**  
- JRAデータスクレイピング技術白書（Ver.1） :contentReference[oaicite:0]{index=0}  
- data_sources.md :contentReference[oaicite:1]{index=1}  
- etl_plan.md :contentReference[oaicite:2]{index=2}  

---

# 1. 実装全体方針（設計思想）

## ■ 1-1. データソース統一
- **一次ソースは JRA公式サイト（レース結果ページ 1 枚）に統一**  
- オッズ・人気含め **追加ページへの遷移は不要**  
- すべて `accessS.html?CNAME=xxxx` の結果ページから抽出可能  

（根拠 → data_sources.md） :contentReference[oaicite:3]{index=3}

---

## ■ 1-2. 取得経路（スクレイピング戦略）
白書（Ver.1）が示す通り、race_id から直接 URL を生成できないため、  
**トップページ → カレンダー → 開催日 → レース一覧 → レース結果**  
という階層遷移を必ず踏む。

（根拠 → 白書 1, 3 章） :contentReference[oaicite:4]{index=4}

---

## ■ 1-3. CNAME パラメータの扱い
- JRA の動的リンクは `doAction(url, cname)` により **POST 遷移**  
- 近年は `href="/JRADB/accessS.html?CNAME=xxxx"` も存在 → **GET で取得可能**

（根拠 → 白書 2 章） :contentReference[oaicite:5]{index=5}

---

## ■ 1-4. HTML 保存ポリシー
data/raw/jra/race_<race_id>.html

yaml
コードをコピーする
- **再取得禁止**（公式への負荷軽減・再現性確保）  
- 後から Transform だけ修正可能になる  

（根拠 → data_sources.md / etl_plan.md）  
:contentReference[oaicite:6]{index=6} :contentReference[oaicite:7]{index=7}

---

## ■ 1-5. ETL は段階的実装（v4）
- Step1：最低限 end-to-end  
- Step2：タイム系  
- Step3：条件・コーナー  
- Step4：course_id マッピング  
- Step5：前走リンクバッチ  

（根拠 → etl_plan.md 4 章） :contentReference[oaicite:8]{index=8}

---

# 2. HTML 構造の理解とパース仕様

## ■ 2-1. 中間ページ
- **カレンダー**：日付リンク抽出  
- **開催日ページ**：開催（2回東京9日など）の CNAME 抽出  
- **レース一覧ページ**：各レースの CNAME（`pw01sde...`）を抽出  

（根拠 → 白書 4 章） :contentReference[oaicite:9]{index=9}

---

## ■ 2-2. レース結果ページから取れる項目
Must の全項目が **1 ページ内で完結して取得可能**。

- レース概要  
- 枠番・馬番  
- 馬名・馬 ID  
- 騎手名・騎手 ID  
- 調教師名・調教師 ID  
- タイム  
- 上がり3F  
- 馬体重  
- オッズ  
- 人気  
- コーナー  
- 着差  

（根拠 → 白書 6 章） :contentReference[oaicite:10]{index=10}

---

## ■ 2-3. エンコーディング
- JRA は **Shift_JIS（CP932）**  
- BeautifulSoup パース時は  
  `response.content.decode("cp932", errors="ignore")` を推奨  

（根拠 → 白書 4,5 章） :contentReference[oaicite:11]{index=11}

---

# 3. 実装タスク一覧（詳細）

## フェーズ 0：環境準備
- [ ] GitHub リポジトリ init  
- [ ] ディレクトリ構造（etl_plan 準拠）  
scripts/
data/raw/jra/
db/
docs/

yaml
コードをコピーする
- [ ] v4 テーブル定義の SQLite 反映  

---

## フェーズ 1：Extract（HTML 取得）

### 1-1. トップ → カレンダー抽出
- [ ] TOP ページから JavaScript `doAction`/href を抽出  
- [ ] カレンダー月リンクを取得  
- [ ] 日付リンク（開催日ページ）を抽出

### 1-2. 開催日ページ → CNAME 取得
- [ ] 「○回東京○日」のリンクを抽出  
- [ ] href or onclick の CNAME を取得

### 1-3. レース一覧ページ → CNAME
- [ ] `<th class="race_num">` 内の `<a href="...CNAME=xxx">` を抽出  
- [ ] レース番号と CNAME を紐づけ

### 1-4. レース結果ページ取得
- [ ] GET（or POST）で `accessS.html?CNAME=xxxx`  
- [ ] HTML 保存（上書き禁止）

（根拠 → 白書 1,2,4 章） :contentReference[oaicite:12]{index=12}

---

## フェーズ 2：Transform（HTML → dict）
- [ ] レース概要パース  
- [ ] テーブルヘッダ自動検出  
- [ ] 行パース → race_results dict  
- [ ] 各 ID（horse/jockey/trainer）抽出  
- [ ] 数値項目クレンジング  
- [ ] 特殊空白 `\u00A0` 正規化  
- [ ] タイム変換（任意：秒変換）

（根拠 → 白書 5 章、etl_plan）  
:contentReference[oaicite:13]{index=13} :contentReference[oaicite:14]{index=14}

---

## フェーズ 3：Load（SQLite 挿入）
- [ ] 1 レース単位で Transaction  
- [ ] races：INSERT OR REPLACE  
- [ ] race_results：UPSERT  
- [ ] horses / jockeys / trainers：INSERT OR IGNORE  
- [ ] ログ保存

（根拠 → etl_plan 3 章） :contentReference[oaicite:15]{index=15}

---

## フェーズ 4：バッチ（ Step 5 前走リンク）
- [ ] 全レースの race_results を日付順に並べる  
- [ ] 馬ごとに prev_* を埋める  
- [ ] days_since_last の計算

（根拠 → etl_plan Step5） :contentReference[oaicite:16]{index=16}

---

# 4. ハイブリッド方式の実装方針

白書が結論として推奨する方式：

### **✔ race_id 管理 + 必要時だけリンク探索（ハイブリッド）**
- race_id は v4 の主キー  
- CNAME は毎回リンクから拾う（生成不能）  
- 余計なクロールを避けるため、  
**既知の開催日だけページを開く** のが最適

（根拠 → 白書 8 章） :contentReference[oaicite:17]{index=17}

---

# 5. 最低限の完成基準（MVP）

### ✔ 1レース ID を入力 → DB に完全保存  
（Extract → Transform → Load が通る）

必要項目：
- race（date, distance, surface, weather…）  
- race_results（finish, odds, weight, last3F…）  
- horses / jockeys / trainers（ID+name）

（根拠 → etl_plan Step1） :contentReference[oaicite:18]{index=18}

---

# 6. 今後の拡張
- course_id 自動推定  
- パドック情報（対象外）  
- 払戻金ページ（v4では対象外）  
- ML 用特徴量生成  
- DVC / MLflow によるデータバージョン管理

---

# まとめ
- **すべての Must データは結果ページ 1 枚から取得可能**  
- **CNAME は固定生成できない → ページ遷移必須**  
- **HTML 完全保存 → 再取得不要**  
- **v4 ETL を5段階で拡張しながら構築**
