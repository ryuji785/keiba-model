# 競馬予測DB テーブル定義 v4（JRA公式サイトベース）

## 0. 方針

- 予測タスク  
  - 各レース・各馬ごとの「3着以内に入る確率 P(in_top3)」を予測する
- データソース  
  - **JRA公式サイトのレース結果ページのみ** を一次ソースとする
  - netkeiba 等、スクレイピング禁止が明示されているサイトは使わない
- 設計方針
  - 「1レース × 1頭 = 1レコード」の粒度を維持
  - **Must/Should の中でも JRA結果ページから自動取得できる項目だけをコアに採用**
  - 勝率・複勝率などの統計値は「statsテーブル（派生テーブル）」として後から追加する

---

## 1. courses テーブル（コースマスタ）

競馬場＋コース種別（芝/ダート、内/外など）のマスタ。  
基本的には **手入力 or 半手動メンテ** を想定。

```sql
CREATE TABLE courses (
    course_id      TEXT PRIMARY KEY,   -- 例: 'KYT_T_OUT_3000'
    venue_id       TEXT NOT NULL,      -- 例: 'KYT'（京都）
    course_name    TEXT NOT NULL,      -- 例: '京都 芝 外回り 3000m'
    surface        TEXT NOT NULL,      -- 'turf' / 'dirt'
    track_type     TEXT,               -- 'left' / 'right' / 'straight' / 'outer' etc.
    straight_len   INTEGER,            -- 直線距離(m) ※分かる範囲で
    slope_max      REAL,               -- 最大勾配(%) ※分かる範囲で
    features_text  TEXT                -- コースの特徴メモ（任意）
);
将来、courses を元に「コース適性」などの特徴量を作る想定

2024〜2025の中央競馬で使うコースだけ埋めればOK

2. races テーブル（レース情報）
JRAレース結果ページの「レース概要」から取得する情報。

sql
コードをコピーする
CREATE TABLE races (
    race_id       TEXT PRIMARY KEY,    -- 例: 'R2025KYT11'（独自ID）
    date          TEXT NOT NULL,       -- 開催日 'YYYY-MM-DD'
    course_id     TEXT NOT NULL,       -- courses.course_id
    race_no       INTEGER NOT NULL,    -- 1〜12（〇R）
    race_name     TEXT,                -- レース名（例: '第86回菊花賞'）
    distance      INTEGER NOT NULL,    -- 距離(m)
    surface       TEXT NOT NULL,       -- 'turf' / 'dirt'
    weather       TEXT,                -- 天候（晴 / 曇 / 雨 / 雪 など）
    going         TEXT,                -- 馬場状態（良 / 稍重 / 重 / 不良）
    class         TEXT,                -- クラス（新馬 / 未勝利 / 1勝 / OP / G3 / G2 / G1 等）
    age_cond      TEXT,                -- 年齢条件（テキストのまま保持: '3歳以上' など）
    sex_cond      TEXT,                -- 性別条件（'牝馬限定' など）
    num_runners   INTEGER,             -- 出走頭数
    win_time_sec  REAL,                -- 1着馬の走破タイム（秒）
    race_type     TEXT DEFAULT 'FLAT', -- 'FLAT' / 'JUMP'
    FOREIGN KEY(course_id) REFERENCES courses(course_id)
);
race_id は現行どおり「年＋場＋R」をベースに独自採番でOK

num_runners は race_results の件数からでも算出可能だが、冗長でも持っておく

3. horses テーブル（馬マスタ：コア）
現段階では 「このレースに出ている馬」を識別できればよい ので最低限。

sql
コードをコピーする
CREATE TABLE horses (
    horse_id     TEXT PRIMARY KEY,
    horse_name   TEXT NOT NULL,
    sex          TEXT,      -- '牡' / '牝' / 'セ' など
    birth_year   INTEGER    -- 馬齢から逆算できれば入れる（不明ならNULL）
);
現実的には、当面は race_id + 馬番 でユニークな horse_id を振る運用でも可
（例: 'R2025KYT11_H09'）

将来、本格運用するなら JRA固有の馬ID を採用できるようにリファクタする

4. jockeys テーブル（騎手マスタ）
sql
コードをコピーする
CREATE TABLE jockeys (
    jockey_id    TEXT PRIMARY KEY,   -- 当面は名前をそのままIDとして扱う
    jockey_name  TEXT NOT NULL
);
※ 今は jockey_id = jockey_name でOK。
　将来、JRAの騎手コード（リンクURL内のID）を使うように変えてもよい。

5. trainers テーブル（調教師マスタ）
sql
コードをコピーする
CREATE TABLE trainers (
    trainer_id    TEXT PRIMARY KEY,   -- 当面は名前をそのままIDとして扱う
    trainer_name  TEXT NOT NULL
);
※ ここも将来、JRAの調教師コードに置き換え可能な設計。

6. race_results テーブル（出走結果：コア）
1レコード = 1レース・1頭分の結果。

sql
コードをコピーする
CREATE TABLE race_results (
    race_id           TEXT NOT NULL,
    horse_id          TEXT NOT NULL,
    bracket_no        INTEGER,        -- 枠番
    horse_no          INTEGER,        -- 馬番
    finish_rank       INTEGER,        -- 着順（1,2,3... / 競走中止などはNULL＋finish_statusで表現）
    finish_status     TEXT,           -- 'OK','SCR','DNF','DQ' など
    finish_time_sec   REAL,           -- 走破タイム（秒）
    odds              REAL,           -- 単勝オッズ
    popularity        INTEGER,        -- 単勝人気（1=1番人気）
    weight            INTEGER,        -- 斤量(kg)
    weight_diff       INTEGER,        -- 馬体重増減(kg)
    body_weight       INTEGER,        -- 馬体重(kg)
    jockey_id         TEXT,
    trainer_id        TEXT,
    corner_pass_order TEXT,           -- コーナー通過順位（例: '3-3-2-1'）
    last_3f           REAL,           -- 上がり3Fタイム
    margin_sec        REAL,           -- 1着からの着差（秒換算）
    prize             REAL,           -- 本賞金（あれば）
    -- 前走情報（シンプル版）
    prev_race_id      TEXT,           -- 前走の race_id（分かる範囲で）
    prev_finish_rank  INTEGER,        -- 前走着順
    prev_margin_sec   REAL,           -- 前走着差（秒）
    prev_time_sec     REAL,           -- 前走タイム（秒）
    prev_last_3f      REAL,           -- 前走上がり3F
    days_since_last   INTEGER,        -- 前走からの間隔（日）
    PRIMARY KEY (race_id, horse_id),
    FOREIGN KEY (race_id)   REFERENCES races(race_id),
    FOREIGN KEY (horse_id)  REFERENCES horses(horse_id),
    FOREIGN KEY (jockey_id) REFERENCES jockeys(jockey_id),
    FOREIGN KEY (trainer_id)REFERENCES trainers(trainer_id)
);
Must で挙がっていた項目

finish_rank, finish_time_sec, odds, popularity, weight, horse_no, horse_id など

前走情報（prev_*）

Claude案にあった last_race_* のうち、
「とりあえず1レース分だけリンクすれば良い」ものを最低限入れている

最初のフェーズでは NULL でもよい（後からETLで埋められる構造にしておくイメージ）

7. ビュー：v_race_features（学習用結合ビュー）※論理設計
学習用に毎回 JOIN を書かなくてよいように、
主要テーブルを JOIN したビューを用意する想定。

sql
コードをコピーする
CREATE VIEW v_race_features AS
SELECT
    rr.race_id,
    r.date AS race_date,
    r.course_id,
    c.venue_id,
    c.course_name,
    r.race_no,
    r.race_name,
    r.distance,
    r.surface,
    r.weather,
    r.going,
    r.class,
    r.age_cond,
    r.sex_cond,
    r.num_runners,
    rr.horse_id,
    h.horse_name,
    h.sex AS horse_sex,
    h.birth_year,
    rr.bracket_no,
    rr.horse_no,
    rr.finish_rank,
    rr.finish_status,
    rr.finish_time_sec,
    rr.odds,
    rr.popularity,
    rr.weight,
    rr.weight_diff,
    rr.body_weight,
    rr.corner_pass_order,
    rr.last_3f,
    rr.margin_sec,
    rr.prize,
    j.jockey_name,
    t.trainer_name,
    rr.prev_race_id,
    rr.prev_finish_rank,
    rr.prev_margin_sec,
    rr.prev_time_sec,
    rr.prev_last_3f,
    rr.days_since_last
FROM race_results rr
JOIN races   r ON rr.race_id = r.race_id
JOIN horses  h ON rr.horse_id = h.horse_id
LEFT JOIN courses  c ON r.course_id = c.course_id
LEFT JOIN jockeys  j ON rr.jockey_id = j.jockey_id
LEFT JOIN trainers t ON rr.trainer_id = t.trainer_id;
Python 側では基本的に SELECT * FROM v_race_features を起点に
学習用の DataFrame を作る運用を想定。

8. 将来拡張テーブル（今は「定義だけ」）
8-1. horses_stats（馬の通算成績）
JRAから「直接」は取れないが、
race_results を集計すれば作れる統計値を入れるテーブル案。

sql
コードをコピーする
CREATE TABLE horses_stats (
    horse_id                 TEXT NOT NULL,
    stats_as_of_date         TEXT NOT NULL, -- この日までの成績
    career_starts            INTEGER,
    career_wins              INTEGER,
    career_places            INTEGER, -- 2着以内
    career_shows             INTEGER, -- 3着以内
    win_rate_turf            REAL,
    win_rate_dirt            REAL,
    win_rate_sprint          REAL, -- 〜1400m
    win_rate_mile            REAL, -- 1401〜1800m
    win_rate_middle          REAL, -- 1801〜2200m
    win_rate_long            REAL, -- 2201m〜
    show_rate_turf           REAL,
    show_rate_dirt           REAL,
    avg_finish_last5         REAL,
    PRIMARY KEY (horse_id, stats_as_of_date),
    FOREIGN KEY (horse_id) REFERENCES horses(horse_id)
);
8-2. jockeys_stats / trainers_stats
似た形で年単位の集計を置くイメージ。
実装は「全レース集計フェーズ」に入ってからでOK。

9. まとめ
この v4 スキーマは：

JRAレース結果ページのみを一次ソース としても自動 ETL が可能

予測タスクで Must/Should と定義した項目の 「生データ部分」 をほぼカバー

勝率・複勝率などの統計値は 別テーブルで後付け可能 な構造