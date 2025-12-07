-- Feature views for modeling (v4 schema)

-- Race + result enriched (joins horses/jockeys/trainers)
CREATE VIEW IF NOT EXISTS v_race_results_enriched AS
SELECT
    rr.race_id,
    r.date,
    r.course_id,
    r.race_no,
    r.distance,
    r.surface,
    r.weather,
    r.going,
    r.class,
    r.age_cond,
    r.sex_cond,
    rr.horse_id,
    h.horse_name,
    h.sex,
    rr.bracket_no,
    rr.horse_no,
    rr.finish_rank,
    rr.finish_status,
    rr.finish_time_sec,
    rr.margin_sec,
    rr.corner_pass_order,
    rr.last_3f,
    rr.odds,
    rr.popularity,
    rr.weight,
    rr.body_weight,
    rr.weight_diff,
    rr.jockey_id,
    j.jockey_name,
    rr.trainer_id,
    t.trainer_name,
    rr.prev_race_id,
    rr.prev_finish_rank,
    rr.prev_margin_sec,
    rr.prev_time_sec,
    rr.prev_last_3f,
    rr.days_since_last,
    r.win_time_sec
FROM race_results rr
JOIN races   r ON r.race_id   = rr.race_id
LEFT JOIN horses   h ON h.horse_id   = rr.horse_id
LEFT JOIN jockeys  j ON j.jockey_id  = rr.jockey_id
LEFT JOIN trainers t ON t.trainer_id = rr.trainer_id;

-- Imputed variants for last_3f/margin: simple fill with race medians
CREATE VIEW IF NOT EXISTS v_race_results_imputed AS
WITH agg AS (
    SELECT
        race_id,
        median(last_3f)  AS med_last_3f,
        median(margin_sec) AS med_margin
    FROM race_results
    GROUP BY race_id
)
SELECT
    e.*,
    COALESCE(e.last_3f, a.med_last_3f)   AS last_3f_imputed,
    COALESCE(e.margin_sec, a.med_margin) AS margin_sec_imputed,
    (e.last_3f IS NULL)   AS last_3f_missing,
    (e.margin_sec IS NULL) AS margin_missing
FROM v_race_results_enriched e
LEFT JOIN agg a ON a.race_id = e.race_id;
