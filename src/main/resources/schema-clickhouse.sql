CREATE TABLE IF NOT EXISTS athlete_data_gold (
    athlete_id String,
    activity_id String,
    heart_rate Float64,
    blood_pressure_sys Float64,
    oxygen_sat Float64,
    ts DateTime,
    consent_version Int32,
    consent_mask UInt64
) ENGINE = MergeTree()
ORDER BY (athlete_id, ts);
