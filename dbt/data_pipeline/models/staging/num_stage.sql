{{
    config(
        materialized='view',
        alias='num_stage'
    )
}}

WITH raw_num AS (
    SELECT * FROM {{ source('raw', 'raw_num') }}
)

SELECT
    CAST(ADSH AS VARCHAR(20)) AS adsh,
    CAST(TAG AS VARCHAR(256)) AS tag,
    CAST(VERSION AS VARCHAR(20)) AS version,
    TO_DATE(DDATE::TEXT, 'YYYYMMDD')::DATE AS ddate,
    CAST(QTRS AS NUMERIC) AS qtrs,
    CAST(UOM AS VARCHAR(20)) AS uom,
    CAST(SEGMENTS AS VARCHAR(1024)) AS segments,
    CAST(COREG AS NUMERIC) AS coreg,
    CAST(VALUE AS DECIMAL(28,4)) AS value,
    CAST(FOOTNOTE AS VARCHAR(512)) AS footnote
FROM raw_num
WHERE
    ADSH IS NOT NULL AND
    TAG IS NOT NULL AND
    VERSION IS NOT NULL AND
    DDATE IS NOT NULL AND
    QTRS IS NOT NULL AND
    UOM IS NOT NULL