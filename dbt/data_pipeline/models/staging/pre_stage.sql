{{
    config(
        materialized='view',
        alias='pre_stage'
    )
}}

WITH raw_pre AS (
    SELECT * FROM {{ source('raw', 'raw_pre') }}
)

SELECT
    CAST(ADSH AS VARCHAR(20)) AS adsh,
    CAST(REPORT AS INTEGER) AS report,
    CAST(LINE AS INTEGER) AS line,
    CAST(STMT AS VARCHAR(2)) AS stmt,
    CAST(INPTH AS BOOLEAN) AS inpth,
    CAST(RFILE AS CHAR(1)) AS rfile,
    CAST(TAG AS VARCHAR(256)) AS tag,
    CAST(VERSION AS VARCHAR(20)) AS version,
    CAST(PLABEL AS VARCHAR(512)) AS plabel,
    CAST(NEGATING AS BOOLEAN) AS negating
FROM raw_pre
WHERE
    ADSH IS NOT NULL AND
    REPORT IS NOT NULL AND
    LINE IS NOT NULL AND
    STMT IS NOT NULL AND
    INPTH IS NOT NULL AND
    RFILE IS NOT NULL AND
    TAG IS NOT NULL AND
    VERSION IS NOT NULL AND
    PLABEL IS NOT NULL