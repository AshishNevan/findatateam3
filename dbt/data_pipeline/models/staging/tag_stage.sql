{{
    config(
        materialized='view',
        alias='tag_stage'
    )
}}

WITH raw_tag AS (
    SELECT * FROM {{ source('raw', 'raw_tag') }}
)

SELECT
    CAST(TAG AS VARCHAR(256)) AS tag,
    CAST(VERSION AS VARCHAR(20)) AS version,
    CAST(CUSTOM AS BOOLEAN) AS custom,
    CAST(ABSTRACT AS BOOLEAN) AS abstract,
    CAST(DATATYPE AS VARCHAR(20)) AS datatype,
    CAST(IORD AS CHAR(1)) AS iord,
    CAST(CRDR AS CHAR(1)) AS crdr,
    CAST(TLABEL AS VARCHAR(512)) AS tlabel,
    CAST(DOC AS TEXT) AS doc
FROM raw_tag
WHERE
    TAG IS NOT NULL AND
    VERSION IS NOT NULL AND
    CUSTOM IS NOT NULL AND
    ABSTRACT IS NOT NULL AND
    DATATYPE IS NOT NULL AND
    IORD IS NOT NULL