{{
    config(
        materialized='table',
        unique_key=['tag', 'version']
    )
}}

WITH tag_stg AS (
    SELECT * FROM {{ ref('tag_stage') }}
)

SELECT
    tag,
    version,
    custom,
    abstract,
    datatype,
    iord,
    crdr,
    tlabel,
    doc,
    CASE
        WHEN custom THEN 'Custom'
        ELSE 'Standard'
    END AS tag_type
FROM tag_stg