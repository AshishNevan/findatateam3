{{
    config(
        materialized='table',
        unique_key=['adsh', 'report', 'line']
    )
}}

WITH pre_stg AS (
    SELECT * FROM {{ ref('pre_stage') }}
)

SELECT
    p.adsh,
    p.report,
    p.line,
    p.stmt,
    p.inpth,
    p.rfile,
    p.tag AS pre_tag,  -- Aliased to avoid duplication
    p.version,
    p.plabel,
    p.negating,
    t.tag AS tag_stage_tag,  -- Aliased to avoid duplication
    t.datatype,
    s.fy,
    s.fp
FROM pre_stg p
JOIN {{ ref('tag_stage') }} t 
    ON p.tag = t.tag 
    AND p.version = t.version
JOIN {{ ref('sub_stage') }} s 
    ON p.adsh = s.adsh