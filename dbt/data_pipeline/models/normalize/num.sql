{{
    config(
        materialized='table',
        unique_key=['adsh', 'tag', 'version', 'ddate', 'qtrs', 'uom', 'segments', 'coreg']
    )
}}

WITH num_stg AS (
    SELECT * FROM {{ ref('num_stage') }}
)

SELECT
    n.adsh,
    n.tag AS num_tag,  --Aliased to avoid duplication
    n.version,
    n.ddate,
    n.qtrs,
    n.uom,
    n.segments,
    n.coreg,
    n.value,
    n.footnote,
    t.tag AS tag_stage_tag,  --Aliased to avoid duplication
    t.datatype,
    t.crdr,
    t.abstract,
    s.fy,
    s.fp
FROM num_stg n
JOIN {{ ref('tag_stage') }} t 
    ON n.tag = t.tag 
    AND n.version = t.version
JOIN {{ ref('sub_stage') }} s 
    ON n.adsh = s.adsh