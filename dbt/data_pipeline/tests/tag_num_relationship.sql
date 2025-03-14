SELECT *
FROM {{ ref('num_stg') }} n
LEFT JOIN {{ ref('tag_stg') }} t
    ON n.tag = t.tag 
    AND n.taxonomy_version = t.taxonomy_version
WHERE t.tag IS NULL
    AND n.tag IS NOT NULL