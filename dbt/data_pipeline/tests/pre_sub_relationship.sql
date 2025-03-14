SELECT *
FROM {{ ref('pre_stg') }} p
LEFT JOIN {{ ref('sub_stg') }} s
    ON p.submission_id = s.submission_id
WHERE s.submission_id IS NULL
    AND p.submission_id IS NOT NULL