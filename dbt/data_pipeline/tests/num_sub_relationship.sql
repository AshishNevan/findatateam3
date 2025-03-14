SELECT *
FROM {{ ref('num_stg') }} n
LEFT JOIN {{ ref('sub_stg') }} s
    ON n.submission_id = s.submission_id
WHERE s.submission_id IS NULL
    AND n.submission_id IS NOT NULL