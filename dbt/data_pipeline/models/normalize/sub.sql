{{
    config(
        materialized='table',
        unique_key='adsh'
    )
}}

WITH sub_stg AS (
    SELECT * FROM {{ ref('sub_stage') }}
)

SELECT
    adsh,
    cik,
    name,
    sic,
    countryba,
    stprba,
    cityba,
    zipba,
    fye,
    form,
    period,
    fy,
    fp,
    filed,
    accepted,
    prevrpt,
    detail,
    afs,
    wksi,
    nciks,
    CASE 
        WHEN fp = 'FY' THEN fy
        ELSE fy || '' || RIGHT('00' || CAST(SUBSTRING(fp, 2) AS VARCHAR), 2)
    END AS fiscal_period_id
FROM sub_stg