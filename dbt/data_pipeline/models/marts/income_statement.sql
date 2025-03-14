SELECT 
    num.adsh as adsh,
    sub.cik,
    sub.name AS company_name,
    NULL AS ticker, 
    sub.sic,
    sub.filed AS filing_date,
    sub.fy AS fiscal_year,
    sub.fp AS fiscal_period,
    num.tag as tag,
    tag.tlabel AS description,
    num.ddate,
    num.value,
    num.uom,
    num.segments AS segment,
    sub.form AS source
FROM {{ref('num_stage')}} as num
JOIN {{ref('sub_stage')}} as sub ON num.adsh = sub.adsh
JOIN {{ref('tag_stage')}} as tag ON num.tag = tag.tag
JOIN {{ref('pre_stage')}} as pre ON num.adsh = pre.adsh AND num.tag = pre.tag
WHERE pre.stmt = 'IS'