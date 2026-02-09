with staging as (
    -- This is where we stack the 3 years together
    select * from {{ ref('stg_h1b_2025') }}
    union all
    select * from {{ ref('stg_h1b_2024') }}
    union all
    select * from {{ ref('stg_h1b_2023') }}
),

seeds as (
    select * from {{ ref('brand_mapping') }}
),

joined as (
    select
        s.*,
        b.clean_brand,
        -- Your Regex logic stays here to clean ALL years at once
        case 
            when regexp_contains(s.employer_name, concat(r'\b', b.keyword, r'\b')) 
            then b.clean_brand 
            else null 
        end as matched_brand
    from staging s
    left join seeds b 
        on regexp_contains(s.employer_name, concat(r'\b', b.keyword, r'\b'))
)

select 
    *,
    coalesce(matched_brand, employer_name) as final_employer_name
from joined