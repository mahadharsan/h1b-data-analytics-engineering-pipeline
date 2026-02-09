with staging as (
    -- This is where we stack the 3 years together
    select * from {{ ref('stg_h1b_2025') }}
    union all
    select * from {{ ref('stg_h1b_2024') }}
    union all
    select * from {{ ref('stg_h1b_2023') }}
),

-- NEW: Cleaning Layer to strip suffixes automatically
cleaned_staging as (
    select 
        *,
        trim(regexp_replace(upper(employer_name), 
            r'\b(INC|LLC|L L C|CORP|CORPORATION|LTD|LIMITED|N A|NA|NATIONAL ASSOCIATION|CO|AND CO|COMPANY|AND COMPANY|SERVICES|TECHNOLOGIES|SOLUTIONS)\b\.?$', 
            ''
        )) as standardized_name
    from staging
),

seeds as (
    select * from {{ ref('brand_mapping') }}
),

joined as (
    select
        s.*,
        b.clean_brand,
        -- Check against your manual mapping first
        case 
            when regexp_contains(s.standardized_name, concat(r'\b', b.keyword, r'\b')) 
            then b.clean_brand 
            else null 
        end as matched_brand
    from cleaned_staging s
    left join seeds b 
        on regexp_contains(s.standardized_name, concat(r'\b', b.keyword, r'\b'))
)

select 
    *,
    -- This is your safety net to ensure no NULLs ever reach the Fact table
    case 
        when matched_brand is not null then matched_brand
        when standardized_name is not null and standardized_name != '' then standardized_name
        else 'OTHER/UNMAPPED' 
    end as final_employer_name
from joined