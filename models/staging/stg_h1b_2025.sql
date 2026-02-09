with source as (
    select * from {{ source('h1b_staged_data', 'silver_h1b_2025') }}
)

select
    -- 1. Identity (Passing through Spark-cleaned names)
    employer_name,
    cast(fiscal_year as int64) as fiscal_year,
    industry_naics_code,
    
    -- 2. Fixing the Decimal Bug for Zip and Tax ID
    -- split_part looks for the '.' and takes the first part (the integer)
    split(cast(zip_code as string), '.')[OFFSET(0)] as zip_code,
    split(cast(tax_id as string), '.')[OFFSET(0)] as tax_id,

    -- 3. Geography
    upper(city) as city,
    upper(state) as state,

    -- 4. Approval/Denial Metrics (Already cast in Spark, but we cast again for safety)
    cast(new_employment_approval as int64) as new_emp_app,
    cast(new_employment_denial as int64) as new_emp_den,
    cast(continuation_approval as int64) as cont_app,
    cast(continuation_denial as int64) as cont_den,
    cast(same_employer_approval as int64) as same_emp_app,
    cast(same_employer_denial as int64) as same_emp_den,
    cast(change_of_employer_approval as int64) as change_emp_app,
    cast(change_of_employer_denial as int64) as change_emp_den,
    cast(amended_approval as int64) as amended_app,
    cast(amended_denial as int64) as amended_den

from source