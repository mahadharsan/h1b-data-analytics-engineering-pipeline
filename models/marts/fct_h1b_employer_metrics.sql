with branded_data as (
    select * from {{ ref('int_h1b_branded') }}
)

select
    -- We use the FINAL_EMPLOYER_NAME we created in the last step
    final_employer_name as employer_name,
    fiscal_year,
    state,
    
    -- Summing up all the different approval types
    sum(new_emp_app + cont_app + same_emp_app + change_emp_app + amended_app) as total_approvals,
    
    -- Summing up all denials
    sum(new_emp_den + cont_den + same_emp_den + change_emp_den + amended_den) as total_denials,
    
    -- Total Applications
    sum(new_emp_app + cont_app + same_emp_app + change_emp_app + amended_app + 
        new_emp_den + cont_den + same_emp_den + change_emp_den + amended_den) as total_applications,

    -- Calculating the Success Rate (Approval Rate)
    -- We use safe_divide to avoid 'Division by Zero' errors if applications are 0
    safe_divide(
        sum(new_emp_app + cont_app + same_emp_app + change_emp_app + amended_app),
        sum(new_emp_app + cont_app + same_emp_app + change_emp_app + amended_app + 
            new_emp_den + cont_den + same_emp_den + change_emp_den + amended_den)
    ) * 100 as approval_percentage

from branded_data
group by 1, 2, 3
order by total_applications desc