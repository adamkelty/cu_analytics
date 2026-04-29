with credit_unions as (

    select * from {{ ref('stg_credit_unions') }}

),

financials as (

    select * from {{ ref('stg_financials') }}

),

peers as (

    select
        cu.cu_number,
        cu.cu_name,
        cu.city,
        cu.state,
        cu.peer_group,
        cu.year_opened,
        f.quarter,
        f.reporting_date,

        -- balance sheet
        f.total_assets,
        f.total_loans,
        f.total_shares_and_deposits,
        f.total_borrowings,
        f.allowance_for_loan_losses,
        f.total_members,

        -- profitability
        f.net_income,
        f.total_gross_income,
        f.provision_for_loan_losses,

        -- credit quality
        f.total_delinquent_loans,
        f.total_charge_offs_ytd,
        f.total_recoveries_ytd,

        -- calculated ratios
        round(f.total_loans / nullif(f.total_shares_and_deposits, 0) * 100, 2)  as loan_to_share_ratio,
        round(f.total_delinquent_loans / nullif(f.total_loans, 0) * 100, 2)     as delinquency_rate,
        round(f.total_charge_offs_ytd / nullif(f.total_loans, 0) * 100, 2)      as charge_off_rate,
        round(f.allowance_for_loan_losses / nullif(f.total_loans, 0) * 100, 2)  as reserve_coverage_ratio

    from credit_unions cu
    join financials f
        on cu.cu_number = f.cu_number

    where cu.cu_number in (
        159,    -- three rivers
        620,    -- power one
        1427,   -- midwest america
        4968,   -- inova
        5431,   -- fort financial
        7688,   -- partners 1st
        17012,  -- profed
        21593,  -- fire police city county
        24781,  -- urban beginnings choice
        64275   -- public service #3
    )

)

select * from peers
order by total_assets desc