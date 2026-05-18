with source as (

    select * from {{ source('bronze', 'fs220') }}

),

renamed as (

    select
        cu_number,
        cycle_date                              as reporting_date,

        -- balance sheet
        cast(acct_010 as bigint)                as total_assets,
        cast(acct_018 as bigint)                as total_shares_and_deposits,
        cast(acct_025b as bigint)               as total_loans,
        cast(acct_719 as bigint)                as allowance_for_loan_losses,
        cast(acct_860c as bigint)               as total_borrowings,

        -- income and profitability
        cast(acct_100 as bigint)                as total_gross_income,
        cast(acct_602 as bigint)                as net_income,
        cast(acct_380 as bigint)                as dividends_on_shares,
        cast(acct_300 as bigint)                as provision_for_loan_losses,

        -- credit quality
        cast(acct_041b as bigint)               as total_delinquent_loans,
        cast(acct_550 as bigint)                as total_charge_offs_ytd,
        cast(acct_551 as bigint)                as total_recoveries_ytd,

        -- membership
        cast(acct_083 as bigint)                as total_members,

        -- equity
        cast(acct_940 as bigint)                as undivided_earnings,
        cast(acct_931 as bigint)                as regular_reserves,

        quarter

    from source

)

select * from renamed