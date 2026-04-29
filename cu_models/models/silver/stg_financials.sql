with source as (

    select * from {{ source('bronze', 'fs220') }}

),

renamed as (

    select
        cu_number,
        cycle_date                  as reporting_date,

        -- balance sheet
        acct_010                    as total_assets,
        acct_018                    as total_shares_and_deposits,
        acct_025b                   as total_loans,
        acct_719                    as allowance_for_loan_losses,
        acct_860c                   as total_borrowings,

        -- income and profitability
        acct_100                    as total_gross_income,
        acct_602                    as net_income,
        acct_380                    as dividends_on_shares,
        acct_300                    as provision_for_loan_losses,

        -- credit quality
        acct_041b                   as total_delinquent_loans,
        acct_550                    as total_charge_offs_ytd,
        acct_551                    as total_recoveries_ytd,

        -- membership
        acct_083                    as total_members,

        -- equity
        acct_940                    as undivided_earnings,
        acct_931                    as regular_reserves,

        quarter

    from source

)

select * from renamed