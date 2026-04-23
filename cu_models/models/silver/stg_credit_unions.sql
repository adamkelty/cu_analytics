with source as (

    select * from {{ source('bronze', 'foicu') }}

),

renamed as (

    select
        cu_number,
        cu_name,
        lower(city)             as city,
        state,
        zip_code,
        lower(street)           as address,
        year_opened,
        peer_group,
        tom_code,
        ismdi                   as is_mdi,
        cycle_date              as reporting_date,
        quarter

    from source

)

select * from renamed