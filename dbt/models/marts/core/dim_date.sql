-- Calendar spine covering the full range of ledger activity, so trend marts
-- never have gaps on days/months with zero transactions.
with bounds as (
    select
        min(cast(event_ts as date)) as min_date,
        max(cast(event_ts as date)) as max_date
    from {{ ref('stg_ledger_entries') }}
),

spine as (
    select unnest(generate_series(
        (select min_date from bounds),
        (select max_date from bounds),
        interval 1 day
    ))::date as date_day
)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    strftime(date_day, '%B') as month_name,
    date_trunc('month', date_day)::date as month_start,
    extract(day from date_day) as day_of_month,
    extract(dow from date_day) as day_of_week,
    strftime(date_day, '%A') as day_name,
    extract(dow from date_day) in (0, 6) as is_weekend
from spine
