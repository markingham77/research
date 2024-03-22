select 
    'FYTD' as PERIOD_DS_FREQ,
    date_from_parts(2023,4,1) as event_ds,
    -- min(event_ds) as event_ds,
    -- 2024 as FY,
    -- year(max(to_date(event_ds,'YYYY-MM-DD:HH24-MI-SS'))) as FY,
    MAJOR_MARKET_PARTNER,
    IS_MEMBER,
    USER_TYPE,
    SESSION_TRAFFIC_SOURCE_GROUPING,
    sum(user_count) as user_count,
    sum(session_count) as session_count,
    sum(session_count)/nullif(sum(user_count),0) as sessions_per_user,
    sum(lead_generation_count) as lead_generation_count,
    sum(lead_generation_count)/nullif(sum(user_count),0) as lead_generation_rate,
    sum(conversion_count) as conversion_count,
    sum(conversion_count)/nullif(sum(lead_generation_count),0) as track_conversion_rate,
    sum(order_count) as order_count,
    sum(order_count)/nullif(sum(conversion_count),0) as order_count_per_conversion,     
    sum(AOV*nullif(order_count,0))/nullif(sum(order_count),0) as AOV,
    sum(cpa_rate * nullif(AOV*order_count,0))/nullif(sum(AOV*nullif(order_count,0)),0) as cpa_rate,
    sum(retain_rate*nullif(cpa_rate * nullif(AOV*order_count,0),0))/nullif(sum(cpa_rate * nullif(AOV*order_count,0)),0) as retain_rate,
    sum(GMV) as GMV,
    sum(gross_commission) as gross_commission,
    sum(net_commission) as net_commission    
from nc_components_derived_aggregates_v2
where 
    PERIOD_DS_FREQ='WEEK'
and event_ds>'2023-03-31'
and event_ds<'2024-01-23'     
group by
    MAJOR_MARKET_PARTNER,
    IS_MEMBER,
    USER_TYPE,
    SESSION_TRAFFIC_SOURCE_GROUPING

UNION ALL

select 
    'FYTD' as PERIOD_DS_FREQ,
    date_from_parts(2022,4,1) as event_ds,
    -- date_from_parts(2023,4,1) as event_ds,
    -- min(event_ds) as event_ds,
    -- 2023 as FY,
    -- year(max(to_date(event_ds,'YYYY-MM-DD:HH24-MI-SS'))) as FY,
    -- 'FY' || year(max(to_date(event_ds,'YYYY-MM-DD:HH24-MI-SS'))) || ':W0-W' || datediff(WEEK,min(to_date(event_ds,'YYYY-MM-DD:HH24-MI-SS')),max(to_date(event_ds,'YYYY-MM-DD:HH24-MI-SS'))) as PERIOD,    
    MAJOR_MARKET_PARTNER,
    IS_MEMBER,
    USER_TYPE,
    SESSION_TRAFFIC_SOURCE_GROUPING,
    sum(user_count) as user_count,
    sum(session_count) as session_count,
    sum(session_count)/nullif(sum(user_count),0) as sessions_per_user,
    sum(lead_generation_count) as lead_generation_count,
    sum(lead_generation_count)/nullif(sum(user_count),0) as lead_generation_rate,
    sum(conversion_count) as conversion_count,
    sum(conversion_count)/nullif(sum(lead_generation_count),0) as track_conversion_rate,
    sum(order_count) as order_count,
    sum(order_count)/nullif(sum(conversion_count),0) as order_count_per_conversion,     
    sum(AOV*nullif(order_count,0))/nullif(sum(order_count),0) as AOV,
    sum(cpa_rate * nullif(AOV*order_count,0))/nullif(sum(AOV*nullif(order_count,0)),0) as cpa_rate,
    sum(retain_rate*nullif(cpa_rate * nullif(AOV*order_count,0),0))/nullif(sum(cpa_rate * nullif(AOV*order_count,0)),0) as retain_rate,
    sum(GMV) as GMV,
    sum(gross_commission) as gross_commission,
    sum(net_commission) as net_commission    
from nc_components_derived_aggregates_v2
where 
    PERIOD_DS_FREQ='WEEK'
and event_ds>'2022-03-31'
and event_ds<'2023-01-24'     
group by
    MAJOR_MARKET_PARTNER,
    IS_MEMBER,
    USER_TYPE,
    SESSION_TRAFFIC_SOURCE_GROUPING    

UNION ALL


select 
    'FYTD' as PERIOD_DS_FREQ,
    date_from_parts(2021,4,1) as event_ds,
    -- date_from_parts(2023,4,1) as event_ds,
    -- min(event_ds) as event_ds,
    -- 2023 as FY,
    -- year(max(to_date(event_ds,'YYYY-MM-DD:HH24-MI-SS'))) as FY,
    -- 'FY' || year(max(to_date(event_ds,'YYYY-MM-DD:HH24-MI-SS'))) || ':W0-W' || datediff(WEEK,min(to_date(event_ds,'YYYY-MM-DD:HH24-MI-SS')),max(to_date(event_ds,'YYYY-MM-DD:HH24-MI-SS'))) as PERIOD,    
    MAJOR_MARKET_PARTNER,
    IS_MEMBER,
    USER_TYPE,
    SESSION_TRAFFIC_SOURCE_GROUPING,
    sum(user_count) as user_count,
    sum(session_count) as session_count,
    sum(session_count)/nullif(sum(user_count),0) as sessions_per_user,
    sum(lead_generation_count) as lead_generation_count,
    sum(lead_generation_count)/nullif(sum(user_count),0) as lead_generation_rate,
    sum(conversion_count) as conversion_count,
    sum(conversion_count)/nullif(sum(lead_generation_count),0) as track_conversion_rate,
    sum(order_count) as order_count,
    sum(order_count)/nullif(sum(conversion_count),0) as order_count_per_conversion,     
    sum(AOV*nullif(order_count,0))/nullif(sum(order_count),0) as AOV,
    sum(cpa_rate * nullif(AOV*order_count,0))/nullif(sum(AOV*nullif(order_count,0)),0) as cpa_rate,
    sum(retain_rate*nullif(cpa_rate * nullif(AOV*order_count,0),0))/nullif(sum(cpa_rate * nullif(AOV*order_count,0)),0) as retain_rate,
    sum(GMV) as GMV,
    sum(gross_commission) as gross_commission,
    sum(net_commission) as net_commission    
from nc_components_derived_aggregates_v2
where 
    PERIOD_DS_FREQ='WEEK'
and event_ds>'2021-03-31'
and event_ds<'2022-01-24'     
group by
    MAJOR_MARKET_PARTNER,
    IS_MEMBER,
    USER_TYPE,
    SESSION_TRAFFIC_SOURCE_GROUPING    

UNION ALL
   
select 
    'FYTD' as PERIOD_DS_FREQ,
    date_from_parts(2021,4,1) as event_ds,
    -- date_from_parts(2023,4,1) as event_ds,
    -- min(event_ds) as event_ds,
    -- 2023 as FY,
    -- year(max(to_date(event_ds,'YYYY-MM-DD:HH24-MI-SS'))) as FY,
    -- 'FY' || year(max(to_date(event_ds,'YYYY-MM-DD:HH24-MI-SS'))) || ':W0-W' || datediff(WEEK,min(to_date(event_ds,'YYYY-MM-DD:HH24-MI-SS')),max(to_date(event_ds,'YYYY-MM-DD:HH24-MI-SS'))) as PERIOD,    
    MAJOR_MARKET_PARTNER,
    IS_MEMBER,
    USER_TYPE,
    SESSION_TRAFFIC_SOURCE_GROUPING,
    sum(user_count) as user_count,
    sum(session_count) as session_count,
    sum(session_count)/nullif(sum(user_count),0) as sessions_per_user,
    sum(lead_generation_count) as lead_generation_count,
    sum(lead_generation_count)/nullif(sum(user_count),0) as lead_generation_rate,
    sum(conversion_count) as conversion_count,
    sum(conversion_count)/nullif(sum(lead_generation_count),0) as track_conversion_rate,
    sum(order_count) as order_count,
    sum(order_count)/nullif(sum(conversion_count),0) as order_count_per_conversion,     
    sum(AOV*nullif(order_count,0))/nullif(sum(order_count),0) as AOV,
    sum(cpa_rate * nullif(AOV*order_count,0))/nullif(sum(AOV*nullif(order_count,0)),0) as cpa_rate,
    sum(retain_rate*nullif(cpa_rate * nullif(AOV*order_count,0),0))/nullif(sum(cpa_rate * nullif(AOV*order_count,0)),0) as retain_rate,
    sum(GMV) as GMV,
    sum(gross_commission) as gross_commission,
    sum(net_commission) as net_commission    
from nc_components_derived_aggregates_v2
where 
    PERIOD_DS_FREQ='WEEK'
and event_ds>'2020-03-31'
and event_ds<'2021-01-24'     
group by
    MAJOR_MARKET_PARTNER,
    IS_MEMBER,
    USER_TYPE,
    SESSION_TRAFFIC_SOURCE_GROUPING    

    