{% import 'mymacros.jinja' as mymacros %}

{% set this = ({
        "min_query_date": "2023-11-01",
        "max_query_date": "2023-12-31",
    })
%}

select
    DATE_TRUNC(DAY,event_timestamp) as event_ds,
    region.major_market_partner major_market_partner,
    iff(is_member,'TRUE','FALSE') as is_member,
    user_type,
    {{ mymacros.session_traffic_source_grouping() }},
    {{ mymacros.nc_aggregates() }}

from union_touch_points
left join lyst_analytics.partner_reporting_major_market region
on user_geoip_country = region.country_code
where
    event_ds>'{{min_query_date|default(this.min_query_date)}}'
and    
    event_ds<'{{max_query_date|default(this.max_query_date)}}'
group by
    event_ds,
    session_traffic_source_grouping,
    major_market_partner,
    is_member,
    user_type
;
