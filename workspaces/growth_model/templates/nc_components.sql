{% set this = ({
        "min_query_date": "2023-11-01",
        "max_query_date": "2023-12-31",
        "freq": ["WEEK","MONTH",'QUARTER']
    })
%}

{% for f in freq|default(this.freq) %}
    select
        '{{f}}' as PERIOD_DS_FREQ, 
        DATE_TRUNC({{f}},event_timestamp) as event_ds,
        region.major_market_partner major_market_partner,
        is_member,
        user_type,
        CASE
            WHEN (session_traffic_source) = 'PLA-CO' THEN 'PLA-CO'
            WHEN (session_traffic_source) like 'PLA-EC' THEN 'PLA-EC'
            WHEN (session_traffic_source) = 'APP PAID' THEN 'APP PAID'
            WHEN (session_traffic_source) = 'DIRECT' THEN 'DIRECT'
            WHEN ((session_traffic_source) IN ('ORGANIC', 'EMAIL', 'SEO',   'SELF REFERRER', 'SOCIAL', 'OTHER SEO', 'REFERRAL', 'WEB TO APP', 'APP INDEXING', 'APP DIRECT - APP INDEXING', 'PUSH')
            OR (session_traffic_source) IS NULL) THEN 'ORGANIC'
            WHEN (session_traffic_source) IN ('PAID','OTHER PAID', 'PAID SOCIAL') THEN 'PAID'
            WHEN (session_traffic_source) = 'CSS' THEN 'CSS'
        ELSE 'OTHER'
        END as session_traffic_source_grouping,
        count(distinct(ultimate_id)) as user_count,
        count(distinct(ultimate_session_id)) as session_count,
        count(distinct(purchase_intent_id)) as lead_generation_count,
        sum(converted_track) as conversion_count,
        sum(order_count) as order_count,
        sum(order_count)/nullif(sum(converted_track),0) as order_count_per_conversion,
        sum(gross_amount)/nullif(sum(order_count),0) as AOV,
        sum(gross_commission)/nullif(sum(gross_amount),0) as cpa_rate,
        sum(net_commission)/nullif(sum(gross_commission),0) as retain_rate,
        sum(gross_amount) as GMV,
        sum(gross_commission) as gross_commission,
        sum(net_commission) as net_commission

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
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
;
