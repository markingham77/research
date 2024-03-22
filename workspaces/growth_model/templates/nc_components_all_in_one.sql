{% set this = ({
        "min_query_date": "2023-11-01",
        "max_query_date": "2023-12-31",
        "freq": ["WEEK","MONTH",'QUARTER'],
        "dimensions": ["major_market_partner", "is_member", "user_type", "session_traffic_source_grouping"]        
    })
%}

with stage0 as (
{% for f in freq|default(this.freq) %}
    select
        '{{f}}' as PERIOD_DS_FREQ, 
        DATE_TRUNC({{f}},event_timestamp) as event_ds,
        region.major_market_partner major_market_partner,
        cast(is_member as varchar(256)) as is_member,
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
),
stage1 as (
{% for f in freq|default(this.freq) %}
    {% for k_combo in dimensions|default(this.dimensions)|dqt_combinations() %}     
            {# {{k_combo}}, #}
            {% for combo in k_combo %}
            select
                PERIOD_DS_FREQ,   
                event_ds, 
            {# each combo should result in one select block #}
                {% for d in dimensions|default(this.dimensions) %}            
                    {# {{combo}} #}
                    {% if d in combo %}
                    'ALL' {{d}},
                    {% else %}
                    {{d}},
                    {% endif %}
                {% endfor %}    

                    
                    sum(user_count) as user_count,
                    sum(session_count) as session_count,
                    sum(lead_generation_count) as lead_generation_count,
                    sum(conversion_count) as conversion_count,
                    sum(order_count) as order_count,
                    sum(order_count)/nullif(sum(conversion_count),0) as order_count_per_conversion,            
                    sum(AOV*nullif(order_count,0))/nullif(sum(order_count),0) as AOV,
                    sum(cpa_rate * nullif(AOV*order_count,0))/nullif(sum(AOV*nullif(order_count,0)),0) as cpa_rate,
                    sum(retain_rate*nullif(cpa_rate * nullif(AOV*order_count,0),0))/nullif(sum(cpa_rate * nullif(AOV*order_count,0)),0) as retain_rate,
                    sum(AOV*order_count) as GMV,
                    sum(cpa_rate * nullif(AOV*order_count,0)) as gross_commission,
                    sum(retain_rate*nullif(cpa_rate * nullif(AOV*order_count,0),0)) as net_commission
                from core_wip.nc_components
                where
                    event_ds>'{{min_query_date|default(this.min_query_date)}}'
                and    
                    event_ds<'{{max_query_date|default(this.max_query_date)}}' 
                and
                    PERIOD_DS_FREQ = '{{f}}'           
                group by
                    PERIOD_DS_FREQ
                    ,event_ds

                {% for d in dimensions|default(this.dimensions) %}            
                    {# {{combo}} #}
                    {% if d not in combo %}                    
                        ,{{d}}
                    {% endif %}
                {% endfor %}  

                {% if not loop.last %}
                UNION ALL
                {% endif %}     
            {% endfor %} 
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
)
select * from stage0
UNION ALL
select * from stage1;

