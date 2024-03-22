{% set this = ({
        "min_query_date": "2023-11-01",
        "max_query_date": "2023-12-31",
        "freq": ["WEEK","MONTH",'QUARTER'],
        "dimensions": ["major_market_partner", "is_member", "user_type", "session_traffic_source_grouping"]
    })
%}


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
;