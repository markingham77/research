{% import 'mymacros.jinja' as mymacros %}

{% set this = ({
        "min_query_date": "2023-11-01",
        "max_query_date": "2024-03-01",
        "freq": ["DAY","WEEK","MONTH",'QUARTER'],
        "dimensions": ["major_market_partner", "is_member", "user_type", "session_traffic_source_grouping"]
    })
%}


{% for f in freq|default(this.freq) %}
    {% for k_combo in dimensions|default(this.dimensions)|dqt_combinations() %}
            {# {{k_combo}}, #}
            {% for combo in k_combo %}
            select
                '{{f}}' PERIOD_DS_FREQ,   
                DATE_TRUNC({{f}}, TO_DATE(TO_TIMESTAMP(event_ds, 'YYYY-MM-DD:HH24-MI-SS'))) as event_ds,
                {# DATE_TRUNC({{f}},event_ds) as event_ds,  #}
            {# each combo should result in one select block #}
                {% for d in dimensions|default(this.dimensions) %}
                    {# {{combo}} #}
                    {% if d in combo %}
                    'ALL' {{d}},
                    {% else %}
                    {{d}},
                    {% endif %}
                {% endfor %}    

                {{ mymacros.nc_derived_aggregates() }}

                from core_wip.nc_components_base
                where
                    event_ds>'{{min_query_date|default(this.min_query_date)}}'
                and    
                    event_ds<'{{max_query_date|default(this.max_query_date)}}' 
                group by
                    {% if f != 'DAY' %}
                        PERIOD_DS_FREQ,
                    {% endif %}    
                    event_ds

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