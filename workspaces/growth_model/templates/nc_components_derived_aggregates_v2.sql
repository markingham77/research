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

                {{ mymacros.nc_derived_aggregates() }}

                from core_wip.nc_components_v2
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