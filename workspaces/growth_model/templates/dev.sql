{% import 'mymacros.jinja' as mymacros %}

{% set this = ({
        "min_query_date": "2023-11-01",
        "max_query_date": "2024-03-01",
        "dimensions": ["major_market_partner", "is_member", "user_type", "session_traffic_source_grouping"],
        "metrics": ["user_count","session_count","lead_generation_count","conversion_count","order_count","GMV","gross_commission","net_commission"]

    })
%}


select
TO_DATE(TO_TIMESTAMP(event_ds, 'YYYY-MM-DD:HH24-MI-SS')) as event_ds,
{# DATE_TRUNC({{f}},event_ds) as event_ds,  #}
{# each combo should result in one select block #}
{% for d in dimensions|default(this.dimensions) %}
    {{d}},
{% endfor %}    

{% for metric in items|default(this.metrics) %}
    sum({{metric}}) over (
        partition by 
        
        {% for d in dimensions|default(this.dimensions) %}                                                       
                {{d}}
                {% if not loop.last %},{% endif %}
        {% endfor %}  
        order by event_ds
        range unbounded preceding
    ) cumm_{{metric}}
    {% if not loop.last %},{% endif %}
{% endfor %}

from core_wip.nc_components_derived_aggregates_from_base
where
    PERIOD_DS_FREQ='DAY'
and    
    event_ds>'{{min_query_date|default(this.min_query_date)}}'
and    
    event_ds<'{{max_query_date|default(this.max_query_date)}}' 
;