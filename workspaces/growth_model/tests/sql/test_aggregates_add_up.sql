{# {% test aggregates_add_up(metric, metric_type, dimensions, min_test_date, max_query_date, freq) %} #}

{% set this = ({
        "min_test_date": "2023-11-01",
        "max_test_date": "2023-12-31",
        "metric": "user_count",
        "metric_type": "count",
        "freq":"MONTH",
        "dimensions": ['session_traffic_source_grouping']
    })
%}


select 
    event_ds
    ,CASE
        WHEN 
            {% if metric_type|default(this.metric_type)|upper in ('COUNT','SUM') %}
                sum(CASE
                    WHEN session_traffic_source_grouping='ALL' THEN 0
                    ELSE
                        {{metric|default(this.metric)}}
                    END)
                - sum(CASE
                    WHEN session_traffic_source_grouping='ALL' THEN {{metric|default(this.metric)}}
                    ELSE
                        0
                    END) = 0 THEN false
            {% else %}
                {{ metric_type|default(this.metric_type) }}
            {% endif %}
        ELSE
            true
        END as valid    
from nc_components_derived_aggregates 
where period_ds_freq='{{ freq|default(this.freq) }}'
and major_market_partner='ALL'
and is_member='ALL'
and user_type='ALL'
-- and session_traffic_source_grouping='ALL'
group by 
    event_ds
having
    valid=true
;

{# {% endtest %} #}