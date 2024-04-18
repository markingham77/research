{% set my_vars = ({
        "freq": "WEEK",
        "lag": "0",
        "n_periods": "52",
        "metrics": ['SESSION_COUNT','USER_COUNT','SESSION_PER_USER','PURCHASE_INTENT_COUNT','CONVERTED_PURCHASE_INTENT_COUNT','ORDER_COUNT','AOV_USD']
    })
%}


select
    MAJOR_MARKET,
    IS_MEMBER,
    USER_TYPE,
    SESSION_TRAFFIC_SOURCE_GROUPING,
    {% for metric in my_vars.metrics %}
        NULLIF(first_value({{metric}}) OVER
        (PARTITION BY MAJOR_MARKET, IS_MEMBER, USER_TYPE, SESSION_TRAFFIC_SOURCE_GROUPING ORDER BY EVENT_DATE 
        RANGE BETWEEN UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING),0) as starting_value_{{metric}},
        {# NULLIF(last_value({{metric}}) OVER
        (PARTITION BY MAJOR_MARKET, IS_MEMBER, USER_TYPE, SESSION_TRAFFIC_SOURCE_GROUPING ORDER BY EVENT_DATE 
        RANGE BETWEEN UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING),0) as ending_value_{{metric}}, #}
        ((last_value({{metric}}) OVER
        (PARTITION BY MAJOR_MARKET, IS_MEMBER, USER_TYPE, SESSION_TRAFFIC_SOURCE_GROUPING ORDER BY EVENT_DATE 
        RANGE BETWEEN UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING))/
        NULLIF(first_value({{metric}}) OVER
        (PARTITION BY MAJOR_MARKET, IS_MEMBER, USER_TYPE, SESSION_TRAFFIC_SOURCE_GROUPING ORDER BY EVENT_DATE 
        RANGE BETWEEN UNBOUNDED PRECEDING
        AND UNBOUNDED FOLLOWING),0)-1)*100 as pct_change_{{metric}}
    {% if not loop.last %}
        ,
    {% endif %}     
    {% endfor %}
from lyst.lyst_analytics.growth_model
where
    (
        {% if lag=="0" %}
            EVENT_DATE = dateadd({{freq|default(my_vars.freq)}},-1,date_trunc({{freq|default(my_vars.freq)}},current_date))
        {% else %}
            EVENT_DATE = dateadd({{freq|default(my_vars.freq)}},-{{lag|default(my_vars.lag)}}-1,date_trunc({{freq|default(my_vars.freq)}},current_date))        
        {% endif %}    
    or
        {% if lag=="0" %}
            EVENT_DATE = dateadd({{freq|default(my_vars.freq)}},-{{n_periods|default(my_vars.n_periods)}}-1,date_trunc({{freq|default(my_vars.freq)}},current_date))        
        {% else %}
            EVENT_DATE = dateadd({{freq|default(my_vars.freq)}},-{{n_periods|default(my_vars.n_periods)}}-{{lag|default(my_vars.lag)}}-1,date_trunc({{freq|default(my_vars.freq)}},current_date))        
        {% endif %}    
    )
AND PERIOD_DATE_FREQ = '{{freq|default(my_vars.freq)}}'
AND MAJOR_MARKET <> 'ALL'
AND SESSION_TRAFFIC_SOURCE_GROUPING <> 'ALL'
AND IS_MEMBER <> 'ALL'
AND USER_TYPE <> 'ALL'
;