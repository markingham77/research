{% set my_vars = ({
        "min_query_date": "2024-02-01",
        "max_query_date": "2024-02-05",
    })
%}

select iff(view_type in ('product', 'product_overlay'), previous_view_id, view_id) as view_id,
max(view_at) as view_at,
max(view_has_purchase_intent or view_has_pi_event or view_type in ('product', 'product_overlay')) as has_engagement
from lyst.lyst_analytics.feed_view_engagement
where (view_type = 'search'
OR (view_type = 'feed' and view_sub_type = 'search results'))
and view_at::date BETWEEN '{{min_query_date|default(my_vars.min_query_date)}}' AND '{{max_query_date|default(my_vars.max_query_date)}}'
group by 1;

