{% set my_vars = ({
        "min_query_date": "2024-02-01",
        "max_query_date": "2024-02-05"
    })
%}

with stage as (
select iff(view_type in ('product', 'product_overlay'), previous_view_id, view_id) as view_id,
max(view_at) as view_at,
max(view_has_purchase_intent or view_has_pi_event or view_type in ('product', 'product_overlay')) as has_engagement
from lyst.lyst_analytics.feed_view_engagement
where (view_type = 'search'
OR (view_type = 'feed' and view_sub_type = 'search results'))
and view_at::date BETWEEN '{{min_query_date|default(my_vars.min_query_date)}}' AND '{{max_query_date|default(my_vars.max_query_date)}}'
group by 1
),
stage2 as (
SELECT 
       CASE
           WHEN user_agent = 'mobile-api' THEN f.screenview_id
           ELSE f.pageview_id
       END AS view_id,
        f.session_id,
       f.event_timestamp,
       f.user_agent,
       page_start_index,
       PARSE_URL(CONCAT('http://ly.com/q?', f.pre_filters)):parameters AS pre_filters_parameters,
       OBJECT_KEYS(pre_filters_parameters) AS parameters_keys,
       LOWER(pre_filters_parameters:category::STRING) AS filters_category,
       LOWER(pre_filters_parameters:designer_slug::STRING) AS filters_designer_slug,
       LOWER(pre_filters_parameters:gender::STRING) AS filters_gender,
       LOWER(pre_filters_parameters:sale::STRING) AS filters_sale,
       LOWER(pre_filters_parameters:product_type::STRING) AS filters_product_type,
       ARRAY_EXCEPT(filter_options:free_text:token_entities, TO_ARRAY('stopword')) AS _token_entities,
       ARRAY_INTERSECTION(_token_entities, ARRAY_CONSTRUCT('gender', 'category', 'sale', 'product_type')) AS token_entities,
       ARRAY_CONSTRUCT_COMPACT(LOWER(query), filters_gender, filters_category, filters_product_type, filters_sale) AS search_query,
       coalesce(LOWER(query),'') || ' ' || coalesce(filters_gender,'') || ' ' || coalesce(filters_category,'') || ' ' || coalesce(filters_product_type,'') ||  ' ' || coalesce(filters_sale,'') as full_query,
       ARRAY_CONSTRUCT_COMPACT(LOWER(query)) AS query_only,
       ARRAY_CONTAINS('designer_slug'::VARIANT, _token_entities) AS designer_slug_entity,
       ARRAY_CONTAINS('unknown'::VARIANT, _token_entities) AS unkown_entity
FROM lyst.search_and_rank.feeds_browse_search_json_flat f
WHERE f.event_timestamp::DATE BETWEEN '{{min_query_date|default(my_vars.min_query_date)}}' AND '{{max_query_date|default(my_vars.max_query_date)}}'
AND f.user_agent IN (
                        'web-backend-internal',
                        'lyst-website',
                        'mobile-api'
                    )
AND pre_filters_parameters:language = 'en'),
stage3 as (
    select 
    row_number() over( partition by stage.view_id  order by stage.view_id) as row_num,
    stage.view_id, 
    full_query as query,     
    has_engagement
from stage
inner join stage2
on stage.view_id = stage2.view_id
)
select 
    view_id,
    query,
    has_engagement
from stage3
where row_num=1
;