{% set my_vars = ({
        "min_query_date": "2023-11-01",
        "max_query_date": "2024-03-01",
    })
%}

SELECT f.session_id,
       f.event_timestamp,
       f.user_agent,
       CASE
           WHEN user_agent = 'mobile-api' THEN f.screenview_id
           ELSE f.pageview_id
       END AS view_id,
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
       ARRAY_CONSTRUCT_COMPACT(LOWER(query)) AS query_only,
       ARRAY_CONTAINS('designer_slug'::VARIANT, _token_entities) AS designer_slug_entity,
       ARRAY_CONTAINS('unknown'::VARIANT, _token_entities) AS unkown_entity
FROM lyst.search_and_rank.feeds_browse_search_json_flat f
WHERE f.event_timestamp::DATE BETWEEN '{{min_query_date|default(my_vars.min_query_date)}}' AND '{{max_query_date|default(my_vars.max_query_date)}}'
  AND f.user_agent IN ('web-backend-internal',
                       'mobile-api',
                       'unknown')
  AND pre_filters_parameters:language = 'en';        

        