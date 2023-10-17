{{ config (
    alias = target.database + '_ga4_performance_by_term'
)}}
{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH 
    {% for date_granularity in date_granularity_list -%}

    performance_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        SPLIT_PART(property,'/',2) as profile,
        first_user_source_medium as source_medium,
        first_user_campaign_name as campaign,
        first_user_campaign_id as campaign_id,
        first_user_manual_term as adset,
        COALESCE(SUM(sessions),0) as sessions,
        COALESCE(SUM(sessions::float*(percent_new_sessions::float/100::float)),0) as new_sessions,
        COALESCE(SUM(sessions::float*(bounce_rate::float/100::float)),0) as bounced_sessions,
        COALESCE(SUM(engaged_sessions),0) as engaged_sessions,
        COALESCE(SUM(page_view),0) as pageviews,
        COALESCE(SUM(purchase),0) as purchases,
        COALESCE(SUM(purchase_value),0) as revenue
        
    FROM {{ source('ga4_raw','traffic_sources_term') }}
    GROUP BY 1,2,3,4,5,6)

    {%- if not loop.last %},

    {% endif %}
    {%- endfor %}

{% for date_granularity in date_granularity_list -%}
SELECT * 
FROM performance_{{date_granularity}}
{% if not loop.last %}UNION ALL
{% endif %}
{%- endfor %}
