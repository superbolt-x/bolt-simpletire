{{ config (
    alias = target.database + '_ga4_performance_by_term'
)}}
{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH 
    {% for date_granularity in date_granularity_list -%}

    performance_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        DATE_TRUNC('{{date_granularity}}',date)::date as date,
        SPLIT_PART(property,'/',2) as profile,
        session_source_medium as source_medium,
        session_campaign_name as campaign,
        session_campaign_id as campaign_id,
        session_manual_term as adset,
        COALESCE(SUM(sessions),0) as sessions,
        COALESCE(SUM(new_users),0) as new_users,
        COALESCE(SUM(sessions::float*(bounce_rate::float/100::float)),0) as bounced_sessions,
        COALESCE(SUM(engaged_sessions),0) as engaged_sessions,
        COALESCE(SUM(conversions_purchase),0) as purchases,
        COALESCE(SUM(purchase_revenue),0) as revenue
        
    FROM {{ source('ga4_raw','traffic_sources_term') }}
    GROUP BY 1,2,3,4,5,6,7)

    {%- if not loop.last %},

    {% endif %}
    {%- endfor %}

{% for date_granularity in date_granularity_list -%}
SELECT * 
FROM performance_{{date_granularity}}
{% if not loop.last %}UNION ALL
{% endif %}
{%- endfor %}
