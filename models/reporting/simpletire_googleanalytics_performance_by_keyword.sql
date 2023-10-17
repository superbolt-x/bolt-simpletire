{{ config (
    alias = target.database + '_googleanalytics_performance_by_keyword'
)}}
{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH 
    {% for date_granularity in date_granularity_list -%}

    performance_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        profile,
        source_medium,
        campaign,
        keyword,
        COALESCE(SUM(sessions),0) as sessions,
        COALESCE(SUM(sessions::float*(percent_new_sessions::float/100::float)),0) as new_sessions,
        COALESCE(SUM(sessions::float*(bounce_rate::float/100::float)),0) as bounced_sessions,
        COALESCE(SUM(sessions::float*avg_session_duration::float),0) as session_duration,
        COALESCE(SUM(pageviews),0) as pageviews,
        COALESCE(SUM(transactions),0) as purchases,
        COALESCE(SUM(transaction_revenue),0) as revenue
        
    FROM {{ source('googleanalytics_raw','traffic') }}
    GROUP BY 1,2,3,4,5)

    {%- if not loop.last %},

    {% endif %}
    {%- endfor %}

{% for date_granularity in date_granularity_list -%}
SELECT * 
FROM performance_{{date_granularity}}
{% if not loop.last %}UNION ALL
{% endif %}
{%- endfor %}
