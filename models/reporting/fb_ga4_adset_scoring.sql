{{ config (
    alias = target.database + '_fb_ga4_adset_scoring'
)}}

WITH ga4_data as (
    select date_trunc('day',date) as dg, 
    CASE 
        WHEN session_campaign_name in 
            ('[Superbolt] - Prospecting Advantage - Purchase - Auto',
            '[Superbolt] - Prospecting Advantage DPA - Purchase - 7 Day Click - Auto',
            '[Superbolt] - Prospecting Advantage+ - Purchase - Auto',
            '[Superbolt] - Prospecting Advantage+ DPA - Purchase - 7 Day Click - Auto',
            '[Superbolt]+-+Prospecting+Advantage++DPA+-+Purchase+-+7+Day+Click+-+Auto',
            '[Superbolt] - Prospecting Advantage - Purchase - Auto',
            '[Superbolt] - Prospecting Advantage DPA - Purchase - 7 Day Click - Auto',
            '[Superbolt] - Prospecting Advantage+ - Purchase - Auto',
            '[Superbolt] - Prospecting Advantage+ DPA - Purchase - 7 Day Click - Auto',
            '[Superbolt]+-+Prospecting+Advantage++-+Purchase+-+Auto')
            then 6343153224672::VARCHAR 
        WHEN session_campaign_name in 
            ('[Superbolt] - Prospecting LAL + INT - Purchase - 7 Day Click - Auto',
            '[Superbolt] - Prospecting LAL INT - Purchase - 7 Day Click - Auto',
            '[Superbolt] - Prospecting LAL + INT - Purchase - 7 Day Click - Auto',
            '[Superbolt] - Prospecting LAL INT - Purchase - 7 Day Click - Auto',
            '[Superbolt]+-+Prospecting+LAL+++INT+-+Purchase+-+7+Day+Click+-+Auto')
            then 6348892771472::VARCHAR 
        WHEN session_campaign_name in 
            ('[Superbolt] - Prospecting Advantage Creative Testing - Purchase - 7 Day Click - Auto',
            '[Superbolt] - Prospecting Advantage+ Creative Testing - Purchase - 7 Day Click - Auto',
            '[Superbolt] - Prospecting Advantage+ Creative Testing - Purchase - 7 Day Click - Auto Campaign',
            '[Superbolt] - Prospecting Advantage+ Creative Testing - Purchase - 7 Day Click - Auto',
            '[Superbolt] - Prospecting Advantage+ Creative Testing - Purchase - 7 Day Click - Auto Campaign',
            '[Superbolt]+-+Prospecting+Advantage++Creative+Testing+-+Purchase+-+7+Day+Click+-+Auto')
            then 6374145319672::VARCHAR 
        WHEN session_campaign_name in 
            ('[Superbolt] - Retargeting - Purchase - 1 Day Click - Auto',
            '[Superbolt] - Retargeting - Purchase - 1 Day Click - Auto',
            '[Superbolt]+-+Retargeting+-+Purchase+-+1+Day+Click+-+Auto')
            then 6374414439472::VARCHAR
        WHEN session_campaign_name in 
            ('[Superbolt] - Armstrong Prospecting Advantage+ - Purchase - 7 Day Click - Auto',
            '[Superbolt] - Armstrong Prospecting Advantage+ - Purchase - 7 Day Click - Auto')
            then 6392538387272::VARCHAR
        WHEN session_campaign_name in 
            ('[Superbolt] - Prospecting ATC Armstrong - Add to Cart - 7 Day Click - Auto',
            '[Superbolt] - Prospecting ATC Armstrong - Add to Cart - 7 Day Click - Auto')
            then 6403030195672::VARCHAR
        WHEN session_campaign_name in 
            ('[Superbolt] - Prospecting ATC Yokohama - Add to Cart - 7 Day Click - Auto')
            then 6462317707272::VARCHAR
        WHEN session_campaign_name in 
            ('[Superbolt] - Short Term Brand Retargeting - Purchase - 1 Day Click - Auto',
            '[Superbolt] - Short Term Brand Retargeting - Purchase - 1 Day Click - Auto')
            then 6470949986472::VARCHAR
    else session_campaign_id::VARCHAR 
    end as campaign_id,
    CASE 
        WHEN session_manual_term in (
            '[Superbolt] - Prospecting Advantage Creative Testing - Purchase - 7 Day Click - Auto Ad set',
            '[Superbolt] - Prospecting Advantage+ Creative Testing - Purchase - 7 Day Click - Auto Ad set',
            '[Superbolt]+-+Prospecting+Advantage++Creative+Testing+-+Purchase+-+7+Day+Click+-+Auto+Ad+set'
        )
        THEN '[Superbolt] - Prospecting Advantage+ Creative Testing - Purchase - 7 Day Click - Auto Ad set'
        WHEN session_manual_term in (
            'INT - Adv - Auto - Purchase',
            'INT - Adv Broad - Auto - Purchase',
            'INT - Adv+ - Auto - Purchase',
            'INT - Adv+ Broad - Auto - Purchase',
            'INT+-+Adv++-+Auto+-+Purchase',
            'INT+-+Adv++Broad+-+Auto+-+Purchase'
        )
        THEN 'INT - Adv+ Broad - Auto - Purchase'
        WHEN session_manual_term in (
            'INT - Adv Rebates Broad - Auto - Purchase',
            'INT - Adv+ Rebates Broad - Auto - Purchase',
            'INT+-+Adv++Rebates+Broad+-+Auto+-+Purchase',
            'INT - Adv+ Promos Broad - Auto - Purchase'
        ) and session_campaign_id = 6523352903272
        THEN 'INT - Adv+ Promos Broad - Auto - Purchase'
        WHEN session_manual_term in (
            'INT - Auto Insurance - Auto - Purchase',
            'INT+-+Auto+Insurance+-+Auto+-+Purchase'
        )
        THEN 'INT - Auto Insurance - Auto - Purchase'
        WHEN session_manual_term in (
            'INT - Disney Goers - Auto - Purchase',
            'INT+-+Disney+Goers+-+Auto+-+Purchase'
        )
        THEN 'INT - Disney Goers - Auto - Purchase'
        WHEN session_manual_term in (
            'INT - Ski & Snowboard - Auto - Purchase',
            'INT+-+Ski+&+Snowboard+-+Auto+-+Purchase'
        )
        THEN 'INT - Ski & Snowboard - Auto - Purchase'
        WHEN session_manual_term in (
            'INT - Wholesalers - Auto - Purchase',
            'INT+-+Wholesalers+-+Auto+-+Purchase'
        )
        THEN 'INT - Wholesalers - Auto - Purchase'
        WHEN session_manual_term in (
            'RET - Website Visitors 60 Days - Auto - Purchase',
            'RET+-+Website+Visitors+60+Days+-+Auto+-+Purchase'
        )
        THEN 'RET - Website Visitors 60 Days - Auto - Purchase'
        WHEN session_manual_term in (
            'GEO - Northern States - Auto - Purchase',
            'GEO+-+Northern+States+-+Auto+-+Purchase'
        )
        THEN 'GEO - Northern States - Auto - Purchase'
        WHEN session_manual_term = '(not set)' and session_campaign_name = '[Superbolt] - Retargeting - Purchase - 1 Day Click - Auto'
        THEN 'RET - Website Visitors 60 Days - Auto - Purchase'
    else session_manual_term
    end as adset_name,
    sum(conversions_purchase) as ga4_purchases, sum(total_revenue) as ga4_revenue, sum(sessions) as ga4_traffic 
    from {{ source('ga4_raw','traffic_sources_term') }}
    where session_source_medium = 'fb / cpc'
    group by 1,2,3
    order by 1 desc
)
, fb_data as (
    select date as dg, campaign_id::VARCHAR as campaign_id, campaign_name, adset_name, 
    sum(spend) as fb_spend, sum(impressions) as impressions, sum(link_clicks) as link_clicks, sum(view_content) as view_content
    from {{ source('reporting','facebook_ad_performance') }}
    where date_granularity = 'day'
    group by 1,2,3,4
)

, final_data as (
select dg as date, 
    'Simple Tire' as account_name, 
    campaign_name, 
    adset_name,
    sum(fb_spend) as spend, 
    sum(link_clicks) as link_clicks,
    sum(impressions) as impressions,
    sum(view_content) as view_content,
    sum(coalesce(ga4_revenue,0)) as ga4_revenue,
    sum(coalesce(ga4_purchases,0)) as ga4_purchases,
    sum(coalesce(ga4_traffic,0)) as ga4_traffic
from fb_data left join ga4_data using(dg,campaign_id,adset_name)
group by 1,2,3,4
)

select * from final_data
