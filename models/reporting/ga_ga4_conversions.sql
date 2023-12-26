with meta_data as 
(select date, 
case when campaign_name = '[Superbolt] - Retargeting - Purchase - Auto' then '[Superbolt] - Retargeting - Purchase - Auto (inactive)' else campaign_name end as campaign_name, 
campaign_id::varchar as campaign_id,
adset_name, 
--ad_name, 
sum(spend) as meta_spend,
sum(link_clicks) as meta_link_clicks,
sum(impressions) as meta_impressions
from {{ source('reporting','facebook_ad_performance') }} 
where date_granularity = 'day' and campaign_name ~* 'superbolt'
group by 1,2,3,4)
  
, ga_data as 
(select date, 
case 
    when campaign in 
    ('[Superbolt] - Prospecting Advantage+ - Purchase - Auto',
    '[Superbolt]+-+Prospecting+Advantage++-+Purchase+-+Auto',
    '[Superbolt] - Prospecting Advantage+ - Purchase - 7 Day Click - Auto',
    '[Superbolt] - Prospecting Advantage - Purchase - Auto',
    '[Superbolt]+-+Prospecting+Advantage++-+Purchase+-+7+Day+Click+-+Auto') 
    then '[Superbolt] - Prospecting Advantage+ - Purchase - 7 Day Click - Auto'
    when campaign in 
    ('[Superbolt] - Prospecting LAL + INT - Purchase - 7 Day Click - Auto',
    '[Superbolt]+-+Prospecting+LAL+++INT+-+Purchase+-+7+Day+Click+-+Auto')
    then '[Superbolt] - Prospecting LAL + INT - Purchase - 7 Day Click - Auto'
    when campaign in 
    ('[Superbolt] - Retargeting - Purchase - 7 Day Click - Auto',
    '[Superbolt]+-+Retargeting+-+Purchase+-+7+Day+Click+-+Auto') 
    then '[Superbolt] - Retargeting - Purchase - 7 Day Click - Auto'
    when campaign in 
    ('[Superbolt] - Retargeting - Purchase - Auto',
    '[Superbolt]+-+Retargeting+-+Purchase+-+Auto')
    then '[Superbolt] - Retargeting - Purchase - Auto (inactive)'
    when campaign in 
    ('[Superbolt]+-+Reactivation+-+Purchase+-+7+Day+Click+-+Auto')
    then '[Superbolt] - Reactivation - Purchase - 7 Day Click - Auto'
    when campaign in 
    ('[Superbolt] - Prospecting Advantage+ Creative Testing - Purchase - 7 Day Click - Auto',
    '[Superbolt] - Prospecting Advantage+ Creative Testing - Purchase - 7 Day Click - Auto Campaign')
    then '[Superbolt] - Prospecting Advantage+ Creative Testing - Purchase - 7 Day Click - Auto'
    when campaign in 
    ('[Superbolt] - Prospecting Advantage+ DPA - Purchase - 7 Day Click - Auto',
    '[Superbolt] - Prospecting Advantage+ - Purchase - Auto')
    then '[Superbolt] - Prospecting Advantage+ DPA - Purchase - 7 Day Click - Auto'
    else campaign 
end as campaign_name, 
case when keyword in 
    ('INT - Disney Goers - Auto - Purchase', 'INT+-+Disney+Goers+-+Auto+-+Purchase')
    then 'INT - Disney Goers - Auto - Purchase'
    when keyword in 
    ('INT - Disney Goers - Auto - Purchase - tROAS', 'INT+-+Disney+Goers+-+Auto+-+Purchase+-+tROAS')
    then 'INT - Disney Goers - Auto - Purchase - tROAS'
    when keyword in 
    ('LAL - 5% 180 Day Past Purchasers - Auto - Purchase', 'LAL+-+5%+180+Day+Past+Purchasers+-+Auto+-+Purchase')
    then 'LAL - 5% 180 Day Past Purchasers - Auto - Purchase'
    when keyword in 
    ('INT - Middle America - Auto - Purchase', 'INT+-+Middle+America+-+Auto+-+Purchase')
    then 'INT - Middle America - Auto - Purchase'
    when keyword in 
    ('INT - Adv+ Broad - Auto - Purchase', 'INT+-+Adv++Broad+-+Auto+-+Purchase')
    then 'INT - Adv+ Broad - Auto - Purchase'
    when keyword in 
    ('REACT - Tire Replacement 5 Years - Auto - Purchase', 'REACT+-+Tire+Replacement+5+Years+-+Auto+-+Purchase')
    then 'REACT - Tire Replacement 5 Years - Auto - Purchase'
    when keyword in 
    ('REACT - Tire Replacement 2 Years - Auto - Purchase', 'REACT+-+Tire+Replacement+2+Years+-+Auto+-+Purchase')
    then 'REACT - Tire Replacement 2 Years - Auto - Purchase'
    when keyword in 
    ('RET - Website Visitors 60 Days - Auto - Purchase', 'RET+-+Website+Visitors+60+Days+-+Auto+-+Purchase')
    then 'RET - Website Visitors 60 Days - Auto - Purchase'
    when keyword in 
    ('RET - Add To Cart 30 Days - Auto - Purchase', 'RET+-+Add+To+Cart+30+Days+-+Auto+-+Purchase')
    then 'RET - Add To Cart 30 Days - Auto - Purchase'
    when keyword in 
    ('[Superbolt] - Prospecting Advantage+ Creative Testing - Purchase - 7 Day Click - Auto Ad set')
    then '[Superbolt] - Prospecting Advantage+ Creative Testing - Purchase - 7 Day Click - Auto Ad set'
end as adset_name, 
--ad_content as ad_name, 
sum(purchases) as ga_purchases, 
sum(revenue) as ga_revenue,
sum(sessions) as ga_sessions
from {{ source('reporting','googleanalytics_performance_by_keyword') }}
where source_medium IN ('fb / cpc','facebook / social') and campaign ~* 'superbolt'
and date_granularity = 'day'
GROUP BY 1,2,3)
, ga4_data as 
(select date,
case
    when campaign_id = '(not set)' and campaign in 
    ('[Superbolt] - Prospecting Advantage+ - Purchase - Auto',
    '[Superbolt] - Prospecting Advantage - Purchase - Auto',
    '[Superbolt]+-+Prospecting+Advantage++-+Purchase+-+Auto',
    '[Superbolt] - Prospecting Advantage+ - Purchase - 7 Day Click - Auto',
    '[Superbolt]+-+Prospecting+Advantage++-+Purchase+-+7+Day+Click+-+Auto',
    '[Superbolt] - Prospecting Advantage DPA - Purchase - 7 Day Click - Auto',
    '[Superbolt]+-+Prospecting+Advantage++DPA+-+Purchase+-+7+Day+Click+-+Auto')
    then '6343153224672'
    when campaign_id = '(not set)' and campaign in 
    ('[Superbolt] - Prospecting LAL + INT - Purchase - 7 Day Click - Auto',
    '[Superbolt] - Prospecting LAL INT - Purchase - 7 Day Click - Auto',
    '[Superbolt]+-+Prospecting+LAL+++INT+-+Purchase+-+7+Day+Click+-+Auto')
    then '6348892771472'
    when campaign_id = '(not set)' and campaign in 
    ('[Superbolt] - Retargeting - Purchase - 7 Day Click - Auto',
    '[Superbolt]+-+Retargeting+-+Purchase+-+7+Day+Click+-+Auto') 
    then '6348263697872'
    when campaign_id = '(not set)' and campaign in 
    ('[Superbolt] - Retargeting - Purchase - Auto',
    '[Superbolt]+-+Retargeting+-+Purchase+-+Auto')
    then '6343172227672'
    when campaign_id = '(not set)' and campaign in 
    ('[Superbolt]+-+Reactivation+-+Purchase+-+7+Day+Click+-+Auto')
    then '6359143147272'
    when campaign_id = '(not set)' and campaign in 
    ('[Superbolt] - Armstrong Prospecting Advantage - Purchase - 7 Day Click - Auto',
    '[Superbolt] - Armstrong Prospecting Advantage+ - Purchase - 7 Day Click - Auto')
    then '6392538387272'
    when campaign_id = '(not set)' and campaign in 
    ('[Superbolt] - Prospecting ATC Armstrong - Add to Cart - 7 Day Click - Auto')
    then '6403030195672'
    when campaign_id = '(not set)' and campaign in 
    ('[Superbolt] - Prospecting ATC Yokohama - Add to Cart - 7 Day Click - Auto')
    then '6462317707272'
    when campaign_id = '(not set)' and campaign in 
    ('[Superbolt] - Prospecting Advantage Creative Testing - Purchase - 7 Day Click - Auto Campaign',
    '[Superbolt] - Prospecting Advantage Creative Testing - Purchase - 7 Day Click - Auto')
    then '6374145319672'
    when campaign_id = '(not set)' and campaign in 
    ('[Superbolt] - Retargeting - Purchase - 1 Day Click - Auto')
    then '6374414439472'
    else campaign_id
end as campaign_id,
case when adset in 
    ('INT - Disney Goers - Auto - Purchase', 'INT+-+Disney+Goers+-+Auto+-+Purchase')
    then 'INT - Disney Goers - Auto - Purchase'
    when adset in 
    ('INT - Disney Goers - Auto - Purchase - tROAS', 'INT+-+Disney+Goers+-+Auto+-+Purchase+-+tROAS')
    then 'INT - Disney Goers - Auto - Purchase - tROAS'
    when adset in 
    ('LAL - 5% 180 Day Past Purchasers - Auto - Purchase', 'LAL+-+5%+180+Day+Past+Purchasers+-+Auto+-+Purchase')
    then 'LAL - 5% 180 Day Past Purchasers - Auto - Purchase'
    when adset in 
    ('INT - Middle America - Auto - Purchase', 'INT+-+Middle+America+-+Auto+-+Purchase')
    then 'INT - Middle America - Auto - Purchase'
    when adset in 
    ('INT - Adv+ Broad - Auto - Purchase', 'INT+-+Adv++Broad+-+Auto+-+Purchase')
    then 'INT - Adv+ Broad - Auto - Purchase'
    when adset in 
    ('REACT - Tire Replacement 5 Years - Auto - Purchase', 'REACT+-+Tire+Replacement+5+Years+-+Auto+-+Purchase')
    then 'REACT - Tire Replacement 5 Years - Auto - Purchase'
    when adset in 
    ('REACT - Tire Replacement 2 Years - Auto - Purchase', 'REACT+-+Tire+Replacement+2+Years+-+Auto+-+Purchase')
    then 'REACT - Tire Replacement 2 Years - Auto - Purchase'
    when adset in 
    ('RET - Website Visitors 60 Days - Auto - Purchase', 'RET+-+Website+Visitors+60+Days+-+Auto+-+Purchase')
    then 'RET - Website Visitors 60 Days - Auto - Purchase'
    when adset in 
    ('RET - Add To Cart 30 Days - Auto - Purchase', 'RET+-+Add+To+Cart+30+Days+-+Auto+-+Purchase')
    then 'RET - Add To Cart 30 Days - Auto - Purchase'
    when adset in 
    ('[Superbolt] - Prospecting Advantage+ Creative Testing - Purchase - 7 Day Click - Auto Ad set')
    then '[Superbolt] - Prospecting Advantage+ Creative Testing - Purchase - 7 Day Click - Auto Ad set'
    when adset in 
    ('INT - Adv - Auto - Purchase','INT - Adv Broad - Auto - Purchase','INT - Adv+ - Auto - Purchase','INT+-+Adv++Broad+-+Auto+-+Purchase')
    then 'INT - Adv+ Broad - Auto - Purchase'
    when adset in 
    ('RET - Website Visitors 60 Days - Auto - Purchase','RET - Website Visitors Past 60 Days - Auto - Purchase')
    then 'RET - Website Visitors 60 Days - Auto - Purchase'
    when adset in
    ('[Superbolt] - Armstrong Prospecting Advantage - Purchase - 7 Day Click - Auto Ad set',
    '[Superbolt] - Armstrong Prospecting Advantage+ - Purchase - 7 Day Click - Auto Ad set')
    then '[Superbolt] - Armstrong Prospecting Advantage+ - Purchase - 7 Day Click - Auto Ad set'
    when adset in 
    ('[Superbolt] - Prospecting Advantage Creative Testing - Purchase - 7 Day Click - Auto Ad set')
    then '[Superbolt] - Prospecting Advantage+ Creative Testing - Purchase - 7 Day Click - Auto Ad set'
end as adset_name, 
sum(purchases) as ga4_purchases, 
sum(revenue) as ga4_revenue,
sum(sessions) as ga4_sessions
from {{ source('reporting','ga4_performance_by_term') }} 
where source_medium IN ('fb / cpc','facebook / social') and campaign ~* 'superbolt'
and date_granularity = 'day'
GROUP BY 1,2,3)

, final_data as (select date,
campaign_name,
campaign_id,
adset_name,
sum(meta_spend) as fb_spend,
sum(meta_link_clicks) as fb_link_clicks,
sum(meta_impressions) as fb_impressions,
sum(coalesce(ga_purchases,0)) as ga_purchases,
sum(coalesce(ga_revenue,0)) as ga_revenue,
sum(coalesce(ga_sessions,0)) as ga_sessions,
sum(coalesce(ga4_purchases,0)) as ga4_purchases,
sum(coalesce(ga4_revenue,0)) as ga4_revenue,
sum(coalesce(ga4_sessions,0)) as ga4_sessions
from
meta_data 
left join ga4_data using(date,campaign_id,adset_name)
left join ga_data using(date,campaign_name,adset_name)
--where date >= '2023-07-03'
group by 1,2,3,4)

select * from final_data order by date desc 
