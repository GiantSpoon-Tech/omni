SELECT 
date,
utm_medium,
utm_source,
placement,
utm_campaign,
utm_content,
utm_term,
sum(impressions) as impressions,
sum(media_cost) as cost,
 FROM `looker-studio-pro-452620.repo_tables.delivery_unified` 
group by 1,2,3,4,5,6,7