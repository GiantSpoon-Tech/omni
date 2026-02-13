select distinct platform,campaign_name, account_name from repo_mart.mart__olipop__crossplatform 
WHERE date_day >= '2025-01-01' and account_name = 'Olipop'

