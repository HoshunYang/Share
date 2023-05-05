SET user_table_prefix = '_temp.hsy_';
SET input_table = concat($user_table_prefix, 'product_engagement');
SET output_table = concat($user_table_prefix, 'brand_c2_prior_input');
SET dp_recency_impression_th = 20;
SET dp_count_th = 1;

CREATE OR REPLACE TABLE IDENTIFIER($output_table) AS

-- Each data point is products within a brand in a taxonomy.
with brand_category_table as (
  select
    retailer_tz
    , brand_token
  -- category level definition
    , c2

  -- prior set statistics
    , count(case when recency_impressions >= $dp_recency_impression_th then 1 else null end) as dp_qualified_count
    , dp_qualified_count / count(*) as dp_qualify_rate
    , sum(recency_impressions) as total_recency_impressions

  -- impression level aggregate
    , sum(recency_clicks) / total_recency_impressions as recency_click_rate
    , sum(recency_adds) / total_recency_impressions as recency_add_rate
    , sum(recency_weighted_engagements) / total_recency_impressions as recency_weighted_engagement_rate

  -- data point level aggregate
  -- rate, variance, and Beta(alpha, beta)
    , avg(case when recency_impressions >= $dp_recency_impression_th then recency_click_rate else null end) as dp_recency_click_rate
    , avg(case when recency_impressions >= $dp_recency_impression_th then recency_add_rate else null end) as dp_recency_add_rate
    , avg(case when recency_impressions >= $dp_recency_impression_th then recency_weighted_engagement_rate else null end) as dp_recency_weighted_engagement_rate

    , variance(case when recency_impressions >= $dp_recency_impression_th then recency_click_rate else null end) as dp_recency_click_rate_var
    , fit_alpha(dp_recency_click_rate, dp_recency_click_rate_var) as dp_recency_click_rate_alpha
    , fit_beta(dp_recency_click_rate, dp_recency_click_rate_var) as dp_recency_click_rate_beta
    , variance(case when recency_impressions >= $dp_recency_impression_th then recency_add_rate else null end) as dp_recency_add_rate_var
    , fit_alpha(dp_recency_add_rate, dp_recency_add_rate_var) as dp_recency_add_rate_alpha
    , fit_beta(dp_recency_add_rate, dp_recency_add_rate_var) as dp_recency_add_rate_beta
    , variance(case when recency_impressions >= $dp_recency_impression_th then recency_weighted_engagement_rate else null end) as dp_recency_weighted_engagement_rate_var
    , fit_alpha(dp_recency_weighted_engagement_rate, dp_recency_weighted_engagement_rate_var) as dp_recency_weighted_engagement_rate_alpha
    , fit_beta(dp_recency_weighted_engagement_rate, dp_recency_weighted_engagement_rate_var) as dp_recency_weighted_engagement_rate_beta

 -- percentile
 -- click rate percentile
    , percentile_cont(0.25) within group (order by (case when recency_impressions >= $dp_recency_impression_th then recency_click_rate else null end)) as p25_recency_click_rate
    , percentile_cont(0.33) within group (order by (case when recency_impressions >= $dp_recency_impression_th then recency_click_rate else null end)) as p33_recency_click_rate
    , percentile_cont(0.50) within group (order by (case when recency_impressions >= $dp_recency_impression_th then recency_click_rate else null end)) as p50_recency_click_rate
    , percentile_cont(0.67) within group (order by (case when recency_impressions >= $dp_recency_impression_th then recency_click_rate else null end)) as p67_recency_click_rate
    , percentile_cont(0.75) within group (order by (case when recency_impressions >= $dp_recency_impression_th then recency_click_rate else null end)) as p75_recency_click_rate
-- add rate percentile
    , percentile_cont(0.25) within group (order by (case when recency_impressions >= $dp_recency_impression_th then recency_add_rate else null end)) as p25_recency_add_rate
    , percentile_cont(0.33) within group (order by (case when recency_impressions >= $dp_recency_impression_th then recency_add_rate else null end)) as p33_recency_add_rate
    , percentile_cont(0.50) within group (order by (case when recency_impressions >= $dp_recency_impression_th then recency_add_rate else null end)) as p50_recency_add_rate
    , percentile_cont(0.67) within group (order by (case when recency_impressions >= $dp_recency_impression_th then recency_add_rate else null end)) as p67_recency_add_rate
    , percentile_cont(0.75) within group (order by (case when recency_impressions >= $dp_recency_impression_th then recency_add_rate else null end)) as p75_recency_add_rate
-- engagement rate percentile
    , percentile_cont(0.25) within group (order by (case when recency_impressions >= $dp_recency_impression_th then recency_weighted_engagement_rate else null end)) as p25_recency_weighted_engagement_rate
    , percentile_cont(0.33) within group (order by (case when recency_impressions >= $dp_recency_impression_th then recency_weighted_engagement_rate else null end)) as p33_recency_weighted_engagement_rate
    , percentile_cont(0.50) within group (order by (case when recency_impressions >= $dp_recency_impression_th then recency_weighted_engagement_rate else null end)) as p50_recency_weighted_engagement_rate
    , percentile_cont(0.67) within group (order by (case when recency_impressions >= $dp_recency_impression_th then recency_weighted_engagement_rate else null end)) as p67_recency_weighted_engagement_rate
    , percentile_cont(0.75) within group (order by (case when recency_impressions >= $dp_recency_impression_th then recency_weighted_engagement_rate else null end)) as p75_recency_weighted_engagement_rate
  from table($input_table)
  where c2 is not null
  group by 1, 2, 3
  order by retailer_tz, brand_token, c2
)

select * from brand_category_table