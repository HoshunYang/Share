SET user_table_prefix = '_temp.hsy_';
SET output_table = concat($user_table_prefix, 'query_product_daily_engagement');
SET my_date = '2023-04-15';
SET my_interval_days = 35; // 5 weeks

CREATE OR REPLACE TABLE IDENTIFIER($output_table) AS

-- Step 1 Normalize by retailer and day
with retailer_day_engagement_indicator as (
  select
    retailer_token
    , date(created_at) as created_date
    , datediff('day', created_date, $my_date) as past_day
    -- We aggregate on the English translated query to get more data coverage in EU.
    , query_text_norm as eng_translated_query
    , product_token
    , any_value(query_text) as request_query
    , any_value(brand_token) as brand_token
    , case when count(*) > 0       then 1 else 0 end as impressions
    , case when sum(has_click) > 0 then 1 else 0 end as clicks
    , case when sum(has_same_product_cart_1h) > 0  then 1 else 0 end as adds
    , case when sum(has_same_product_order_1h) > 0 then 1 else 0 end as orders
  from
    etl_ranking.product_impression_attribution_1h
  where
    -- We assume the data is available from (my_date - 1)
    datediff(day, created_at::date, $my_date::date) <= $my_interval_days
    and created_at::date < $my_date::date
    and (is_viewport_impression = true or is_viewport_impression is null)
    and path_type='SEARCH'
  group by
    retailer_token
    , created_date
    , eng_translated_query
    , product_token
)

-- Step 2 Join tradezone to the data
-- Step 2.1 Join country to the data
, retailer_day_engagement_country as (
  select
    q.*
    , r.country as retailer_country
  from retailer_day_engagement_indicator q
    join analytics.canonical.retailers r
    on q.retailer_token = r.token
)

-- Step 2.2 Define three trade zones: NA_TZ, EU_TZ, and GBR_TZ
, country_to_tradezone as (
    select  ISO_ALPHA_3 as country
            ,  case when ISO_ALPHA_3='GBR' then 'GBR_TZ' else concat(ISO_CONTINENT_CODE, '_TZ') end as tradezone_gbr
            ,  case when tradezone_gbr in ('NA_TZ', 'EU_TZ', 'GBR_TZ') then tradezone_gbr else 'EU_TZ' end as tradezone
    from analytics.facts.countries
)

-- Step 2.3 Join tradezone to the data
, retailer_day_engagement_tz as (
  select
    q.*
    , c_to_tz.tradezone as retailer_tz
  from retailer_day_engagement_country q
    join country_to_tradezone c_to_tz
    on q.retailer_country = c_to_tz.country
)

-- Step 3 Compuate query<>product engagement data by tradezone
, query_product_daily_engagement_tz as (
  select
    retailer_tz
    , eng_translated_query
    , product_token
    , past_day
    , any_value(brand_token) as brand_token
    , sum(impressions) as impressions
    -- clicks
    , sum(clicks) as clicks
    , sum(clicks) / sum(impressions) as click_rate
    -- adds
    , sum(adds) as adds
    , sum(adds) / sum(impressions) as add_rate
    -- orders
    , sum(orders) as orders
    , sum(orders) / sum(impressions) as order_rate
  from
    retailer_day_engagement_tz
  group by
    retailer_tz
    , eng_translated_query
    , product_token
    , past_day
)

-- Step 4 Join with product and brand
-- [Hoshun] For training, we don't need to put filters on product and brands here. Perhaps we do this to get SQL to run faster?
-- Step 4.1 filter products
, for_sale_product as (
  select
    token as product_token
    , taxonomy_type_id as taxonomy_id
    , taxonomy_type_name as taxonomy_name
    , c1_categories
    , c2_categories
    , c3_categories
  from analytics.canonical.products
  where state = 'FOR_SALE'
)

-- Step 4.2 filter brands and get first year value
, active_brand as (
  select
    b.token as brand_token
    , b.id as brand_id
    , country as brand_country
    , b.num_stockists as brand_stockists
    , first_year_value as brand_first_year_value
  from canonical.brands b
    left join analytics.etl_datascience.brand_account_lead_scores p
    on b.id = p.brand_id
  where is_active = true
)

-- Step 4.3 Join search engagement data with product and brand information for prior uses
, query_product_daily_engagement_tz_joined_product_and_brand as (
  select
    q.*
    , p.taxonomy_id
    , p.taxonomy_name
    , p.c1_categories
    , p.c2_categories
    , p.c3_categories
    , b.brand_country
    , b.brand_stockists
    , b.brand_first_year_value
  from
    query_product_daily_engagement_tz q
    left join for_sale_product p
    on q.product_token = p.product_token
    left join active_brand b
    on q.brand_token = b.brand_token
)

-- Step 4.4 Get brand/product tradezone for analysis
, query_product_daily_engagement_tz_final as (
  select
    q.*
    , c_to_tz.tradezone as brand_tz
  from
    query_product_daily_engagement_tz_joined_product_and_brand q
    left join country_to_tradezone c_to_tz
    on q.brand_country = c_to_tz.country
)

select * from query_product_daily_engagement_tz_final