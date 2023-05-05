SET user_table_prefix = '_temp.hsy_';
SET input_table = concat($user_table_prefix, 'query_product_engagement');
SET output_table = concat($user_table_prefix, 'product_engagement');

SET w_add =  5;
SET w_order = 20;

CREATE OR REPLACE TABLE IDENTIFIER($output_table) AS

-- Aggregate engagement data by retailer_tz and product
with product_engagement as (
  select
    retailer_tz
    , product_token
    -- prior groups
    , any_value(brand_token) as brand_token
    , any_value(brand_tz) as brand_tz
    , any_value(taxonomy_id) as taxonomy_id
    , any_value(taxonomy_name) as taxonomy_name
    -- arbitarily selects the first category
    , any_value(c1_categories)[0] as c1
    , any_value(c2_categories)[0] as c2
    , any_value(c3_categories)[0] as c3
    -- brand quality
    , any_value(brand_stockists) as brand_stockists
    , any_value(brand_first_year_value) as brand_first_year_value
    -- metrics
    , sum(impressions) as impressions
    , sum(recency_impressions) as recency_impressions

    -- As adds and orders are too sparse, we consider using them to enhance click metrics instead of using them as separate metrics
    , sum(clicks + $w_add * adds + $w_order * orders ) as weighted_engagements
    , sum(clicks + $w_add * adds + $w_order * orders ) / sum(impressions) as weighted_engagement_rate
    , sum(recency_clicks + $w_add * recency_adds + $w_order * recency_orders) as recency_weighted_engagements
    , sum(recency_clicks + $w_add * recency_adds + $w_order * recency_orders) / sum(recency_impressions) as recency_weighted_engagement_rate

    -- clicks
    , sum(clicks) as clicks
    , sum(recency_clicks) as recency_clicks
    , sum(clicks) / sum(impressions) as click_rate
    , sum(recency_clicks) / sum(recency_impressions) as recency_click_rate
    -- adds
    , sum(adds) as adds
    , sum(recency_adds) as recency_adds
    , sum(adds) / sum(impressions) as add_rate
    , sum(recency_adds) / sum(recency_impressions) as recency_add_rate
    -- orders
    , sum(orders) as orders
    , sum(recency_orders) as recency_orders
    , sum(orders) / sum(impressions) as order_rate
    , sum(recency_orders) / sum(recency_impressions) as recency_order_rate

  from table($input_table)
  group by
    retailer_tz
    , product_token
)

select * from product_engagement