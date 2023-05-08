SET user_table_prefix = '_temp.hsy_';
SET input_table = concat($user_table_prefix, 'product_daily_engagement');
SET output_table = concat($user_table_prefix, 'brand_c2_velocity');

SET w_add =  5;
SET w_order = 20;
SET four_weeks_days = 28;
SET one_week_days = 7;
SET previous_day = 1;
SET moving_avg_window = 7;

CREATE OR REPLACE TABLE IDENTIFIER($output_table) AS

/**
    Example SQL for computing velocity metrics for different entities. This example uses brand-C2 entity.

    We will compute velocity metrics for product, brand-C3C2 (concat C3 and C2 names), brand-C2, and brand-C1 entities.

    Note that some velocity metrics are computed using Python UDF. Snowflake interprets the Python UDF's None as NaN in
    this context. The downstream use case needs to check for both is null and is NaN.
 */
with brandC2_daily as (
  select
    retailer_tz
    , brand_token
    , c2
    , past_day
    -- metrics
    , sum(impressions) as impressions
    , sum(clicks) as clicks
    , sum(clicks + $w_add * adds + $w_order * orders ) as weighted_engagements
    , sum(adds) as adds
    , sum(orders) as orders
  from table($input_table)
  group by 1, 2, 3, 4
)

, brandC2_velocity as (
  select
    retailer_tz
    , brand_token
    , c2
    -- input metrics for velocity computations
    , sum(case when past_day <= $one_week_days then impressions else null end) as impressions_1w
    , sum(case when past_day <= $four_weeks_days then impressions else null end) as impressions_4w
    , sum(case when past_day <= $one_week_days then clicks else null end) as clicks_1w
    , sum(case when past_day <= $four_weeks_days then clicks else null end) as clicks_4w
    , sum(case when past_day <= $one_week_days then adds else null end) as adds_1w
    , sum(case when past_day <= $four_weeks_days then adds else null end) as adds_4w
    , sum(case when past_day <= $one_week_days then weighted_engagements else null end) as weighted_engagements_1w
    , sum(case when past_day <= $four_weeks_days then weighted_engagements else null end) as weighted_engagements_4w
    , div0(clicks_1w, impressions_1w) as click_rate_1w
    , div0(clicks_4w, impressions_4w) as click_rate_4w
    , div0(adds_1w, impressions_1w) as add_rate_1w
    , div0(adds_4w, impressions_4w) as add_rate_4w
    , div0(weighted_engagements_1w, impressions_1w) as weighted_engagement_rate_1w
    , div0(weighted_engagements_4w, impressions_4w) as weighted_engagement_rate_4w
    , array_agg(past_day) within group (order by past_day) as past_day_arr
    , array_agg(impressions) within group (order by past_day) as impressions_arr
    , array_agg(clicks) within group (order by past_day) as clicks_arr
    , array_agg(adds) within group (order by past_day) as adds_arr
    , array_agg(weighted_engagements) within group (order by past_day) as weighted_engagements_arr

    -- actual velocity metrics
    -- 1week over 4weeks
    , case when click_rate_4w > 0 then click_rate_1w / click_rate_4w else null end as click_rate_1w4w_ratio
    , case when add_rate_4w > 0 then add_rate_1w / add_rate_4w else null end as add_rate_1w4w_ratio
    , case when weighted_engagement_rate_4w > 0 then weighted_engagement_rate_1w / weighted_engagement_rate_4w else null end as weighted_engagement_rate_1w4w_ratio
    -- ascending similarity
    -- Usage note: Snowflake interprets the Python UDF None as NaN in this context
    , ascending_similarity($four_weeks_days, $previous_day, $moving_avg_window, past_day_arr, clicks_arr) as click_ascending_similarity
    , ascending_similarity($four_weeks_days, $previous_day, $moving_avg_window, past_day_arr, adds_arr) as add_ascending_similarity
    , ascending_similarity($four_weeks_days, $previous_day, $moving_avg_window, past_day_arr, weighted_engagements_arr) as weighted_engagement_ascending_similarity
    -- linear regression slope not included due to slowness in current framework
  from brandC2_daily
  group by 1, 2, 3
)

select * from brandC2_velocity