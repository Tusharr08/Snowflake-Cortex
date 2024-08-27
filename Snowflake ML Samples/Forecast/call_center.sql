
// Change the db name, uncomment & run
// use database demodb;
// Uncomment and run the next line once
create or replace schema snow_ml;
use schema snow_ml;
// The following is an optional convenience. It needs more privileged role - accountadmin
// Without the following, you will need to fully qualify ML functions with db_name.schema_name
ALTER ACCOUNT SET search_path='$current, $public, snowflake.ml';



// Michels data gen script
create or replace table normalcalls as
select ts::timestamp_ntz as ts, call_center, cc_group, product, weeks_since_launch, escalated from ((select 
    abs(mod(random(), 100000))/100000  as rnd1,
    abs(mod(random(), 100000))/100000  as rnd2,
    LEAST(rnd1, rnd2) as rnd_lin,
    IFF(abs(mod(random(), 100000))/100000 > 0.2, rnd1, rnd_lin) as rnd,
    truncate(rnd * 120) as days,
    TIMEADD(day, days, to_date('1/1/2023')) as ts1,
    CASE
        WHEN DAYOFWEEK(ts1) in (2) THEN TIMEADD(day, MOD(RANDOM(), 2), ts1)
        WHEN DAYOFWEEK(ts1) in (2, 3, 4, 5, 6) THEN TIMEADD(day, MOD(RANDOM(), 3), ts1)
    END as ts2,
    IFF(MOD(RANDOM(), 100) < 20 AND ts2 > to_date('2/15/2023') AND ts2 < to_date('2/19/2023'), TIMEADD(day, MOD(RANDOM(),4), ts2), ts2) as ts25,
    TIMEADD(day, MOD(RANDOM(), 2), ts25) as ts3,
    TIMEADD(day, MOD(RANDOM(), 2), ts3) as ts4,
    TIMEADD(day, MOD(RANDOM(), 2), ts4) as ts,
    ['Canada', 'India', 'France', 'Texas'][MOD(ABS(RANDOM()), 4)] as call_center,
    ['Front1', 'Front2', 'Front3', 'Specialist1', 'Tech1', 'Manager'][MOD(ABS(RANDOM()), 6)] as cc_group,
    IFF( ts1 > to_date('3/15/2023'), ['S22', 'Galaxy7', 'Iphone14', 'Pixel7', 'Pixel6A', 'PlusOne', 'PlusTwo'][MOD(ABS(RANDOM()), 7)], ['S22', 'Galaxy7', 'Iphone14', 'Pixel7', 'Pixel6A', 'One'][MOD(ABS(RANDOM()), 6)]) as product,
    [78, 123, 32, 23, 45, 123 ][MOD(ABS(RANDOM()), 6)] as weeks_since_launch,
    -- Iphone14, and Canada or France is 3 timer more likely to escalate
    IFF(product = 'Iphone14' AND (call_center = 'France' OR call_center = 'Canada'), [false, false, false, false, true, true, true ][MOD(ABS(RANDOM()), 7)],  [false, false, false, false, false, false, true ][MOD(ABS(RANDOM()), 7)]) as escalated
from table(generator(ROWCOUNT => 1000000)) where ts > to_date('1/2/2023') AND ts < to_date('4/15/2023') )
);

SELECT * FROM normalcalls;

create or replace table anomalycalls as
select ts::timestamp_ntz as ts, call_center, cc_group, product, weeks_since_launch, escalated from ((select 
    abs(mod(random(), 100000))/100000  as rnd1,
    abs(mod(random(), 100000))/100000  as rnd2,
    LEAST(rnd1, rnd2) as rnd_lin,
    IFF(abs(mod(random(), 100000))/100000 > 0.2, rnd1, rnd_lin) as rnd,
    truncate(rnd * 120) as days,
    TIMEADD(day, days, to_date('4/1/2023')) as ts1,
    CASE
        WHEN DAYOFWEEK(ts1) in (2) THEN TIMEADD(day, MOD(RANDOM(), 2), ts1)
        WHEN DAYOFWEEK(ts1) in (2, 3, 4, 5, 6) THEN TIMEADD(day, MOD(RANDOM(), 3), ts1)
    END as ts2,
    IFF(MOD(RANDOM(), 100) < 20 AND ts2 > to_date('2/15/2023') AND ts2 < to_date('2/19/2023'), TIMEADD(day, MOD(RANDOM(),4), ts2), ts2) as ts25,
    TIMEADD(day, MOD(RANDOM(), 2), ts25) as ts3,
    TIMEADD(day, MOD(RANDOM(), 2), ts3) as ts4,
    TIMEADD(day, MOD(RANDOM(), 2), ts4) as ts,
    ['Canada', 'India', 'France', 'Texas'][MOD(ABS(RANDOM()), 4)] as call_center,
    ['Front1', 'Front2', 'Front3', 'Specialist1', 'Tech1', 'Manager'][MOD(ABS(RANDOM()), 6)] as cc_group,
    IFF( ts1 > to_date('3/15/2023'), ['S22', 'PlusTwo', 'Iphone14', 'PlusTwo', 'PlusTwo', 'PlusTwo', 'PlusTwo'][MOD(ABS(RANDOM()), 7)], ['S22', 'Galaxy7', 'Iphone14', 'Pixel7', 'Pixel6A', 'One'][MOD(ABS(RANDOM()), 6)]) as product,
    [78, 1, 32, 2, 3, 4, 5 ][MOD(ABS(RANDOM()), 7)] as weeks_since_launch,
    -- Iphone14, and Canada or France is 3 timer more likely to escalate
    IFF(product = 'Iphone14' AND (call_center = 'France' OR call_center = 'Canada'), [false, false, false, false, true, true, true ][MOD(ABS(RANDOM()), 7)],  [false, false, false, false, false, false, true ][MOD(ABS(RANDOM()), 7)]) as escalated
from table(generator(ROWCOUNT => 200000)) where ts > to_date('1/2/2023') AND ts < to_date('4/15/2023') )
);

SELECT * FROM anomalycalls;

create or replace view calls as select * from normalcalls union all select * from anomalycalls;

select * from calls;

// Partitioned data for anomaly detection
create or replace view calls_history as 
select cast(calls.ts as timestamp_ntz) date, count(*) as total_calls 
from calls 
where ts between to_date('1/1/2023') and to_date('3/14/2023') 
group by date;

SELECT * FROM CALLS_HISTORY;

create or replace view calls_for_detection as 
select cast(calls.ts as timestamp_ntz) date, count(*) as total_calls 
from calls 
where ts between to_date('3/15/2023') and to_date('4/14/2023') 
group by date;


SELECT * FROM CALLS_FOR_DETECTION;

create or replace view calls_history_by_product as 
select cast(calls.ts as timestamp_ntz) ts, product, count(*) as total_calls 
from calls 
where ts between to_date('1/1/2023') and to_date('3/31/2023') 
group by ts, product;

SELECT * FROM calls_history_by_product;

create or replace view calls_for_detection_by_product as 
select cast(calls.ts as timestamp_ntz) date, product, count(*) as total_calls 
from calls 
where ts between to_date('4/1/2023') and to_date('4/14/2023') 
group by date, product;

SELECT * FROM calls_for_detection_by_product;

// ***
// Data for supervised Anomaly Detection
// DML to change data to create anomalies. Table instead of view to run DML
// ***
create or replace table labeled_calls as
select * from calls_history;
select * from labeled_calls limit 10;

select * from labeled_calls
where date = to_date('1/11/2023');

update labeled_calls
set total_calls = total_calls + 400
where date = to_date('1/5/2023');

update labeled_calls
set total_calls = total_calls + 500
where date = to_date('1/11/2023');

update labeled_calls
set total_calls = total_calls - 400
where date = to_date('1/21/2023');

update labeled_calls
set total_calls = total_calls - 500
where date = to_date('1/22/2023');

update labeled_calls
set total_calls = total_calls - 500
where date = to_date('2/18/2023');

// Now label the anomalies
alter table labeled_calls
add column label boolean default false;

update labeled_calls
set label = true
where date in (
  to_date('1/5/2023'),
  to_date('1/11/2023'),
  to_date('1/21/2023'),
  to_date('1/22/2023'),
  to_date('2/18/2023')
);

select * from labeled_calls
where date in (
  to_date('1/5/2023'),
  to_date('1/11/2023'),
  to_date('1/21/2023'),
  to_date('1/22/2023'),
  to_date('2/18/2023')
);


// For Contribution Explorer
create or replace view daily_calls as
select to_date(ts) as date, call_center, cc_group, product, count(*) as call_count
from calls
group by to_date(ts), call_center, cc_group, product
;

select * from daily_calls limit 10;

// ******** End of data gen


// ********
// Show the generated data
// ********

select * from calls limit 10; 
select count(*) from calls;

// ***************
// Forecasting
// ***************

// Use a subset to train forecast model by aggregating daily counts
create or replace view calls_for_training as 
select cast(calls.ts as timestamp_ntz) date, count(*) as total_calls 
from calls 
where date < to_date('3/15/2023') 
group by date;

// See chart of training data
// Exogenous variables could be added to time series
select * from calls_for_training; 


-- //*** 
-- // Forecast training / model building
-- //*** 

-- // Built using Snowpark
-- // LGBM + polynomial regression
-- // Vuew/table/query can be used as input

create or replace snowflake.ml.forecast calls_forecast (
    input_data => SYSTEM$REFERENCE('view', 'calls_for_training'),
    timestamp_colname => 'date',
    target_colname => 'total_calls'
);

// Made available through "data cloud" capabilities
// Instances of the class
show snowflake.ml.forecast;

// Path has been set so no need to qualify
show forecast;

-- //*** 
-- // Forecast generation / inference
-- //*** 

call calls_forecast!forecast(
    forecasting_periods => 30
);
-- The following uses last query's results and is hence inseparable
-- Augment training (past) data with the forecast (future)
SELECT date, total_calls AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
  FROM calls_for_training 
UNION ALL
SELECT ts as date, NULL AS actual, round(forecast,0), round(lower_bound,0), round(upper_bound,0)
  FROM TABLE(RESULT_SCAN(-));

// With non-default prediction interval
call calls_forecast!forecast(
    forecasting_periods => 30,
    config_object => {'prediction_interval': 0.999}
);


// Starting point for more explanation
// Particularly useful for exogenous variables - e.g. a marketing promotion or campaign here
call calls_forecast!explain_feature_importance();

-- //***
-- // Multiple time-series
-- // Add product to the view
-- //***

create or replace view calls_for_training_by_product as 
select cast(calls.ts as timestamp_ntz) ts , product, count(*) as total_calls 
from calls 
where ts < to_date('4/21/2023') 
group by ts, product;

Select * from calls_for_training_by_product order by ts;

// Multiple time-series in parallel
create or replace forecast calls_forecast_by_product (
    input_data => SYSTEM$REFERENCE('view', 'calls_for_training_by_product'),
    series_colname => 'product', -- multiple time series in one go
    timestamp_colname => 'ts',
    target_colname => 'total_calls'
);

call calls_forecast_by_product!forecast(
    forecasting_periods => 30
);

// ***
// Use forecast in data pipelines
// ***
// Weekly training
create task forecast_retrain 
schedule = 'USING CRON 0 1 * * SUN America/Los_Angeles' -- every Sunday at 1am PT
as
    create or replace snowflake.ml.forecast callsh_forecast (
        input_data => SYSTEM$REFERENCE('view', 'calls_last_180_days'),
        timestamp_colname => 'ts',
        target_colname => 'total_calls'
    )
;

// Daily inference (forecast) after metric calculation
// Could be used in dbt as well
create task metric_calculate
as
  // metric computation goes here
;

create task forecast_infer
    after metric_calculate
as
    call callsh_forecast!forecast(
        forecasting_periods => 7,
        config_object => {}
    )
    // Update & extend the forecast in appropriate table
;


