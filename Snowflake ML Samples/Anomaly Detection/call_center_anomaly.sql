// ***************
// Anomaly Detection
// ***************

// Start with unsupervised - uses prediction interval as threshold

select * from calls_history limit 10;

create or replace snowflake.ml.anomaly_detection ad_model1(
                                        input_data => SYSTEM$REFERENCE('VIEW', 'calls_history'),
                                        timestamp_colname => 'date',
                                        target_colname => 'total_calls',
                                        label_colname => '' --unsupervised
                                    );

show anomaly_detection;                                    

call ad_model1!detect_anomalies(input_data => SYSTEM$REFERENCE('VIEW', 'calls_for_detection'),
                    timestamp_colname =>'date',
                    target_colname => 'total_calls'
                   );


// ****
// Supervised AD training
// ****

describe table labeled_calls;
// Show labeled anomalies
select * from labeled_calls;

create or replace anomaly_detection ad_model2(
                              input_data => SYSTEM$REFERENCE('TABLE', 'labeled_calls'),
                              timestamp_colname => 'date',
                              target_colname => 'total_calls',
                              label_colname => 'label' 
                           );
                                    
call ad_model2!detect_anomalies(input_data => SYSTEM$REFERENCE('VIEW', 'calls_for_detection'),
                    timestamp_colname =>'date',
                    target_colname => 'total_calls',
                    config_object => {'prediction_interval': 0.9999}
                   );
// The following uses last query's results and is hence inseparable
SELECT count(*)
FROM TABLE(RESULT_SCAN(-1))
where is_anomaly = true;


// Show count with smaller prediction interval
call ad_model2!detect_anomalies(input_data => SYSTEM$REFERENCE('VIEW', 'calls_for_detection'),
                    timestamp_colname =>'date',
                    target_colname => 'total_calls',
                    config_object => {'prediction_interval': 0.95}
                   );
// The following uses last query's results and is hence inseparable
SELECT count(*)
FROM TABLE(RESULT_SCAN(-1))
where is_anomaly = true;

// ****
// Anomaly detection with notification
// You will need to 
// 1. Create an appropriate integration
// 2. Replace the placeholder email below
// ****

create or replace procedure anomaly_notification()
    returns int
    language SQL
    as
    $$
      declare
        anomaly_count integer;
      begin
        call ad_model2!detect_anomalies(
                    input_data => SYSTEM$QUERY_REFERENCE('select * from calls_for_detection_stream'),
                    timestamp_colname =>'date',
                    target_colname => 'total_calls'
                   );           
        let c1 cursor for select count(*) from table(RESULT_SCAN(-1)) where is_anomaly = true;
        open c1;
        fetch c1 into anomaly_count;
        if (anomaly_count >0) then 
          call SYSTEM$SEND_EMAIL(
            'pipelines_email_int',
            '<email address here>', 
            'Notification of anomalies',
             concat('Anomalies found in calls at',current_timestamp(1))
          );
        end if;
        return anomaly_count;
       end;
     $$
     ;

// The above can be run in a scheduled task or some other orchestrator
// You can also test it 
// call anomaly_notification();




// ********************************************
// Contribution Explorer 
// ********************************************

// UI Demo first!

select date, call_center, product, sum(call_count) as calls_total
from daily_calls
group by date, call_center, product
;


// Show chart
// Change the radio button for Contribution explorer
// Select test & control periods: 
//    test to cover approx the last two weeks starting from the trough (4/2) to end w/ new trend
//    control to cover some number (e.g. 6) of immediately preceding weeks: 2/18 to 4/2)
// Select both the available dimensions: call_center and product
// Click "Run Contribution Explorer"
// Explain results as segments with greatest surprise


// Now SQL function call demo w/o UI

create or replace view change_contributors as (
with input as (
  select {
    'call center': call_center,
    'cc group': cc_group
  } as categorical_dimensions,
  {
  } as continuous_dimensions,
  call_count as metric,
  iff (date between to_date('2023-04-02') and to_date('2023-04-15'), TRUE, FALSE) as label
  from daily_calls
  where date between '2023-01-01' and '2023-04-15'
)
select res.* 
from input, TABLE(
  Snowflake.ml.top_insights(
    input.categorical_dimensions,
    input.continuous_dimensions,
    metric:: float,
    input.label
  )
  over (partition by 0)
) res
);


select * from change_contributors
order by relative_change desc;