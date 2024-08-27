CREATE OR REPLACE TABLE training_purchase_data AS (
    SELECT
        CAST(UNIFORM(0, 4, RANDOm()) as VARCHAR) as user_interest_score,
        UNIFORM(0, 3, RANDOM()) as user_rating, FALSE AS label,
        'not_interested' AS class
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(4, 7, RANDOM()) AS VARCHAR) AS user_interest_score,
        UNIFORM(3, 7, RANDOM()) AS user_rating, FALSE AS label,
        'add_to_wishlist' AS class
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(7, 10, RANDOM()) AS VARCHAR) AS user_interest_score,
        UNIFORM(7, 10, RANDOM()) AS user_rating,
        TRUE as label, 'purchase' AS class
    FROM TABLE(GENERATOR(rowCount => 100))
);

SELECT * FROM TRAINING_PURCHASE_DATA;

CREATE OR REPLACE table prediction_purchase_data AS (
    SELECT
        CAST(UNIFORM(0, 4, RANDOM()) AS VARCHAR) AS user_interest_score,
        UNIFORM(0, 3, RANDOM()) AS user_rating
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(4, 7, RANDOM()) AS VARCHAR) AS user_interest_score,
        UNIFORM(3, 7, RANDOM()) AS user_rating
    FROM TABLE(GENERATOR(rowCount => 100))
    UNION ALL
    SELECT
        CAST(UNIFORM(7, 10, RANDOM()) AS VARCHAR) AS user_interest_score,
        UNIFORM(7, 10, RANDOM()) AS user_rating
    FROM TABLE(GENERATOR(rowCount => 100))
);

SELECT * FROM PREDICTION_PURCHASE_DATA;

CREATE OR REPLACE view binary_classification_view AS
    SELECT user_interest_score, user_rating, label
FROM training_purchase_data;

SELECT * FROM binary_classification_view ORDER BY RANDOM(42) LIMIT 5;

CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION model_binary(
    INPUT_DATA => SYSTEM$REFERENCE('view', 'binary_classification_view'),
    TARGET_COLNAME => 'label'
);

SELECT model_binary!PREDICT(INPUT_DATA => {*})
    as prediction from prediction_purchase_data;

SELECT *, model_binary!PREDICT(
    INPUT_DATA => {*})
    as predictions from prediction_purchase_data;

CREATE OR REPLACE VIEW multiclass_classification_view AS
    SELECT user_interest_score, user_rating, class
FROM training_purchase_data;

SELECT * FROM multiclass_classification_view ORDER BY RANDOM(42) LIMIT 10;

CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION model_multiclass(
    INPUT_DATA => SYSTEM$REFERENCE('view', 'multiclass_classification_view'),
    TARGET_COLNAME => 'class'
);

SELECT *, model_multiclass!PREDICT(
    INPUT_DATA => {*})
    as predictions from prediction_purchase_data;

CREATE OR REPLACE TABLE my_predictions AS
SELECT *, model_multiclass!PREDICT(INPUT_DATA => {*}) as predictions from prediction_purchase_data;

SELECT * FROM my_predictions;
SELECT
    predictions:class AS predicted_class,
    ROUND(predictions:probability:not_interested,4) AS not_interested_class_probability,
    ROUND(predictions['probability']['purchase'],4) AS purchase_class_probability,
    ROUND(predictions['probability']['add_to_wishlist'],4) AS add_to_wishlist_class_probability
FROM my_predictions
LIMIT 5;

CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION model(
    INPUT_DATA => SYSTEM$REFERENCE('view', 'binary_classification_view'),
    TARGET_COLNAME => 'label',
    CONFIG_OBJECT => {'evaluate': TRUE}
);

CALL model!SHOW_EVALUATION_METRICS();
CALL model_multiclass!SHOW_GLOBAL_EVALUATION_METRICS();
CALL model_multiclass!SHOW_CONFUSION_MATRIX();
CALL model_multiclass!SHOW_FEATURE_IMPORTANCE();