CREATE OR REPLACE DATABASE CORTEX_POC;

CREATE OR REPLACE SCHEMA DISPUTE;

create or replace WAREHOUSE CORTEX_WH WITH WAREHOUSE_SIZE = 'XSMALL' 
WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 600 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 2 SCALING_POLICY = 'STANDARD';

USE ROLE SYSADMIN;
USE DATABASE CORTEX_POC;
USE SCHEMA DISPUTE;
USE WAREHOUSE CORTEX_WH;

SELECT
  *
FROM
  CORTEX_POC.DISPUTE.DISPUTE_DATA
LIMIT
  10;

--DRop table DISPUTE_DATA;

--------------CLEANING DATA--------------------------------------------------------------------------------------------------------

SELECT DISPUTE_ID, CUSTOMER_STATEMENT, DISPUTE_TYPE, product_type from DISPUTE_DATA where length(CUSTOMER_STATEMENT)<20;

SELECT DISPUTE_ID, CUSTOMER_STATEMENT, DISPUTE_TYPE, product_type from DISPUTE_PREDICTION where length(CUSTOMER_STATEMENT)<40;

UPDATE DISPUTE_PREDICTION 
SET CUSTOMER_STATEMENT = CASE DISPUTE_ID
    WHEN 'DS001035' THEN 'Dear Merchant MC000040, I am writing to bring to your attention an unauthorized recurring charge that I have noticed on my account. This charge relates to home essentials, which I did not authorize. It is essential for me to understand why this charge was processed without my explicit permission, as I have not subscribed to any such service. I kindly request a prompt review and reversal of this charge.'
    WHEN 'DS001193' THEN 'Dear Chargeback Department, I hope this message finds you well. I am contacting you regarding a lack of disclosure related to a recent purchase I made concerning sports equipment. At the time of the transaction, critical information regarding the terms of purchase was not clearly presented to me. This has led to significant confusion and dissatisfaction with the equipment I received, which does not meet my expectations based on the information available at the time of purchase. I require clarification on this issue and guidance on how we can resolve this matter effectively.'
    ELSE CUSTOMER_STATEMENT  -- No change if DISPUTE_ID does not match
END
WHERE DISPUTE_ID IN ('DS001035', 'DS001193');

UPDATE DISPUTE_PREDICTION 
SET CUSTOMER_STATEMENT = CASE DISPUTE_ID
    WHEN 'DS001029' THEN 'Dear XYZ, I am writing to express my dissatisfaction with the baby care services I received recently. The service provided was not up to the standard I was expecting. Specific aspects of the care did not meet the description given at the time of the purchase, leaving me disappointed and seeking a resolution.'
    WHEN 'DS001030' THEN 'Dear [Merchant], I am reaching out regarding an important issue with my recent furniture order. Unfortunately, the service that was supposed to be rendered has not been provided. I was eagerly anticipating the setup and arrangement of my new furniture, but this has not happened, and I would like to know the next steps.'
    WHEN 'DS001053' THEN 'Dear Merchant, I hope this message finds you well. I need to bring to your attention a canceled transaction regarding a footwear purchase. I was looking forward to receiving the items, but they were canceled unexpectedly. I would like to know why this happened and seek any potential resolution or future purchases.'
    WHEN 'DS001075' THEN 'Dear Merchant, I am contacting you about a sports equipment order that I placed not too long ago. I unfortunately have not received the product as expected. I was excited to use it for my upcoming activities, but as of now, the item has not arrived. Could you please provide an update on the status of my order?'
    WHEN 'DS001091' THEN 'Dear Merchant, I am writing to discuss an issue related to a billing descriptor on my electronics purchase. The descriptor on my statement does not match the details I was provided at the time of checkout. This has caused confusion regarding my records and I would appreciate your assistance in clarifying this matter.'
    WHEN 'DS001092' THEN 'Dear merchant, I am reaching out to inform you about a serious issue with my recent grocery order. The products were not received, and I am very concerned about this situation. I planned my meals around these groceries, and now I am left without essential items. Please let me know what steps you can take to resolve this issue.'
    WHEN 'DS001124' THEN 'Dear Merchant, I hope you are doing well. I would like to address an issue regarding a recent order from your fashion hub. There was a lack of disclosure related to particular product details before my purchase. I was not fully informed about certain aspects, which has led to disappointment. Your guidance on how to move forward would be appreciated.'
    WHEN 'DS001160' THEN 'Dear Merchant, I am contacting you to dispute an incorrect amount charged on my recent order of home essentials. Upon reviewing my statement, I discovered that I was charged more than what was advertised during my purchase. This discrepancy is quite troubling, and I would like you to rectify the charge and provide clarity on why this occurred.'
    WHEN 'DS001179' THEN 'Dear Merchant, I hope you are having a good day. I wish to discuss a billing descriptor issue concerning a recent furniture transaction. The information displayed does not accurately reflect my purchase, leading to confusion on my end. I would be grateful if you could help clarify this and ensure that my records accurately represent the transaction.'
    ELSE CUSTOMER_STATEMENT  -- No change if DISPUTE_ID does not match
END
WHERE DISPUTE_ID IN ('DS001029', 'DS001030', 'DS001053', 'DS001075', 'DS001091', 
                      'DS001092', 'DS001124', 'DS001160', 'DS001179');

UPDATE DISPUTE_DATA 
SET CUSTOMER_STATEMENT = CASE DISPUTE_ID
    WHEN 'DS000034' THEN 'I believe there was a mistake made in processing my order.'
    WHEN 'DS000113' THEN 'I would like to report a merchant error regarding my recent purchase.'
    WHEN 'DS000119' THEN 'I did not receive complete information at the time of purchase.'
    WHEN 'DS000098' THEN 'I did not authorize this recurring charge on my account.'
    WHEN 'DS000132' THEN 'The product I received is defective and not functioning as expected.'
    WHEN 'DS000159' THEN 'I noticed an incorrect amount charged on my statement.'
    WHEN 'DS000166' THEN 'The return policy was not honored by the merchant.'
    WHEN 'DS000167' THEN 'I have not received the credit that was promised.'
    WHEN 'DS000187' THEN 'I was charged an incorrect amount for my order.'
    WHEN 'DS000200' THEN 'I was not properly informed about the product before purchasing.'
    WHEN 'DS000244' THEN 'The item I received was defective and not as described.'
    WHEN 'DS000251' THEN 'I see an incorrect charge for my purchased home essentials.'
    WHEN 'DS000421' THEN 'I do not recognize this transaction on my account.'
    WHEN 'DS000460' THEN 'I believe I have been a victim of a fraudulent transaction.'
    WHEN 'DS000490' THEN 'There is an unauthorized charge on my card that I wish to contest.'
    WHEN 'DS000492' THEN 'The services I received were not satisfactory as promised.'
    WHEN 'DS000495' THEN 'I did not receive the service that was supposed to be provided.'
    WHEN 'DS000506' THEN 'There is an incorrect amount charged on my statement.'
    WHEN 'DS000537' THEN 'I wish to dispute a canceled transaction that should have processed.'
    WHEN 'DS000564' THEN 'I believe this charge is part of a fraudulent transaction.'
    WHEN 'DS000611' THEN 'I have concerns about a possible merchant error on my recent purchase.'
    WHEN 'DS000676' THEN 'I’m reporting identity theft related to my recent footwear purchase.'
    WHEN 'DS000733' THEN 'There was an incorrect amount charged for my beauty product.'
    WHEN 'DS000736' THEN 'I\'ve been charged incorrectly for an electronic item.'
    WHEN 'DS000384' THEN 'I noticed an incorrect charge for my pet supply order.'
    WHEN 'DS000389' THEN 'My order for toys and games was not delivered.'
    WHEN 'DS000428' THEN 'I believe I\'ve been a victim of identity theft regarding my essentials.'
    WHEN 'DS000430' THEN 'The service that I paid for was not rendered as promised.'
    WHEN 'DS000455' THEN 'I did not receive the footwear I ordered.'
    WHEN 'DS000482' THEN 'The baby care product I received is defective.'
    WHEN 'DS000511' THEN 'I have not received the credit due for my transaction.'
    WHEN 'DS000516' THEN 'The groceries I received did not meet the standards promised.'
    WHEN 'DS000670' THEN 'I see an unauthorized recurring charge for my art supplies.'
    WHEN 'DS000685' THEN 'The automotive appliance service I ordered was not delivered.'
    WHEN 'DS000720' THEN 'I wish to report a fraudulent transaction regarding my furniture purchase.'
    WHEN 'DS000726' THEN 'The return process for my sports equipment was not respected.'
    WHEN 'DS000729' THEN 'I was not informed about some important details before purchase.'
    WHEN 'DS000887' THEN 'There is an unauthorized recurring charge associated with my health product.'
    WHEN 'DS000925' THEN 'I am facing an issue with the billing descriptor on my groceries.'
    WHEN 'DS000964' THEN 'There’s an incorrect price charged for my baby care essentials.'
    WHEN 'DS000966' THEN 'I was not properly informed about the footwear I bought.'
    WHEN 'DS000983' THEN 'My pet supplies order has not yet arrived.'
    WHEN 'DS000893' THEN 'I did not receive the baby care items I ordered.'
    WHEN 'DS000908' THEN 'The pet supplies I ordered were not delivered as expected.'
    WHEN 'DS000927' THEN 'The baby care services rendered were unsatisfactory.'
    WHEN 'DS000991' THEN 'The return policy for my electronics wasn’t followed as promised.'
    ELSE CUSTOMER_STATEMENT  -- No change if DISPUTE_ID does not match
END
WHERE DISPUTE_ID IN ('DS000034', 'DS000113', 'DS000119', 'DS000098', 'DS000132', 
                      'DS000159', 'DS000166', 'DS000167', 'DS000187', 
                      'DS000200', 'DS000244', 'DS000251', 'DS000421', 
                      'DS000460', 'DS000490', 'DS000492', 'DS000495', 
                      'DS000506', 'DS000537', 'DS000564', 'DS000611', 
                      'DS000676', 'DS000733', 'DS000736', 'DS000384', 
                      'DS000389', 'DS000428', 'DS000430', 'DS000455', 
                      'DS000482', 'DS000511', 'DS000516', 'DS000670', 
                      'DS000685', 'DS000720', 'DS000726', 'DS000729', 
                      'DS000887', 'DS000925', 'DS000964', 'DS000966', 
                      'DS000983', 'DS000893', 'DS000908', 'DS000927', 
                      'DS000991');

UPDATE DISPUTE_DATA 
SET CUSTOMER_STATEMENT = CASE DISPUTE_ID
    WHEN 'DS000041' THEN 'I wish to dispute an unauthorized recurring charge on my home essentials.'
    WHEN 'DS000167' THEN 'I have not received the credit that was promised to me.'
    WHEN 'DS000187' THEN 'I noticed an incorrect amount charged for my order.'
    WHEN 'DS000361' THEN 'The product I received is defective and of poor quality.'
    WHEN 'DS000472' THEN 'I did not authorize this purchase as it was not made by me.'
    WHEN 'DS000561' THEN 'The product does not match the description provided.'
    WHEN 'DS000589' THEN 'There seems to be an issue with the billing descriptor on my toys and games.'
    WHEN 'DS000303' THEN 'I did not authorize this recurring charge for baby care items.'
    WHEN 'DS000368' THEN 'The item I received does not look like what was shown in the photo.'
    WHEN 'DS000536' THEN 'The transaction in question was not approved by me.'
    WHEN 'DS000601' THEN 'I have unauthorized recurring charges related to pet supplies.'
    WHEN 'DS000389' THEN 'My order for toys and games has not yet been delivered.'
    WHEN 'DS000455' THEN 'I did not receive the footwear I ordered, and I wish to dispute this.'
    WHEN 'DS000482' THEN 'The baby care product I received is defective.'
    WHEN 'DS000777' THEN 'I feel there was a lack of disclosure regarding my groceries.'
    WHEN 'DS000774' THEN 'I’m contesting an unrecognized transaction on my sports equipment purchase.'
    WHEN 'DS000808' THEN 'I wish to dispute an unauthorized recurring charge on my baby care items.'
    WHEN 'DS000983' THEN 'My order for pet supplies has not arrived yet.'
    WHEN 'DS000893' THEN 'I did not receive the baby care items I ordered.'
    ELSE CUSTOMER_STATEMENT  -- No change if DISPUTE_ID does not match
END
WHERE DISPUTE_ID IN ('DS000041', 'DS000167', 'DS000187', 'DS000361', 'DS000472', 
                      'DS000561', 'DS000589', 'DS000303', 'DS000368', 
                      'DS000536', 'DS000601', 'DS000389', 'DS000455', 
                      'DS000482', 'DS000777', 'DS000774', 'DS000808', 
                      'DS000983', 'DS000893');

-------------------------------------------HISTORIC TREND--------------------------------------------------------------------------------
SELECT CUSTOMER_ID,DISPUTE_TYPE, COUNT(*)as DISPUTE_COUNTS FROM DISPUTE_DATA GROUP BY CUSTOMER_ID, DISPUTE_TYPE  having count(*)>0 order by DISPUTE_COUNTS desc;

SELECT CUSTOMER_ID, COUNT(*)as DISPUTE_COUNTS FROM DISPUTE_DATA GROUP BY CUSTOMER_ID  having count(*)>=15 order by DISPUTE_COUNTS desc;

SELECT CUSTOMER_ID, DISPUTE_TYPE from DISPUTE_DATA ORDER BY CUSTOMER_ID;
SELECT DISPUTE_TYPE, COUNT(*)as DISPUTE_COUNTS FROM DISPUTE_DATA GROUP BY DISPUTE_TYPE  having count(*)>0 order by DISPUTE_COUNTS desc;

SELECT * FROM DISPUTE_DATA WHERE CUSTOMER_ID='C000084';


  ------------------------------------COUNT CHECKS----------------------------------------------------------------------------------
SELECT CUSTOMER_ID , COUNT(*) FROM DISPUTE_DATA GROUP BY CUSTOMER_ID HAVING COUNT(*)>5;
SELECT CUSTOMER_ID , COUNT(*) FROM DISPUTE_PREDICTION GROUP BY CUSTOMER_ID HAVING COUNT(*)>1;

SELECT STATUS , COUNT(*) FROM DISPUTE_DATA GROUP BY STATUS HAVING COUNT(*)>5;
SELECT MERCHANT_ID , COUNT(*) FROM DISPUTE_DATA GROUP BY MERCHANT_ID HAVING COUNT(*)>5;
SELECT DISPUTE_TYPE , COUNT(*) FROM DISPUTE_DATA GROUP BY DISPUTE_TYPE HAVING COUNT(*)>5;
SELECT OUTCOME , COUNT(*) FROM DISPUTE_DATA GROUP BY OUTCOME HAVING COUNT(*)>5;
SELECT OUTCOME , COUNT(*) FROM DISPUTE_PREDICTION GROUP BY OUTCOME HAVING COUNT(*)>5;

SELECT DISTINCT DISPUTE_TYPE FROM DISPUTE_DATA;
SELECT DISTINCT CUSTOMER_DOCUMENTATION FROM DISPUTE_DATA;
SELECT DISTINCT LIABLE_FOR_DISPUTE FROM DISPUTE_OUTCOME;
--DROP TABLE DISPUTE_PREDICTION;

--------------------------------------------REJECTED DISPUTES-------------------------------------------------------------------------
UPDATE DISPUTE_PREDICTION  -- replace with the actual name of your table
SET OUTCOME = CASE
        WHEN DISPUTE_TYPE = 'Return Policy Violation' THEN 'resolved in favor of merchant'
        WHEN DISPUTE_TYPE = 'Merchant Error' 
            --AND (CUSTOMER_DOCUMENTATION LIKE '%Receipt%' OR CUSTOMER_DOCUMENTATION LIKE '%Service Agreement%') 
            THEN 'resolved in favor of customer'
        --WHEN DISPUTE_TYPE = 'Merchant Error' THEN 'resolved in favor of merchant'
        WHEN DISPUTE_TYPE = 'Product Defective' 
            --AND (CUSTOMER_DOCUMENTATION LIKE '%Receipt%' OR CUSTOMER_DOCUMENTATION LIKE '%Fraud Report%') 
        THEN 'resolved in favor of customer'
        --WHEN DISPUTE_TYPE = 'Product Defective' THEN 'resolved in favor of merchant'
        WHEN DISPUTE_TYPE = 'Billing Descriptor Issue' 
            --AND CUSTOMER_DOCUMENTATION LIKE '%Fraud Report%' 
        THEN 'resolved in favor of customer'
        --WHEN DISPUTE_TYPE = 'Billing Descriptor Issue' THEN 'resolved in favor of merchant'
        WHEN DISPUTE_TYPE = 'Incorrect Amount Charged' 
            --AND (CUSTOMER_DOCUMENTATION LIKE '%Receipt%' OR CUSTOMER_DOCUMENTATION LIKE '%Bank Statement%') 
        THEN 'resolved in favor of customer'
        --WHEN DISPUTE_TYPE = 'Incorrect Amount Charged' THEN 'resolved in favor of merchant'
        WHEN DISPUTE_TYPE = 'Fraudulent Transaction' 
            --AND CUSTOMER_DOCUMENTATION LIKE '%Fraud Report%' 
            THEN 'resolved in favor of customer'
        --WHEN DISPUTE_TYPE = 'Fraudulent Transaction' THEN 'resolved in favor of merchant'
        WHEN DISPUTE_TYPE = 'Identity Theft' 
            --AND (CUSTOMER_DOCUMENTATION LIKE '%Fraud Report%' OR CUSTOMER_DOCUMENTATION LIKE '%Identity Verification Documents%') 
            THEN 'resolved in favor of customer'
        --WHEN DISPUTE_TYPE = 'Identity Theft' THEN 'resolved in favor of merchant'
        WHEN DISPUTE_TYPE = 'Service Not Rendered' 
            AND CUSTOMER_DOCUMENTATION LIKE '%No Documentation Provided%' THEN 'resolved in favor of customer'
        ELSE 'resolved in favor of merchant'
    END
WHERE STATUS IN ('New','Resolved', 'Rejected', 'Pending'); 


------------------------------------------------SEARCH SERVICE--------------------------------------------------------------------
CREATE OR REPLACE CORTEX SEARCH SERVICE DISPUTE_SAMPLE
ON SEARCH_COL
ATTRIBUTES DISPUTE_TYPE, 
WAREHOUSE = CORTEX_WH
TARGET_LAG='1 MINUTE'
AS(
SELECT 
CONCAT('PRODUCT_TYPE: ', PRODUCT_TYPE, '\nCUSTOMER_STATEMENT: ', CUSTOMER_STATEMENT) AS SEARCH_COL
,DISPUTE_ID
,CUSTOMER_ID
,MERCHANT_ID
,AMOUNT
,PRODUCT_TYPE
,DISPUTE_TYPE
,CUSTOMER_STATEMENT
,ASSIGNED_STAFF
,CUSTOMER_DOCUMENTATION
,STATUS
FROM DISPUTE_DATA
);

--------------------------------------------------sidebar cost for model-------------------------------------------------------------

CREATE OR REPLACE TABLE MODEL_PRICING (
    MODEL VARCHAR,
    CREDITS_PER_MILLION_TOKENS FLOAT
);

INSERT INTO MODEL_PRICING (MODEL, CREDITS_PER_MILLION_TOKENS) VALUES 
('mistral-large', 5.10),
('llama3.1-405b', 3.00),
('llama3-70b', 1.21),
('llama3.1-70b', 1.21),
('snowflake-arctic', 0.84),
('jamba-instruct', 0.83),
('llama2-chat-70b', 0.45),
('reka-flash', 0.45),
('mixtral-8x7b', 0.22),
('llama3-8b', 0.19),
('llama3.1-8b', 0.19),
('mistral-7b', 0.12),
('gemma-7b', 0.12);

SELECT * FROM MODEL_PRICING;
UPDATE MODEL_PRICING SET MODEL='reka-core' where model='reka-core 5.50';

CREATE OR REPLACE VIEW DISPUTE_PRICING AS
SELECT 'COMPLETE' AS FUNCTION ,'mixtral-8x7b' AS MODEL, SUM(TOKENS), SUM(TOKENS)/1000000 AS TOTAL_TOKENS_FOR_CREDITS, TOTAL_TOKENS_FOR_CREDITS*0.22 AS CREDITS_BILLED, CREDITS_BILLED * 3 AS TOTAL_COST_USD
FROM
(SELECT DISPUTE_ID, CUSTOMER_STATEMENT, CONCAT(
'You are an expert assistant in extracting insights from the above dispute data and resolve them. 
                Answer the question based on the context and offer actionable solutions tailored for sales executives seeking to enhance customer service. Be concise and do not hallucinate. 
                If you don\'t have the information, simply state that.  
                Question: 
                Generate actionable recommendations for sales executives to address the issues highlighted in the context and improve customer service effectively. 
                Context:', CUSTOMER_STATEMENT) as prompt, SNOWFLAKE.CORTEX.COUNT_TOKENS('mixtral-8x7b', prompt) AS TOKENS
FROM DISPUTE_OUTCOME where STATUS='Rejected')
UNION
SELECT 'SUMMARIZE' AS FUNCTION ,'mixtral-8x7b' AS MODEL, SUM(TOKENS), SUM(TOKENS)/1000000 AS TOTAL_TOKENS_FOR_CREDITS, TOTAL_TOKENS_FOR_CREDITS*0.10 AS CREDITS_BILLED, CREDITS_BILLED * 3 AS TOTAL_COST_USD
FROM(
SELECT DISPUTE_ID, CUSTOMER_STATEMENT, SNOWFLAKE.CORTEX.SUMMARIZE(CUSTOMER_STATEMENT),SNOWFLAKE.CORTEX.COUNT_TOKENS('summarize', CUSTOMER_STATEMENT)AS TOKENS
FROM DISPUTE_OUTCOME where STATUS='Rejected') -- SUMMARIZE : 0.10 CREDITS/1 MILLION TOKENS
UNION
SELECT 'SENTIMENT' AS FUNCTION ,'mixtral-8x7b' AS MODEL, SUM(TOKENS), SUM(TOKENS)/1000000 AS TOTAL_TOKENS_FOR_CREDITS, TOTAL_TOKENS_FOR_CREDITS*0.08 AS CREDITS_BILLED, CREDITS_BILLED * 3 AS TOTAL_COST_USD
FROM(
SELECT DISPUTE_ID, CUSTOMER_STATEMENT, SNOWFLAKE.CORTEX.Sentiment(CUSTOMER_STATEMENT),
    SNOWFLAKE.CORTEX.COUNT_TOKENS('sentiment', CUSTOMER_STATEMENT) AS TOKENS
FROM DISPUTE_OUTCOME where STATUS='Rejected')
;-- SENTIMENT: 0.08 CREDITS/1 MILLION TOKENS

SELECT * FROM DISPUTE_PRICING;
SELECT MODEL, SUM(TOTAL_COST_USD) AS FINAL_COST_USD FROM DISPUTE_PRICING GROUP BY MODEL;



SELECT 'COMPLETE' AS FUNCTION,'reka-core 5.50' AS MODEL , SUM(TOKENS) AS TOTAL_TOKENS,
           SUM(TOKENS)/1000000 AS TOTAL_TOKENS_FOR_CREDITS,
           (SUM(TOKENS)/1000000) * 0.22 AS CREDITS_BILLED,
           CREDITS_BILLED * 3 AS TOTAL_COST_USD
    FROM (
        SELECT DISPUTE_ID, CUSTOMER_STATEMENT,
               CONCAT('You are an expert assistant in extracting insights from the above dispute data and resolve them. 
               Answer the question based on the context and offer actionable solutions tailored for sales executives seeking to enhance customer service. 
               Be concise and do not hallucinate. 
               If you don\'t have the information, simply state that. Question: 
               Generate actionable recommendations for sales executives to address the issues highlighted in the context and improve customer service effectively. 
               Context: ', CUSTOMER_STATEMENT) AS prompt,
               SNOWFLAKE.CORTEX.COUNT_TOKENS('reka-core', prompt) AS TOKENS
        FROM DISPUTE_OUTCOME
        WHERE STATUS = 'Rejected'
    ) 
    UNION
    SELECT 'SUMMARIZE' AS FUNCTION, 'mixtral-8x7b' AS MODEL, SUM(TOKENS) AS TOTAL_TOKENS,
           SUM(TOKENS)/1000000 AS TOTAL_TOKENS_FOR_CREDITS,
           (SUM(TOKENS)/1000000) * 0.10 AS CREDITS_BILLED,
           CREDITS_BILLED * 3 AS TOTAL_COST_USD
    FROM (
        SELECT DISPUTE_ID, CUSTOMER_STATEMENT,
               --SNOWFLAKE.CORTEX.SUMMARIZE(CUSTOMER_STATEMENT) AS SUMMARY,
               SNOWFLAKE.CORTEX.COUNT_TOKENS('summarize', CUSTOMER_STATEMENT) AS TOKENS
        FROM DISPUTE_DATA
        --WHERE STATUS = 'Rejected'
    ) 
    UNION
    SELECT 'SENTIMENT' AS FUNCTION, 'reka-core 5.50' AS MODEL, SUM(TOKENS) AS TOTAL_TOKENS,
           SUM(TOKENS)/1000000 AS TOTAL_TOKENS_FOR_CREDITS,
           (SUM(TOKENS)/1000000) * 0.08 AS CREDITS_BILLED,
           CREDITS_BILLED * 3 AS TOTAL_COST_USD
    FROM (
        SELECT DISPUTE_ID, CUSTOMER_STATEMENT,
               SNOWFLAKE.CORTEX.Sentiment(CUSTOMER_STATEMENT) AS SENTIMENT,
               SNOWFLAKE.CORTEX.COUNT_TOKENS('sentiment', CUSTOMER_STATEMENT) AS TOKENS
        FROM DISPUTE_OUTCOME
        WHERE STATUS = 'Rejected'
    ) ;