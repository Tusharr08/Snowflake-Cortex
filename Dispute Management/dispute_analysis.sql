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