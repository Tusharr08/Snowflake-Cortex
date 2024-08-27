
USE ROLE ACCOUNTADMIN;

CREATE ROLE CORTEX_AI;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE cortex_user_role;

GRANT ROLE cortex_user_role TO USER SNOWFLAKEPOC;
-------------------------------------------------------------------
USE ROLE SECURITYADMIN;

-- Create or replace role 'SNOWFLAKE_CORTEX_ROLE'
CREATE OR REPLACE ROLE SNOWFLAKE_CORTEX_ROLE;

-- Switch back to SYSADMIN role
USE ROLE SYSADMIN;

-- Grant USAGE privilege on warehouse 'CORTEX_WH' to role 'SNOWFLAKE_CORTEX_ROLE'
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SNOWFLAKE_CORTEX_ROLE;

-- Switch back to SECURITYADMIN role
USE ROLE SECURITYADMIN;

-- Create or replace user 'CORTEX_USER_1' with password 'XXXXX', must change password on first login, and set default role to 'SNOWFLAKE_CORTEX_ROLE'
CREATE OR REPLACE USER CORTEX_USER_1 PASSWORD = 'XXXXX' MUST_CHANGE_PASSWORD = TRUE DEFAULT_ROLE = SNOWFLAKE_CORTEX_ROLE;

-- Grant role 'SNOWFLAKE_CORTEX_ROLE' to user 'CORTEX_USER_1'
GRANT ROLE SNOWFLAKE_CORTEX_ROLE TO USER CORTEX_USER_1;

SHOW DATABASE ROLES IN DATABASE SNOWFLAKE;

-----------------------------------------------------ML FUNCTIONS--------------------------------------------------------------------

select snowflake.cortex.complete('llama2-70b-chat','Where is Snowflake headquarters? ');

SELECT SNOWFLAKE.CORTEX.TRANSLATE('Hi I am Tushar! Nice to meet you.', 'en', 'de');

SELECT * FROM CORTEX_POC.LLM_FUNC.SUPERMARKET_SALES LIMIT 10