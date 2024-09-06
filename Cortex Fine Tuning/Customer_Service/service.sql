use database cortex_poc;
use schema public;

use role sysadmin;
use warehouse cortex_wh;

CREATE or REPLACE file format csvformat
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  type = 'CSV';

CREATE or REPLACE stage support_tickets_data_stage
  file_format = csvformat
  url = 's3://sfquickstarts/finetuning_llm_using_snowflake_cortex_ai/';

CREATE or REPLACE TABLE SUPPORT_TICKETS (
  ticket_id VARCHAR(60),
  customer_name VARCHAR(60),
  customer_email VARCHAR(60),
  service_type VARCHAR(60),
  request VARCHAR,
  contact_preference VARCHAR(60)
);

COPY into SUPPORT_TICKETS
  from @support_tickets_data_stage;

SELECT * FROM SUPPORT_TICKETS;