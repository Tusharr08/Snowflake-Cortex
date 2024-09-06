
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

select snowflake.cortex.complete('llama2-70b-chat','Where is Snowflake\'s headquarters? ');

SELECT SNOWFLAKE.CORTEX.TRANSLATE('Hi I\'m Tushar! Nice to meet you.', 'en', 'de');

Select SNOWFLAKE.CORTEX.SUMMARIZE('Based on the provided search results and tutorial, I’ll break down the explanation of a Retrieval Augmented Generation (RAG) application from scratch.

A RAG application involves combining a retrieval tool with a large language model to generate text. The process begins with creating a prompt, which is then augmented with data retrieved from an external source. This augmented prompt is passed to the language model, producing an output that benefits from the injected information.

Here’s a step-by-step overview:

Retrieve relevant data: Use a retrieval tool to gather relevant information from your dataset or external sources. This data can be in the form of text, images, or other media.
Augment the prompt: Incorporate the retrieved data into the original prompt, blending it with the context or intent of the original query. This step is crucial, as it enables the language model to understand the added information and generate more accurate and informative output.
Pass the augmented prompt to the language model: Feed the modified prompt into a large language model, such as a transformer-based architecture. The model will generate text based on the combined input.
Output and post-processing: The resulting text will reflect the benefits of RAG, including improved accuracy, relevance, and context. You may choose to apply additional processing, such as filtering, ranking, or editing, to refine the output.
Key benefits of RAG include:

Enhanced accuracy by incorporating external knowledge
Improved relevance and context
Ability to generate more informative and detailed text');

Select SNOWFLAKE.CORTEX.SENTIMENT('Based on the provided search results and tutorial, I’ll break down the explanation of a Retrieval Augmented Generation (RAG) application from scratch.

A RAG application involves combining a retrieval tool with a large language model to generate text. The process begins with creating a prompt, which is then augmented with data retrieved from an external source. This augmented prompt is passed to the language model, producing an output that benefits from the injected information.

Here’s a step-by-step overview:

Retrieve relevant data: Use a retrieval tool to gather relevant information from your dataset or external sources. This data can be in the form of text, images, or other media.
Augment the prompt: Incorporate the retrieved data into the original prompt, blending it with the context or intent of the original query. This step is crucial, as it enables the language model to understand the added information and generate more accurate and informative output.
Pass the augmented prompt to the language model: Feed the modified prompt into a large language model, such as a transformer-based architecture. The model will generate text based on the combined input.
Output and post-processing: The resulting text will reflect the benefits of RAG, including improved accuracy, relevance, and context. You may choose to apply additional processing, such as filtering, ranking, or editing, to refine the output.
Key benefits of RAG include:

Enhanced accuracy by incorporating external knowledge
Improved relevance and context
Ability to generate more informative and detailed text');

-- SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER('Based on the provided search results and tutorial, I’ll break down the explanation of a Retrieval Augmented Generation (RAG) application from scratch.

-- A RAG application involves combining a retrieval tool with a large language model to generate text. The process begins with creating a prompt, which is then augmented with data retrieved from an external source. This augmented prompt is passed to the language model, producing an output that benefits from the injected information.

-- Here’s a step-by-step overview:

-- Retrieve relevant data: Use a retrieval tool to gather relevant information from your dataset or external sources. This data can be in the form of text, images, or other media.
-- Augment the prompt: Incorporate the retrieved data into the original prompt, blending it with the context or intent of the original query. This step is crucial, as it enables the language model to understand the added information and generate more accurate and informative output.
-- Pass the augmented prompt to the language model: Feed the modified prompt into a large language model, such as a transformer-based architecture. The model will generate text based on the combined input.
-- Output and post-processing: The resulting text will reflect the benefits of RAG, including improved accuracy, relevance, and context. You may choose to apply additional processing, such as filtering, ranking, or editing, to refine the output.
-- Key benefits of RAG include:

-- Enhanced accuracy by incorporating external knowledge
-- Improved relevance and context
-- Ability to generate more informative and detailed text','What are features of RAG?');