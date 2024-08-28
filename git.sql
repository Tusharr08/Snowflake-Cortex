use role accountadmin;

create or replace secret github_secret
    type = password
    username = 'Tusharr08' 
    password = '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'; 

create or replace api integration git_api_integration
    api_provider = git_https_api
    api_allowed_prefixes = ('https://github.com/Tusharr08') 
    allowed_authentication_secrets = (github_secret)
    enabled = true;

create or replace git repository Snowflake_Cortex
    api_integration = git_api_integration
    git_credentials = github_secret
    origin = 'https://github.com/Tusharr08/Snowflake-Cortex';

-- Show repos added to snowflake.
show git repositories;

-- Show branches in the repo.
show git branches in git repository Snowflake_Cortex;

-- List files.
ls @Snowflake_Cortex/branches/main;

-- Show code in file.
select $1 from @Snowflake_Cortex/branches/main/Airbnb/airbnb_ss.py;

-- Fetch git repository updates.
alter git repository Snowflake_Cortex fetch;

DESCRIBE GIT REPOSITORY Snowflake_Cortex;

--Execute code from a repository
EXECUTE IMMEDIATE FROM @snowflake_extensions/branches/main/sql/create-database.sql;