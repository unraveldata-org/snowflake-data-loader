/**
  Purpose: This script calls the procedures to replicate the Snowflake ACCOUNT_USAGE data for sharing.
  It creates the necessary tables and mask the query text in QUERY_HISTORY table for secure sharing.
 */
SET DATABASE_TO_SHARE = 'UNRAVEL_DB_SHARE';
SET SCHEMA_TO_SHARE = 'UNRAVEL_SCHEMA_SHARE';
USE IDENTIFIER($DATABASE_TO_SHARE);
USE SCHEMA IDENTIFIER($SCHEMA_TO_SHARE);

/**
    Step 1: One time execution for POV (Past X(180) days))
*/
CALL CREATE_QUERY_HISTORY_TABLE_FROM_SNOWFLAKE((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'), 'QUERY_HISTORY');

CALL create_table_from_snowflake((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE') , 'ACCESS_HISTORY');

CALL CREATE_TABLES((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'));

CALL REPLICATE_ACCOUNT_USAGE((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'), '180');

CALL REPLICATE_STORAGE_METADATA((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'),'180');

CALL REPLICATE_ACCESS_HISTORY_SESSION((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'), '180');

CALL REPLICATE_MASKED_QUERY_HISTORY((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'), '180');

CALL WAREHOUSE_PROC((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'));

CALL CREATE_QUERY_PROFILE((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'PROFILE_QUERY_CREDIT'), '14');

/**
    Execute REPLICATE_REALTIME_QUERY_BY_WAREHOUSE procedure if you need real-time queries by warehouse name.
    Maximum of 10,000 real-time queries will be selected for each warehouse in interval of 1 hour.
*/
CALL REPLICATE_REALTIME_QUERY_BY_WAREHOUSE((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'R_DAYS'));

CALL create_shared_db_metadata((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'));

/**
    Step 2: Create task using procedure to run the process periodically for continous polling of data.
    Run only if Step 1 and Share DB to Unravel is executed.
*/
CALL create_tasks_with_schedule((SELECT VALUE FROM config_parameters where CONFIG_ID = 'WAREHOUSE_NAME'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'REPLICATE_METADATA'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'REPLICATE_STORAGE_METADATA'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'REPLICATE_ACCESS_HISTORY_SESSION'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'REPLICATE_MASKED_QUERY_HISTORY'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'REPLICATE_WAREHOUSE_AND_REALTIME_QUERY'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'CLEANUP_DATA_TASK'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'CREATE_SHARED_DB_METADATA'));

/**
    Step 3: Resume all the tasks
 */
ALTER TASK replicate_metadata RESUME;
ALTER TASK replicate_storage_metadata RESUME;
ALTER TASK REPLICATE_ACCESS_HISTORY_SESSION RESUME;
ALTER TASK REPLICATE_MASKED_QUERY_HISTORY RESUME;
ALTER TASK replicate_warehouse_and_realtime_query RESUME;
ALTER TASK cleanup_data_task RESUME;
ALTER TASK create_shared_db_metadata_task RESUME;

/**
    Step 4: Share data with Unravel account
*/
CALL SHARE_TO_ACCOUNT((SELECT VALUE FROM config_parameters where CONFIG_ID = 'ACCOUNT_ID'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SHARE_NAME'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'));
