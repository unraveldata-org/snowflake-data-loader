/**
 * Step-1 (One-time setup for POV for X(180) days)
 * ------------------------------------------------
 * It needs to be executed only once and sets up the required configurations,
 * objects, and data retention for the next 180 days (or the agreed duration).
 */


/**
 * Step-2 (Create continuous polling task)
 * ---------------------------------------
 * In this step, a task is created using the provided stored procedure.
 * The task ensures continuous polling and data collection.
 * IMPORTANT: Run this step only after Step-1 has been successfully completed.
 *
 */

 /**
 Step-3 (START ALL THE TASKS)
 */

 /**
 * Step-4 (Data sharing with Unravel account)
 * -----------------------------------
 * This step grants access to Unravel accountId.
 *
 */


/**
Step-1 (One time execution for POV for X(180) days)
*/
CALL create_table_from_snowflake((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'), 'QUERY_HISTORY');

CALL create_table_from_snowflake((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE') , 'ACCESS_HISTORY');

CALL CREATE_TABLES((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'));

CALL REPLICATE_ACCOUNT_USAGE((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'), '180');

CALL REPLICATE_STORAGE_METADATA((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'),'180');

CALL REPLICATE_HISTORY_QUERY((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'), '180');

CALL WAREHOUSE_PROC((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'));

CALL CREATE_QUERY_PROFILE((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'PROFILE_QUERY_CREDIT'), '14');

/**
Select and run REPLICATE_REALTIME_QUERY_BY_WAREHOUSE procedure if you wish to get real-time queries by warehouse name.It will select a maximum of 10,000 real-time queries for each warehouse at intervals of 1 hours.
*/

CALL REPLICATE_REALTIME_QUERY_BY_WAREHOUSE((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'R_DAYS'));

CALL create_shared_db_metadata((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'));



/**
Step-2  Create task using procedure (continuous polling data task)
Run only if you have executed Steps-1 and share db to unravel.
*/
CALL create_tasks_with_schedule((SELECT VALUE FROM config_parameters where CONFIG_ID = 'WAREHOUSE_NAME'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'REPLICATE_METADATA'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'REPLICATE_STORAGE_METADATA'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'REPLICATE_HISTORY_QUERY'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'REPLICATE_WAREHOUSE_AND_REALTIME_QUERY'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'CLEANUP_DATA_TASK'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'CREATE_SHARED_DB_METADATA'));

/**
 Step-3 (START ALL THE TASKS)
 */
ALTER TASK replicate_metadata RESUME;
ALTER TASK replicate_storage_metadata RESUME;
ALTER TASK replicate_history_query RESUME;
ALTER TASK replicate_warehouse_and_realtime_query RESUME;
ALTER TASK cleanup_data_task RESUME;
ALTER TASK create_shared_db_metadata_task RESUME;

/**
  Step-4 (Data sharing with Unravel account)
*/
CALL SHARE_TO_ACCOUNT((SELECT VALUE FROM config_parameters where CONFIG_ID = 'ACCOUNT_ID'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SHARE_NAME'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'));
