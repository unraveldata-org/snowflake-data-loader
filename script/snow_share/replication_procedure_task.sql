/**
Step-1 (One time execution for POV for 2 days)
*/

CALL CREATE_TABLES('UNRAVEL_SHARE','SCHEMA_4823_T');
CALL REPLICATE_ACCOUNT_USAGE('UNRAVEL_SHARE','SCHEMA_4823_T',2);
CALL REPLICATE_HISTORY_QUERY('UNRAVEL_SHARE','SCHEMA_4823_T',2);
CALL WAREHOUSE_PROC('UNRAVEL_SHARE','SCHEMA_4823_T');
CALL CREATE_QUERY_PROFILE(dbname => 'UNRAVEL_SHARE', schemaname => 'SCHEMA_4823_T', credit
=> '1', days => '2');

/**
  Select one procedure from REPLICATE_REALTIME_QUERY or REPLICATE_REALTIME_QUERY_BY_WAREHOUSE based on requirement.

   Select and run REPLICATE_REALTIME_QUERY procedure if you wish to get real-time queries for all warehouses.
   It will select a maximum of 10,000 real-time queries across all warehouses at intervals of 48 hours.
*/

CALL REPLICATE_REALTIME_QUERY('UNRAVEL_SHARE','SCHEMA_4823_T', 48);

/**
Select and run REPLICATE_REALTIME_QUERY_BY_WAREHOUSE procedure if you wish to get real-time queries by warehouse name.
It will select a maximum of 10,000 real-time queries for each warehouse at intervals of 48 hours.
*/

--CALL REPLICATE_REALTIME_QUERY_BY_WAREHOUSE('UNRAVEL_SHARE','SCHEMA_4823_T',48);




/**
 Step-2 Create Tasks
 create account usage tables Task
*/

CREATE OR REPLACE TASK replicate_metadata
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = '60 MINUTE'
AS
CALL REPLICATE_ACCOUNT_USAGE('UNRAVEL_SHARE','SCHEMA_4823_T',2);

/**
create history query Task
*/

CREATE OR REPLACE TASK replicate_history_query
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = '60 MINUTE'
AS
CALL REPLICATE_HISTORY_QUERY('UNRAVEL_SHARE','SCHEMA_4823_T',2);

/**
create profile replicate task
*/

CREATE OR REPLACE TASK createProfileTable
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = '60 MINUTE'
AS
CALL create_query_profile(dbname => 'UNRAVEL_SHARE',schemaname => 'SCHEMA_4823_T', credit =>
'1', days => '2');

/**
create Task for replicating information schema query history sync with warehouse
*/

CREATE OR REPLACE TASK replicate_warehouse_and_realtime_query
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = '30 MINUTE'
AS
BEGIN
    CALL warehouse_proc('UNRAVEL_SHARE','SCHEMA_4823_T');
    /**
    Select same procedure that you have selected in Step-1
     */
    CALL REPLICATE_REALTIME_QUERY('UNRAVEL_SHARE', 'SCHEMA_4823_T', 48);
    --CALL REPLICATE_REALTIME_QUERY_BY_WAREHOUSE('UNRAVEL_SHARE', 'SCHEMA_4823_T', 48);
END;


/**
 Step-3 (START ALL THE TASKS)
 */
ALTER TASK replicate_metadata RESUME;
ALTER TASK replicate_history_query RESUME;
ALTER TASK createProfileTable RESUME;
ALTER TASK replicate_warehouse_and_realtime_query RESUME;


/**
 Step-4 (Share tables , replace ${CUSTOMER_NAME} with unique value to share)
 */

Create share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
Grant Usage on database UNRAVEL_SHARE to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
Grant Usage on schema SCHEMA_4823_T to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE WAREHOUSE_METERING_HISTORY to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE WAREHOUSE_EVENTS_HISTORY to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE WAREHOUSE_LOAD_HISTORY to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE TABLES to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE METERING_DAILY_HISTORY to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE METERING_HISTORY to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE DATABASE_REPLICATION_USAGE_HISTORY to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE REPLICATION_GROUP_USAGE_HISTORY to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE DATABASE_STORAGE_USAGE_HISTORY to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE STAGE_STORAGE_USAGE_HISTORY to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE SEARCH_OPTIMIZATION_HISTORY to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE DATA_TRANSFER_HISTORY to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE AUTOMATIC_CLUSTERING_HISTORY to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE TAG_REFERENCES to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE QUERY_HISTORY to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE ACCESS_HISTORY to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE IS_QUERY_HISTORY to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE WAREHOUSE_PARAMETERS to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE WAREHOUSES to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE QUERY_PROFILE to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
GRANT SELECT ON TABLE REPLICATION_LOG to share ${CUSTOMER_NAME}_UNRAVEL_SHARE;
alter share ${CUSTOMER_NAME}_UNRAVEL_SHARE add accounts = GDB63908;

