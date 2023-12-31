CREATE OR REPLACE PROCEDURE prepare_advance_replication_schema(dbname string, schemaname string)
    returns VARCHAR(252)
    LANGUAGE javascript

AS
$$

try
{
    var query = 'CREATE DATABASE IF NOT EXISTS ' + DBNAME + ';';
    var stmt = snowflake.createStatement({sqlText:query})
    stmt.execute();
    result = "Database: " + DBNAME + " creation is success";
}
catch (err)
{
    return "Failed to create DB " + DBNAME + ", error: " + err;
}

try
{
    var query = 'CREATE SCHEMA IF NOT EXISTS ' + DBNAME + '.' + SCHEMANAME + ';';
    var stmt = snowflake.createStatement({sqlText:query})
    stmt.execute();
    result += "\nSchema: " + SCHEMANAME + " creation is success";
}
catch (err)
{
    return "Failed to create the schema "+ SCHEMANAME + ", error: " + err;
}

var schemaName = SCHEMANAME;
var dbName = DBNAME;

const tbs = [
    "ACCESS_HISTORY",
    "AUTOMATIC_CLUSTERING_HISTORY",
    "COLUMNS",
    "COPY_HISTORY",
    "DATABASE_STORAGE_USAGE_HISTORY",
    "DATABASES",
    "FILE_FORMATS",
    "FUNCTIONS",
    "GRANTS_TO_ROLES",
    "GRANTS_TO_USERS",
    "LOAD_HISTORY",
    "LOGIN_HISTORY",
    "MASKING_POLICIES",
    "MATERIALIZED_VIEW_REFRESH_HISTORY",
    "METERING_DAILY_HISTORY",
    "METERING_HISTORY",
    "PIPE_USAGE_HISTORY",
    "PIPES",
    "POLICY_REFERENCES",
    "QUERY_HISTORY",
    "QUERY_HISTORY_RT",
    "REPLICATION_USAGE_HISTORY",
    "ROLES",
    "SCHEMATA",
    "SEARCH_OPTIMIZATION_HISTORY",
    "SESSIONS",
    "STAGE_STORAGE_USAGE_HISTORY",
    "STAGES",
    "STORAGE_USAGE",
    "TABLE_STORAGE_METRICS",
    "TABLES",
    "TASK_HISTORY",
    "USERS",
    "VIEWS",
    "WAREHOUSE_EVENTS_HISTORY",
    "WAREHOUSE_LOAD_HISTORY",
    "WAREHOUSE_METERING_HISTORY",
    "WAREHOUSE_PARAMETERS",
    "WAREHOUSES",
    "REPLICATION_GROUP_USAGE_HISTORY",
    "DATA_TRANSFER_HISTORY",
    "SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY",
    "AUTO_REFRESH_REGISTRATION_HISTORY",
    "TAG_REFERENCES"
];

for (var i = 0; i < tbs.length; i++) {
    var drop_tbl = snowflake.createStatement({sqlText:'DROP TABLE IF EXISTS ' + dbName + '.' + schemaName + '.' + tbs[i] +';'});
    try {
        drop_tbl.execute();
    }
    catch (err)
    {
        return "Failed: " + err;
    }
}
const queries = [];
queries[0] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.ACCESS_HISTORY(QUERY_ID VARCHAR(128), QUERY_START_TIME TIMESTAMP_LTZ(9), USER_NAME VARCHAR(128), DIRECT_OBJECTS_ACCESSED ARRAY, BASE_OBJECTS_ACCESSED ARRAY, OBJECTS_MODIFIED ARRAY, OBJECT_MODIFIED_BY_DDL OBJECT, POLICIES_REFERENCED ARRAY);';
queries[1] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.AUTOMATIC_CLUSTERING_HISTORY(START_TIME TIMESTAMP_LTZ(6), END_TIME TIMESTAMP_LTZ(9), CREDITS_USED NUMBER(38,9), NUM_BYTES_RECLUSTERED NUMBER(38,0), NUM_ROWS_RECLUSTERED NUMBER(38,0), TABLE_ID NUMBER(38,0), TABLE_NAME VARCHAR(16777216), SCHEMA_ID NUMBER(38,0), SCHEMA_NAME VARCHAR(16777216), DATABASE_ID NUMBER(38,0), DATABASE_NAME VARCHAR(16777216), INSTANCE_ID NUMBER(38,0));';
queries[2] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.COLUMNS(COLUMN_ID NUMBER(38,0), COLUMN_NAME VARCHAR(16777216), TABLE_ID NUMBER(38,0), TABLE_NAME VARCHAR(16777216), TABLE_SCHEMA_ID NUMBER(38,0), TABLE_SCHEMA VARCHAR(16777216), TABLE_CATALOG_ID NUMBER(38,0), TABLE_CATALOG VARCHAR(16777216), ORDINAL_POSITION NUMBER(38,0), COLUMN_DEFAULT VARCHAR(16777216), IS_NULLABLE VARCHAR(3), DATA_TYPE VARCHAR(16777216), CHARACTER_MAXIMUM_LENGTH NUMBER(38,0), CHARACTER_OCTET_LENGTH NUMBER(38,0), NUMERIC_PRECISION NUMBER(38,0), NUMERIC_PRECISION_RADIX NUMBER(2,0), NUMERIC_SCALE NUMBER(38,0), DATETIME_PRECISION NUMBER(38,0), INTERVAL_TYPE VARCHAR(16777216), INTERVAL_PRECISION VARCHAR(16777216), CHARACTER_SET_CATALOG VARCHAR(16777216), CHARACTER_SET_SCHEMA VARCHAR(16777216), CHARACTER_SET_NAME VARCHAR(16777216), COLLATION_CATALOG VARCHAR(16777216), COLLATION_SCHEMA VARCHAR(16777216), COLLATION_NAME VARCHAR(16777216), DOMAIN_CATALOG VARCHAR(16777216), DOMAIN_SCHEMA VARCHAR(16777216), DOMAIN_NAME VARCHAR(16777216), UDT_CATALOG VARCHAR(16777216), UDT_SCHEMA VARCHAR(16777216), UDT_NAME VARCHAR(16777216), SCOPE_CATALOG VARCHAR(16777216), SCOPE_SCHEMA VARCHAR(16777216), SCOPE_NAME VARCHAR(16777216), MAXIMUM_CARDINALITY VARCHAR(16777216), DTD_IDENTIFIER VARCHAR(16777216), IS_SELF_REFERENCING VARCHAR(2), IS_IDENTITY VARCHAR(3), IDENTITY_GENERATION VARCHAR(16777216), IDENTITY_START VARCHAR(16777216), IDENTITY_INCREMENT VARCHAR(16777216), IDENTITY_MAXIMUM VARCHAR(16777216), IDENTITY_MINIMUM VARCHAR(16777216), IDENTITY_CYCLE VARCHAR(16777216), IDENTITY_ORDERED VARCHAR(16777216), COMMENT VARCHAR(16777216), DELETED TIMESTAMP_LTZ(6));';
queries[3] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.COPY_HISTORY(FILE_NAME VARCHAR(16777216), STAGE_LOCATION VARCHAR(16777216), LAST_LOAD_TIME TIMESTAMP_LTZ(6), ROW_COUNT NUMBER(38,0), ROW_PARSED NUMBER(38,0), FILE_SIZE NUMBER(38,0), FIRST_ERROR_MESSAGE VARCHAR(16777216), FIRST_ERROR_LINE_NUMBER NUMBER(38,0), FIRST_ERROR_CHARACTER_POS NUMBER(38,0), FIRST_ERROR_COLUMN_NAME VARCHAR(16777216), ERROR_COUNT NUMBER(38,0), ERROR_LIMIT NUMBER(38,0), STATUS VARCHAR(16777216), TABLE_ID NUMBER(38,0), TABLE_NAME VARCHAR(16777216), TABLE_SCHEMA_ID NUMBER(38,0), TABLE_SCHEMA_NAME VARCHAR(16777216), TABLE_CATALOG_ID NUMBER(38,0), TABLE_CATALOG_NAME VARCHAR(16777216), PIPE_CATALOG_NAME VARCHAR(16777216), PIPE_SCHEMA_NAME VARCHAR(16777216), PIPE_NAME VARCHAR(16777216), PIPE_RECEIVED_TIME TIMESTAMP_LTZ(6), FIRST_COMMIT_TIME TIMESTAMP_LTZ(6));';
queries[4] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.DATABASE_STORAGE_USAGE_HISTORY(USAGE_DATE DATE, DATABASE_ID NUMBER(38,0), DATABASE_NAME VARCHAR(16777216), DELETED TIMESTAMP_LTZ(6), AVERAGE_DATABASE_BYTES FLOAT, AVERAGE_FAILSAFE_BYTES FLOAT, AVERAGE_HYBRID_TABLE_STORAGE_BYTES FLOAT);';
queries[5] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.DATABASES(DATABASE_ID NUMBER(38,0), DATABASE_NAME VARCHAR(16777216), DATABASE_OWNER VARCHAR(16777216), IS_TRANSIENT VARCHAR(3), COMMENT VARCHAR(16777216), CREATED TIMESTAMP_LTZ(6), LAST_ALTERED TIMESTAMP_LTZ(6), DELETED TIMESTAMP_LTZ(6), RETENTION_TIME NUMBER(38,0), RESOURCE_GROUP VARCHAR(16777216), TYPE VARCHAR(19));';
queries[6] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.FILE_FORMATS(FILE_FORMAT_ID NUMBER(38,0), FILE_FORMAT_NAME VARCHAR(16777216), FILE_FORMAT_SCHEMA_ID NUMBER(38,0), FILE_FORMAT_SCHEMA VARCHAR(16777216), FILE_FORMAT_CATALOG_ID NUMBER(38,0), FILE_FORMAT_CATALOG VARCHAR(16777216), FILE_FORMAT_OWNER VARCHAR(16777216), FILE_FORMAT_TYPE VARCHAR(16777216), RECORD_DELIMITER VARCHAR(16777216), FIELD_DELIMITER VARCHAR(16777216), SKIP_HEADER NUMBER(38,0), DATE_FORMAT VARCHAR(16777216), TIME_FORMAT VARCHAR(16777216), TIMESTAMP_FORMAT VARCHAR(16777216), BINARY_FORMAT VARCHAR(16777216), ESCAPE VARCHAR(16777216), ESCAPE_UNENCLOSED_FIELD VARCHAR(16777216), TRIM_SPACE BOOLEAN, FIELD_OPTIONALLY_ENCLOSED_BY VARCHAR(16777216), NULL_IF VARCHAR(16777216), COMPRESSION VARCHAR(16777216), ERROR_ON_COLUMN_COUNT_MISMATCH BOOLEAN, CREATED TIMESTAMP_LTZ(6), LAST_ALTERED TIMESTAMP_LTZ(6), DELETED TIMESTAMP_LTZ(6), COMMENT VARCHAR(16777216), OWNER_ROLE_TYPE VARCHAR(13));';
queries[7] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.FUNCTIONS(FUNCTION_ID NUMBER(38,0), FUNCTION_NAME VARCHAR(16777216), FUNCTION_SCHEMA_ID NUMBER(38,0), FUNCTION_SCHEMA VARCHAR(16777216), FUNCTION_CATALOG_ID NUMBER(38,0), FUNCTION_CATALOG VARCHAR(16777216), FUNCTION_OWNER VARCHAR(16777216), ARGUMENT_SIGNATURE VARCHAR(16777216), DATA_TYPE VARCHAR(16777216), CHARACTER_MAXIMUM_LENGTH NUMBER(38,0), CHARACTER_OCTET_LENGTH NUMBER(38,0), NUMERIC_PRECISION NUMBER(38,0), NUMERIC_PRECISION_RADIX NUMBER(2,0), NUMERIC_SCALE NUMBER(38,0), FUNCTION_LANGUAGE VARCHAR(16777216), FUNCTION_DEFINITION VARCHAR(16777216), VOLATILITY VARCHAR(16777216), IS_NULL_CALL VARCHAR(16777216), CREATED TIMESTAMP_LTZ(6), LAST_ALTERED TIMESTAMP_LTZ(6), DELETED TIMESTAMP_LTZ(6), COMMENT VARCHAR(16777216), IS_EXTERNAL VARCHAR(3), API_INTEGRATION VARCHAR(16777216), CONTEXT_HEADERS VARCHAR(16777216), MAX_BATCH_ROWS NUMBER(38,0), COMPRESSION VARCHAR(16777216), IMPORTS VARCHAR(16777216), HANDLER VARCHAR(16777216), TARGET_PATH VARCHAR(16777216), RUNTIME_VERSION VARCHAR(16777216), PACKAGES VARCHAR(16777216), INSTALLED_PACKAGES VARCHAR(16777216), OWNER_ROLE_TYPE VARCHAR(13));';
queries[8] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.GRANTS_TO_ROLES(CREATED_ON TIMESTAMP_LTZ(6), MODIFIED_ON TIMESTAMP_LTZ(6), PRIVILEGE VARCHAR(16777216), GRANTED_ON VARCHAR(16777216), NAME VARCHAR(16777216), TABLE_CATALOG VARCHAR(16777216), TABLE_SCHEMA VARCHAR(16777216), GRANTED_TO VARCHAR(16), GRANTEE_NAME VARCHAR(16777216), GRANT_OPTION BOOLEAN, GRANTED_BY VARCHAR(16777216), DELETED_ON TIMESTAMP_LTZ(6), GRANTED_BY_ROLE_TYPE VARCHAR(16777216), OBJECT_INSTANCE VARCHAR(16777216));';
queries[9] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.GRANTS_TO_USERS(CREATED_ON TIMESTAMP_LTZ(6), DELETED_ON TIMESTAMP_LTZ(6), ROLE VARCHAR(16777216), GRANTED_TO VARCHAR(4), GRANTEE_NAME VARCHAR(16777216), GRANTED_BY VARCHAR(16777216));';
queries[10] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.LOAD_HISTORY(TABLE_ID NUMBER(38,0), TABLE_NAME VARCHAR(16777216), SCHEMA_ID NUMBER(38,0), SCHEMA_NAME VARCHAR(16777216), CATALOG_ID NUMBER(38,0), CATALOG_NAME VARCHAR(16777216), FILE_NAME VARCHAR(16777216), LAST_LOAD_TIME TIMESTAMP_LTZ(6), STATUS VARCHAR(16777216), ROW_COUNT NUMBER(38,0), ROW_PARSED NUMBER(38,0), FIRST_ERROR_MESSAGE VARCHAR(16777216), FIRST_ERROR_LINE_NUMBER NUMBER(38,0), FIRST_ERROR_CHARACTER_POSITION NUMBER(38,0), FIRST_ERROR_COL_NAME VARCHAR(16777216), ERROR_COUNT NUMBER(38,0), ERROR_LIMIT NUMBER(38,0));';
queries[11] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.LOGIN_HISTORY(EVENT_ID NUMBER(38,0), EVENT_TIMESTAMP TIMESTAMP_LTZ(6), EVENT_TYPE VARCHAR(16777216), USER_NAME VARCHAR(16777216), CLIENT_IP VARCHAR(16777216), REPORTED_CLIENT_TYPE VARCHAR(16777216), REPORTED_CLIENT_VERSION VARCHAR(16777216), FIRST_AUTHENTICATION_FACTOR VARCHAR(16777216), SECOND_AUTHENTICATION_FACTOR VARCHAR(16777216), IS_SUCCESS VARCHAR(3), ERROR_CODE NUMBER(38,0), ERROR_MESSAGE VARCHAR(16777216), RELATED_EVENT_ID NUMBER(38,0), CONNECTION VARCHAR(16777216));';
queries[12] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.MASKING_POLICIES(POLICY_ID NUMBER(38,0), POLICY_NAME VARCHAR(16777216), POLICY_SCHEMA_ID NUMBER(38,0), POLICY_SCHEMA VARCHAR(16777216), POLICY_CATALOG_ID NUMBER(38,0), POLICY_CATALOG VARCHAR(16777216), POLICY_OWNER VARCHAR(16777216), POLICY_SIGNATURE VARCHAR(16777216), POLICY_RETURN_TYPE VARCHAR(16777216), POLICY_BODY VARCHAR(16777216), POLICY_COMMENT VARIANT, CREATED TIMESTAMP_LTZ(6), LAST_ALTERED TIMESTAMP_LTZ(6), DELETED TIMESTAMP_LTZ(6), OWNER_ROLE_TYPE VARCHAR(13), OPTIONS VARIANT);';
queries[13] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.MATERIALIZED_VIEW_REFRESH_HISTORY(START_TIME TIMESTAMP_LTZ(6), END_TIME TIMESTAMP_LTZ(9), CREDITS_USED NUMBER(38,9), TABLE_ID NUMBER(38,0), TABLE_NAME VARCHAR(16777216), SCHEMA_ID NUMBER(38,0), SCHEMA_NAME VARCHAR(16777216), DATABASE_ID NUMBER(38,0), DATABASE_NAME VARCHAR(16777216));';
queries[14] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.METERING_DAILY_HISTORY(SERVICE_TYPE VARCHAR(25), USAGE_DATE DATE, CREDITS_USED_COMPUTE NUMBER(38,9), CREDITS_USED_CLOUD_SERVICES NUMBER(38,9), CREDITS_USED NUMBER(38,9), CREDITS_ADJUSTMENT_CLOUD_SERVICES NUMBER(38,10), CREDITS_BILLED NUMBER(38,10));';
queries[15] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.METERING_HISTORY(SERVICE_TYPE VARCHAR(26), START_TIME TIMESTAMP_LTZ(6), END_TIME TIMESTAMP_LTZ(6), ENTITY_ID NUMBER(38,0), NAME VARCHAR(16777216), CREDITS_USED_COMPUTE NUMBER(38,9), CREDITS_USED_CLOUD_SERVICES NUMBER(38,9), CREDITS_USED NUMBER(38,9), BYTES VARIANT, "ROWS" NUMBER(38,0), FILES VARIANT, BUDGET_ID NUMBER(38,0));';
queries[16] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.PIPE_USAGE_HISTORY(PIPE_ID NUMBER(38,0), PIPE_NAME VARCHAR(16777216), START_TIME TIMESTAMP_LTZ(0), END_TIME TIMESTAMP_LTZ(9), CREDITS_USED NUMBER(38,9), BYTES_INSERTED FLOAT, FILES_INSERTED VARIANT);';
queries[17] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.PIPES(PIPE_ID NUMBER(38,0), PIPE_NAME VARCHAR(16777216), PIPE_SCHEMA_ID NUMBER(38,0), PIPE_SCHEMA VARCHAR(16777216), PIPE_CATALOG_ID NUMBER(38,0), PIPE_CATALOG VARCHAR(16777216), IS_AUTOINGEST_ENABLED VARCHAR(3), NOTIFICATION_CHANNEL_NAME VARCHAR(16777216), PIPE_OWNER VARCHAR(16777216), DEFINITION VARCHAR(16777216), CREATED TIMESTAMP_LTZ(6), LAST_ALTERED TIMESTAMP_LTZ(6), COMMENT VARCHAR(16777216), PATTERN VARCHAR(16777216), DELETED TIMESTAMP_LTZ(6), OWNER_ROLE_TYPE VARCHAR(13));';
queries[18] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.POLICY_REFERENCES(POLICY_DB VARCHAR(16777216), POLICY_SCHEMA VARCHAR(16777216), POLICY_ID NUMBER(38,0), POLICY_NAME VARCHAR(16777216), POLICY_KIND VARCHAR(17), REF_DATABASE_NAME VARCHAR(16777216), REF_SCHEMA_NAME VARCHAR(16777216), REF_ENTITY_NAME VARCHAR(16777216), REF_ENTITY_DOMAIN VARCHAR(16777216), REF_COLUMN_NAME VARCHAR(16777216), REF_ARG_COLUMN_NAMES VARCHAR(16777216), TAG_DATABASE VARCHAR(16777216), TAG_SCHEMA VARCHAR(16777216), TAG_NAME VARCHAR(16777216), POLICY_STATUS VARCHAR(16777216));';
queries[19] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.QUERY_HISTORY(QUERY_ID VARCHAR(16777216), QUERY_TEXT VARCHAR(16777216), DATABASE_ID NUMBER(38,0), DATABASE_NAME VARCHAR(16777216), SCHEMA_ID NUMBER(38,0), SCHEMA_NAME VARCHAR(16777216), QUERY_TYPE VARCHAR(16777216), SESSION_ID NUMBER(38,0), USER_NAME VARCHAR(16777216), ROLE_NAME VARCHAR(16777216), WAREHOUSE_ID NUMBER(38,0), WAREHOUSE_NAME VARCHAR(16777216), WAREHOUSE_SIZE VARCHAR(16777216), WAREHOUSE_TYPE VARCHAR(16777216), CLUSTER_NUMBER NUMBER(38,0), QUERY_TAG VARCHAR(16777216), EXECUTION_STATUS VARCHAR(16777216), ERROR_CODE VARCHAR(16777216), ERROR_MESSAGE VARCHAR(16777216), START_TIME TIMESTAMP_LTZ(6), END_TIME TIMESTAMP_LTZ(6), TOTAL_ELAPSED_TIME NUMBER(38,0), BYTES_SCANNED NUMBER(38,0), PERCENTAGE_SCANNED_FROM_CACHE FLOAT, BYTES_WRITTEN NUMBER(38,0), BYTES_WRITTEN_TO_RESULT NUMBER(38,0), BYTES_READ_FROM_RESULT NUMBER(38,0), ROWS_PRODUCED NUMBER(38,0), ROWS_INSERTED NUMBER(38,0), ROWS_UPDATED NUMBER(38,0), ROWS_DELETED NUMBER(38,0), ROWS_UNLOADED NUMBER(38,0), BYTES_DELETED NUMBER(38,0), PARTITIONS_SCANNED NUMBER(38,0), PARTITIONS_TOTAL NUMBER(38,0), BYTES_SPILLED_TO_LOCAL_STORAGE NUMBER(38,0), BYTES_SPILLED_TO_REMOTE_STORAGE NUMBER(38,0), BYTES_SENT_OVER_THE_NETWORK NUMBER(38,0), COMPILATION_TIME NUMBER(38,0), EXECUTION_TIME NUMBER(38,0), QUEUED_PROVISIONING_TIME NUMBER(38,0), QUEUED_REPAIR_TIME NUMBER(38,0), QUEUED_OVERLOAD_TIME NUMBER(38,0), TRANSACTION_BLOCKED_TIME NUMBER(38,0), OUTBOUND_DATA_TRANSFER_CLOUD VARCHAR(16777216), OUTBOUND_DATA_TRANSFER_REGION VARCHAR(16777216), OUTBOUND_DATA_TRANSFER_BYTES NUMBER(38,0), INBOUND_DATA_TRANSFER_CLOUD VARCHAR(16777216), INBOUND_DATA_TRANSFER_REGION VARCHAR(16777216), INBOUND_DATA_TRANSFER_BYTES NUMBER(38,0), LIST_EXTERNAL_FILES_TIME NUMBER(38,0), CREDITS_USED_CLOUD_SERVICES FLOAT, RELEASE_VERSION VARCHAR(16777216), EXTERNAL_FUNCTION_TOTAL_INVOCATIONS NUMBER(38,0), EXTERNAL_FUNCTION_TOTAL_SENT_ROWS NUMBER(38,0), EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS NUMBER(38,0), EXTERNAL_FUNCTION_TOTAL_SENT_BYTES NUMBER(38,0), EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES NUMBER(38,0), QUERY_LOAD_PERCENT NUMBER(38,0), IS_CLIENT_GENERATED_STATEMENT BOOLEAN, QUERY_ACCELERATION_BYTES_SCANNED NUMBER(38,0), QUERY_ACCELERATION_PARTITIONS_SCANNED NUMBER(38,0), QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR NUMBER(38,0), TRANSACTION_ID NUMBER(38,0), CHILD_QUERIES_WAIT_TIME NUMBER(38,0), ROLE_TYPE VARCHAR(16777216), QUERY_HASH VARCHAR(16777216), QUERY_HASH_VERSION NUMBER(38,0), QUERY_PARAMETERIZED_HASH VARCHAR(16777216), QUERY_PARAMETERIZED_HASH_VERSION NUMBER(38,0), CLIENT_APPLICATION_ID VARCHAR(16777216), CLIENT_ENVIRONMENT VARCHAR(16777216));';
queries[20] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.QUERY_HISTORY_RT(QUERY_ID VARCHAR(16777216), QUERY_TEXT VARCHAR(16777216), DATABASE_NAME VARCHAR(16777216), SCHEMA_NAME VARCHAR(16777216), QUERY_TYPE VARCHAR(16777216), SESSION_ID NUMBER(38,0), USER_NAME VARCHAR(16777216), ROLE_NAME VARCHAR(16777216), WAREHOUSE_NAME VARCHAR(16777216), WAREHOUSE_SIZE VARCHAR(16777216), WAREHOUSE_TYPE VARCHAR(16777216), CLUSTER_NUMBER NUMBER(38,0), QUERY_TAG VARCHAR(16777216), EXECUTION_STATUS VARCHAR(16777216), ERROR_CODE NUMBER(38,0), ERROR_MESSAGE VARCHAR(16777216), START_TIME TIMESTAMP_LTZ(3), END_TIME TIMESTAMP_LTZ(3), TOTAL_ELAPSED_TIME NUMBER(38,0), BYTES_SCANNED NUMBER(38,0), ROWS_PRODUCED NUMBER(38,0), COMPILATION_TIME NUMBER(38,0), EXECUTION_TIME NUMBER(38,0), QUEUED_PROVISIONING_TIME NUMBER(38,0), QUEUED_REPAIR_TIME NUMBER(38,0), QUEUED_OVERLOAD_TIME NUMBER(38,0), TRANSACTION_BLOCKED_TIME NUMBER(38,0), OUTBOUND_DATA_TRANSFER_CLOUD VARCHAR(16777216), OUTBOUND_DATA_TRANSFER_REGION VARCHAR(16777216), OUTBOUND_DATA_TRANSFER_BYTES NUMBER(38,0), INBOUND_DATA_TRANSFER_CLOUD VARCHAR(16777216), INBOUND_DATA_TRANSFER_REGION VARCHAR(16777216), INBOUND_DATA_TRANSFER_BYTES NUMBER(38,0), CREDITS_USED_CLOUD_SERVICES NUMBER(38,9), LIST_EXTERNAL_FILE_TIME NUMBER(38,0), RELEASE_VERSION VARCHAR(16777216), EXTERNAL_FUNCTION_TOTAL_INVOCATIONS NUMBER(38,0), EXTERNAL_FUNCTION_TOTAL_SENT_ROWS NUMBER(38,0), EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS NUMBER(38,0), EXTERNAL_FUNCTION_TOTAL_SENT_BYTES NUMBER(38,0), EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES NUMBER(38,0), IS_CLIENT_GENERATED_STATEMENT BOOLEAN);';
queries[21] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.REPLICATION_USAGE_HISTORY(START_TIME TIMESTAMP_LTZ(6), END_TIME TIMESTAMP_LTZ(9), DATABASE_NAME VARCHAR(16777216), DATABASE_ID NUMBER(38,0), CREDITS_USED NUMBER(38,9), BYTES_TRANSFERRED NUMBER(38,0));';
queries[22] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.ROLES(ROLE_ID NUMBER(38,0), CREATED_ON TIMESTAMP_LTZ(6), DELETED_ON TIMESTAMP_LTZ(6), NAME VARCHAR(16777216), COMMENT VARCHAR(16777216), OWNER VARCHAR(16777216), ROLE_TYPE VARCHAR(16777216), ROLE_DATABASE_NAME VARCHAR(16777216), ROLE_INSTANCE_ID NUMBER(38,0));';
queries[23] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.SCHEMATA(SCHEMA_ID NUMBER(38,0), SCHEMA_NAME VARCHAR(16777216), CATALOG_ID NUMBER(38,0), CATALOG_NAME VARCHAR(16777216), SCHEMA_OWNER VARCHAR(16777216), RETENTION_TIME NUMBER(38,0), IS_TRANSIENT VARCHAR(3), IS_MANAGED_ACCESS VARCHAR(3), DEFAULT_CHARACTER_SET_CATALOG VARCHAR(16777216), DEFAULT_CHARACTER_SET_SCHEMA VARCHAR(16777216), DEFAULT_CHARACTER_SET_NAME VARCHAR(16777216), SQL_PATH VARCHAR(16777216), COMMENT VARCHAR(16777216), CREATED TIMESTAMP_LTZ(6), LAST_ALTERED TIMESTAMP_LTZ(6), DELETED TIMESTAMP_LTZ(6), OWNER_ROLE_TYPE VARCHAR(13));';
queries[24] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.SEARCH_OPTIMIZATION_HISTORY(START_TIME TIMESTAMP_LTZ(6), END_TIME TIMESTAMP_LTZ(9), CREDITS_USED NUMBER(38,9), TABLE_ID NUMBER(38,0), TABLE_NAME VARCHAR(16777216), SCHEMA_ID NUMBER(38,0), SCHEMA_NAME VARCHAR(16777216), DATABASE_ID NUMBER(38,0), DATABASE_NAME VARCHAR(16777216));';
queries[25] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.SESSIONS(SESSION_ID NUMBER(38,0), CREATED_ON TIMESTAMP_LTZ(6), USER_NAME VARCHAR(16777216), AUTHENTICATION_METHOD VARCHAR(16777216), LOGIN_EVENT_ID NUMBER(38,0), CLIENT_APPLICATION_VERSION VARCHAR(16777216), CLIENT_APPLICATION_ID VARCHAR(16777216), CLIENT_ENVIRONMENT VARCHAR(16777216), CLIENT_BUILD_ID VARCHAR(16777216), CLIENT_VERSION VARCHAR(16777216), CLOSED_REASON VARCHAR(16777216));';
queries[26] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.STAGE_STORAGE_USAGE_HISTORY(USAGE_DATE DATE, AVERAGE_STAGE_BYTES NUMBER(38,6));';
queries[27] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.STAGES(STAGE_ID NUMBER(38,0), STAGE_NAME VARCHAR(16777216), STAGE_SCHEMA_ID NUMBER(38,0), STAGE_SCHEMA VARCHAR(16777216), STAGE_CATALOG_ID NUMBER(38,0), STAGE_CATALOG VARCHAR(16777216), STAGE_URL VARCHAR(16777216), STAGE_REGION VARCHAR(16777216), STAGE_TYPE VARCHAR(16777216), STAGE_OWNER VARCHAR(16777216), COMMENT VARCHAR(16777216), CREATED TIMESTAMP_LTZ(6), LAST_ALTERED TIMESTAMP_LTZ(6), DELETED TIMESTAMP_LTZ(6), OWNER_ROLE_TYPE VARCHAR(13), INSTANCE_ID NUMBER(38,0), ENDPOINT VARCHAR(16777216), DIRECTORY_ENABLED BOOLEAN);';
queries[28] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.STORAGE_USAGE(USAGE_DATE DATE, STORAGE_BYTES NUMBER(38,6), STAGE_BYTES NUMBER(38,6), FAILSAFE_BYTES NUMBER(38,6), HYBRID_TABLE_STORAGE_BYTES NUMBER(38,6));';
queries[29] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.TABLE_STORAGE_METRICS(ID NUMBER(38,0), TABLE_NAME VARCHAR(16777216), TABLE_SCHEMA_ID NUMBER(38,0), TABLE_SCHEMA VARCHAR(16777216), TABLE_CATALOG_ID NUMBER(38,0), TABLE_CATALOG VARCHAR(16777216), CLONE_GROUP_ID NUMBER(38,0), IS_TRANSIENT VARCHAR(3), ACTIVE_BYTES NUMBER(38,0), TIME_TRAVEL_BYTES NUMBER(38,0), FAILSAFE_BYTES NUMBER(38,0), RETAINED_FOR_CLONE_BYTES NUMBER(38,0), DELETED BOOLEAN, TABLE_CREATED TIMESTAMP_LTZ(6), TABLE_DROPPED TIMESTAMP_LTZ(6), TABLE_ENTERED_FAILSAFE TIMESTAMP_LTZ(6), SCHEMA_CREATED TIMESTAMP_LTZ(6), SCHEMA_DROPPED TIMESTAMP_LTZ(6), CATALOG_CREATED TIMESTAMP_LTZ(6), CATALOG_DROPPED TIMESTAMP_LTZ(6), COMMENT VARCHAR(16777216));';
queries[30] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.TABLES(TABLE_ID NUMBER(38,0), TABLE_NAME VARCHAR(16777216), TABLE_SCHEMA_ID NUMBER(38,0), TABLE_SCHEMA VARCHAR(16777216), TABLE_CATALOG_ID NUMBER(38,0), TABLE_CATALOG VARCHAR(16777216), TABLE_OWNER VARCHAR(16777216), TABLE_TYPE VARCHAR(16777216), IS_TRANSIENT VARCHAR(3), CLUSTERING_KEY VARCHAR(16777216), ROW_COUNT NUMBER(38,0), BYTES NUMBER(38,0), RETENTION_TIME NUMBER(38,0), SELF_REFERENCING_COLUMN_NAME VARCHAR(16777216), REFERENCE_GENERATION VARCHAR(16777216), USER_DEFINED_TYPE_CATALOG VARCHAR(16777216), USER_DEFINED_TYPE_SCHEMA VARCHAR(16777216), USER_DEFINED_TYPE_NAME VARCHAR(16777216), IS_INSERTABLE_INTO VARCHAR(3), IS_TYPED VARCHAR(3), COMMIT_ACTION VARCHAR(16777216), CREATED TIMESTAMP_LTZ(6), LAST_ALTERED TIMESTAMP_LTZ(6), LAST_DDL TIMESTAMP_LTZ(6), LAST_DDL_BY VARCHAR(16777216), DELETED TIMESTAMP_LTZ(6), AUTO_CLUSTERING_ON VARCHAR(3), COMMENT VARCHAR(16777216), OWNER_ROLE_TYPE VARCHAR(13), INSTANCE_ID NUMBER(38,0));';
queries[31] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.TASK_HISTORY(NAME VARCHAR(16777216), QUERY_TEXT VARCHAR(16777216), CONDITION_TEXT VARCHAR(16777216), SCHEMA_NAME VARCHAR(16777216), TASK_SCHEMA_ID NUMBER(38,0), DATABASE_NAME VARCHAR(16777216), TASK_DATABASE_ID NUMBER(38,0), SCHEDULED_TIME TIMESTAMP_LTZ(3), COMPLETED_TIME TIMESTAMP_LTZ(3), STATE VARCHAR(25), RETURN_VALUE VARCHAR(16777216), QUERY_ID VARCHAR(16777216), QUERY_START_TIME TIMESTAMP_LTZ(3), ERROR_CODE VARCHAR(16777216), ERROR_MESSAGE VARCHAR(16777216), GRAPH_VERSION NUMBER(38,0), RUN_ID NUMBER(38,0), ROOT_TASK_ID VARCHAR(16777216), SCHEDULED_FROM VARCHAR(12), INSTANCE_ID NUMBER(38,0), ATTEMPT_NUMBER NUMBER(38,0), CONFIG VARCHAR(16777216), QUERY_HASH VARCHAR(16777216), QUERY_HASH_VERSION NUMBER(38,0), QUERY_PARAMETERIZED_HASH VARCHAR(16777216), QUERY_PARAMETERIZED_HASH_VERSION NUMBER(38,0), GRAPH_RUN_GROUP_ID VARCHAR(16777216));';
queries[32] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.USERS(USER_ID NUMBER(38,0), NAME VARCHAR(16777216), CREATED_ON TIMESTAMP_LTZ(6), DELETED_ON TIMESTAMP_LTZ(6), LOGIN_NAME VARCHAR(16777216), DISPLAY_NAME VARCHAR(16777216), FIRST_NAME VARCHAR(16777216), LAST_NAME VARCHAR(16777216), EMAIL VARCHAR(16777216), MUST_CHANGE_PASSWORD BOOLEAN, HAS_PASSWORD BOOLEAN, COMMENT VARCHAR(16777216), DISABLED VARIANT, SNOWFLAKE_LOCK VARIANT, DEFAULT_WAREHOUSE VARCHAR(16777216), DEFAULT_NAMESPACE VARCHAR(16777216), DEFAULT_ROLE VARCHAR(16777216), EXT_AUTHN_DUO VARIANT, EXT_AUTHN_UID VARCHAR(16777216), BYPASS_MFA_UNTIL TIMESTAMP_LTZ(6), LAST_SUCCESS_LOGIN TIMESTAMP_LTZ(6), EXPIRES_AT TIMESTAMP_LTZ(6), LOCKED_UNTIL_TIME TIMESTAMP_LTZ(6), HAS_RSA_PUBLIC_KEY BOOLEAN, PASSWORD_LAST_SET_TIME TIMESTAMP_LTZ(6), OWNER VARCHAR(16777216), DEFAULT_SECONDARY_ROLE VARCHAR(16777216));';
queries[33] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.VIEWS(TABLE_ID NUMBER(38,0), TABLE_NAME VARCHAR(16777216), TABLE_SCHEMA_ID NUMBER(38,0), TABLE_SCHEMA VARCHAR(16777216), TABLE_CATALOG_ID NUMBER(38,0), TABLE_CATALOG VARCHAR(16777216), TABLE_OWNER VARCHAR(16777216), VIEW_DEFINITION VARCHAR(16777216), CHECK_OPTION VARCHAR(4), IS_UPDATABLE VARCHAR(2), INSERTABLE_INTO VARCHAR(2), IS_SECURE VARCHAR(3), CREATED TIMESTAMP_LTZ(6), LAST_ALTERED TIMESTAMP_LTZ(6), LAST_DDL TIMESTAMP_LTZ(6), LAST_DDL_BY VARCHAR(16777216), DELETED TIMESTAMP_LTZ(6), COMMENT VARCHAR(16777216), OWNER_ROLE_TYPE VARCHAR(13), INSTANCE_ID NUMBER(38,0));';
queries[34] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.WAREHOUSE_EVENTS_HISTORY(TIMESTAMP TIMESTAMP_LTZ(6), WAREHOUSE_ID NUMBER(38,0), WAREHOUSE_NAME VARCHAR(16777216), CLUSTER_NUMBER NUMBER(38,0), EVENT_NAME VARCHAR(16777216), EVENT_REASON VARCHAR(16777216), EVENT_STATE VARCHAR(16777216), USER_NAME VARCHAR(16777216), ROLE_NAME VARCHAR(16777216), QUERY_ID VARCHAR(16777216));';
queries[35] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.WAREHOUSE_LOAD_HISTORY(START_TIME TIMESTAMP_LTZ(6), END_TIME TIMESTAMP_LTZ(9), WAREHOUSE_ID NUMBER(38,0), WAREHOUSE_NAME VARCHAR(16777216), AVG_RUNNING NUMBER(38,9), AVG_QUEUED_LOAD NUMBER(38,9), AVG_QUEUED_PROVISIONING NUMBER(38,9), AVG_BLOCKED NUMBER(38,9));';
queries[36] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.WAREHOUSE_METERING_HISTORY(START_TIME TIMESTAMP_LTZ(0), END_TIME TIMESTAMP_LTZ(0), WAREHOUSE_ID NUMBER(38,0), WAREHOUSE_NAME VARCHAR(16777216), CREDITS_USED NUMBER(38,9), CREDITS_USED_COMPUTE NUMBER(38,9), CREDITS_USED_CLOUD_SERVICES NUMBER(38,9));';
queries[37] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.WAREHOUSE_PARAMETERS(WAREHOUSE VARCHAR(1000), KEY VARCHAR(1000), VALUE VARCHAR(1000), DEFAULT VARCHAR(1000), LEVEL VARCHAR(1000), DESCRIPTION VARCHAR(10000), TYPE VARCHAR(100));';
queries[38] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.WAREHOUSES(NAME VARCHAR(16777216), STATE VARCHAR(16777216), TYPE VARCHAR(16777216), SIZE VARCHAR(16777216), MIN_CLUSTER_COUNT NUMBER(38,0), MAX_CLUSTER_COUNT NUMBER(38,0), STARTED_CLUSTERS NUMBER(38,0), RUNNING NUMBER(38,0), QUEUED NUMBER(38,0), IS_DEFAULT VARCHAR(1), IS_CURRENT VARCHAR(1), AUTO_SUSPEND NUMBER(38,0), AUTO_RESUME VARCHAR(16777216), AVAILABLE NUMBER(38,0), PROVISIONING NUMBER(38,0), QUIESCING NUMBER(38,0), OTHER NUMBER(38,0), CREATED_ON TIMESTAMP_LTZ(9), RESUMED_ON TIMESTAMP_LTZ(9), UPDATED_ON TIMESTAMP_LTZ(9), OWNER VARCHAR(16777216), COMMENT VARCHAR(16777216), ENABLE_QUERY_ACCELERATION VARCHAR(16777216), QUERY_ACCELERATION_MAX_SCALE_FACTOR NUMBER(38,0), RESOURCE_MONITOR VARCHAR(16777216), ACTIVES NUMBER(38,0), PENDINGS NUMBER(38,0), FAILED NUMBER(38,0), SUSPENDED NUMBER(38,0), UUID VARCHAR(16777216), SCALING_POLICY VARCHAR(16777216), BUDGET VARCHAR(16777216));';
queries[39] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.REPLICATION_GROUP_USAGE_HISTORY (START_TIME TIMESTAMP_LTZ(6), END_TIME TIMESTAMP_LTZ(9), REPLICATION_GROUP_NAME VARCHAR(16777216), REPLICATION_GROUP_ID NUMBER(38,0), CREDITS_USED NUMBER(38,9), BYTES_TRANSFERRED NUMBER(38,0));';
queries[40] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.DATA_TRANSFER_HISTORY (START_TIME TIMESTAMP_LTZ(9), END_TIME TIMESTAMP_LTZ(9), SOURCE_CLOUD VARCHAR(16777216), SOURCE_REGION VARCHAR(16777216), TARGET_CLOUD VARCHAR(16777216), TARGET_REGION VARCHAR(16777216), BYTES_TRANSFERRED FLOAT, TRANSFER_TYPE VARCHAR(16777216));';
queries[41] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY (START_TIME TIMESTAMP_LTZ(6), END_TIME TIMESTAMP_LTZ(9), CREDITS_USED NUMBER(38,9), NUM_BYTES_MIGRATED NUMBER(38,0), NUM_ROWS_MIGRATED NUMBER(38,0), TABLE_ID NUMBER(38,0), TABLE_NAME VARCHAR(16777216), SCHEMA_ID NUMBER(38,0), SCHEMA_NAME VARCHAR(16777216), DATABASE_ID NUMBER(38,0), DATABASE_NAME VARCHAR(16777216));';
queries[42] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.AUTO_REFRESH_REGISTRATION_HISTORY (START_TIME TIMESTAMP_LTZ(9), END_TIME TIMESTAMP_LTZ(9), OBJECT_NAME VARCHAR(16777216), OBJECT_TYPE VARCHAR(16777216), CREDITS_USED VARCHAR(16777216), FILES_REGISTERED NUMBER(38,0));';
queries[43] = 'CREATE OR REPLACE TABLE ' + dbName + '.' + schemaName + '.TAG_REFERENCES (TAG_DATABASE VARCHAR(16777216), TAG_SCHEMA VARCHAR(16777216), TAG_ID NUMBER(38,0), TAG_NAME VARCHAR(16777216), TAG_VALUE VARCHAR(16777216), OBJECT_DATABASE VARCHAR(16777216), OBJECT_SCHEMA VARCHAR(16777216), OBJECT_ID NUMBER(38,0), OBJECT_NAME VARCHAR(16777216), OBJECT_DELETED TIMESTAMP_LTZ(6), DOMAIN VARCHAR(16777216), COLUMN_ID NUMBER(38,0), COLUMN_NAME VARCHAR(16777216));';

var returnVal = "SUCCESS";
var error = "";
for (let i = 0; i < queries.length; i++) {
    var stmt = snowflake.createStatement({sqlText:queries[i]});
    try
    {
        stmt.execute();
    }
    catch (err)
    {
        error += "Failed: " + err;
    }
}
if(error.length > 0 ) {
    return error;
}
return returnVal;
$$;
