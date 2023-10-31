create or replace stage &{stage_name};
create or replace file format &{file_format} type = 'csv' field_delimiter = ',' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ;

copy into @&{stage_name}/query_history.csv from (SELECT qh.QUERY_ID, qh.QUERY_TEXT, qh.DATABASE_ID, qh.DATABASE_NAME, qh.SCHEMA_ID, qh.SCHEMA_NAME, qh.QUERY_TYPE, qh.SESSION_ID, qh.USER_NAME, qh.ROLE_NAME, qh.WAREHOUSE_ID, qh.WAREHOUSE_NAME, qh.WAREHOUSE_SIZE, qh.WAREHOUSE_TYPE, qh.CLUSTER_NUMBER, qh.QUERY_TAG, qh.EXECUTION_STATUS, qh.ERROR_CODE, qh.ERROR_MESSAGE, qh.START_TIME, qh.END_TIME, qh.TOTAL_ELAPSED_TIME, qh.BYTES_SCANNED, qh.PERCENTAGE_SCANNED_FROM_CACHE, qh.BYTES_WRITTEN, qh.BYTES_WRITTEN_TO_RESULT, qh.BYTES_READ_FROM_RESULT, qh.ROWS_PRODUCED, qh.ROWS_INSERTED, qh.ROWS_UPDATED, qh.ROWS_DELETED, qh.ROWS_UNLOADED, qh.BYTES_DELETED, qh.PARTITIONS_SCANNED, qh.PARTITIONS_TOTAL, qh.BYTES_SPILLED_TO_LOCAL_STORAGE, qh.BYTES_SPILLED_TO_REMOTE_STORAGE, qh.BYTES_SENT_OVER_THE_NETWORK, qh.COMPILATION_TIME, qh.EXECUTION_TIME, qh.QUEUED_PROVISIONING_TIME, qh.QUEUED_REPAIR_TIME, qh.QUEUED_OVERLOAD_TIME, qh.TRANSACTION_BLOCKED_TIME, qh.OUTBOUND_DATA_TRANSFER_CLOUD, qh.OUTBOUND_DATA_TRANSFER_REGION, qh.OUTBOUND_DATA_TRANSFER_BYTES, qh.INBOUND_DATA_TRANSFER_CLOUD, qh.INBOUND_DATA_TRANSFER_REGION, qh.INBOUND_DATA_TRANSFER_BYTES, qh.LIST_EXTERNAL_FILES_TIME, qh.CREDITS_USED_CLOUD_SERVICES, qh.RELEASE_VERSION, qh.EXTERNAL_FUNCTION_TOTAL_INVOCATIONS, qh.EXTERNAL_FUNCTION_TOTAL_SENT_ROWS, qh.EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS, qh.EXTERNAL_FUNCTION_TOTAL_SENT_BYTES, qh.EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES, qh.QUERY_LOAD_PERCENT, qh.IS_CLIENT_GENERATED_STATEMENT, qh.QUERY_ACCELERATION_BYTES_SCANNED, qh.QUERY_ACCELERATION_PARTITIONS_SCANNED, qh.QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR, qh.TRANSACTION_ID, qh.CHILD_QUERIES_WAIT_TIME, qh.ROLE_TYPE, s.CLIENT_APPLICATION_ID, s.CLIENT_ENVIRONMENT from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY as qh inner join SNOWFLAKE.ACCOUNT_USAGE.SESSIONS as s on qh.session_id = s.session_id WHERE START_TIME > dateadd(day, -15, current_timestamp) order by start_time) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
copy into @&{stage_name}/warehouse_load_history.csv from (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY WHERE START_TIME > dateadd(day, -15, current_timestamp) order by start_time) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
copy into @&{stage_name}/warehouse_events_history.csv from (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY WHERE timestamp > dateadd(day, -15, current_timestamp) order by timestamp) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
copy into @&{stage_name}/warehouse_metering_history.csv from (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WHERE START_TIME > dateadd(day, -15, current_timestamp) order by start_time) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
copy into @&{stage_name}/access_history.csv from (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY WHERE QUERY_START_TIME > dateadd(day, -15, current_timestamp) order by QUERY_START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
copy into @&{stage_name}/metering_history.csv from (SELECT SERVICE_TYPE, START_TIME, END_TIME, ENTITY_ID, NAME, CREDITS_USED_COMPUTE, CREDITS_USED_CLOUD_SERVICES, CREDITS_USED, BYTES, ROWS, FILES FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY WHERE START_TIME > dateadd(day, -15, current_timestamp) order by start_time) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
copy into @&{stage_name}/metering_daily_history.csv from (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY WHERE USAGE_DATE > dateadd(day, -15, current_timestamp) order by USAGE_DATE) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
copy into @&{stage_name}/tables.csv from (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES ORDER BY CREATED DESC LIMIT 10000 ) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;


get @&{stage_name}/query_history.csv file://&{path};
get @&{stage_name}/access_history.csv file://&{path};
get @&{stage_name}/warehouse_load_history.csv file://&{path};
get @&{stage_name}/warehouse_events_history.csv file://&{path};
get @&{stage_name}/warehouse_metering_history.csv file://&{path};
get @&{stage_name}/metering_history.csv file://&{path};
get @&{stage_name}/metering_daily_history.csv file://&{path};
get @&{stage_name}/tables.csv file://&{path};
