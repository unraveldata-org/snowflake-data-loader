CREATE OR REPLACE STAGE &{stage_name};
CREATE OR REPLACE FILE format &{file_format} type = 'csv' field_delimiter = ',' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '0x22';

COPY INTO @&{stage_name}/access_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY WHERE QUERY_START_TIME > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY QUERY_START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/automatic_clustering_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/columns.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS ORDER BY COLUMN_ID DESC LIMIT 10000) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/copy_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY WHERE LAST_LOAD_TIME > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY LAST_LOAD_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/database_storage_usage_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY WHERE USAGE_DATE > DATEADD(DAY, -15, CURRENT_DATE) ORDER BY USAGE_DATE) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/databases.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASES WHERE CREATED > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/file_formats.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.FILE_FORMATS WHERE CREATED > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/functions.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.FUNCTIONS WHERE CREATED > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/grants_to_roles.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES WHERE CREATED_ON > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY CREATED_ON) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/grants_to_users.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS WHERE CREATED_ON > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY CREATED_ON) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/load_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY WHERE LAST_LOAD_TIME > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY LAST_LOAD_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/login_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY WHERE EVENT_TIMESTAMP > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY EVENT_TIMESTAMP) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/masking_policies.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.MASKING_POLICIES WHERE CREATED > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/materialized_view_refresh_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/metering_daily_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY WHERE USAGE_DATE > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY USAGE_DATE) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/metering_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/pipe_usage_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/pipes.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.PIPES WHERE CREATED > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/policy_references.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES ORDER BY POLICY_ID DESC LIMIT 10000) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/query_history.csv FROM (SELECT qh.QUERY_ID, qh.QUERY_TEXT, qh.DATABASE_ID, qh.DATABASE_NAME, qh.SCHEMA_ID, qh.SCHEMA_NAME, qh.QUERY_TYPE, qh.SESSION_ID, qh.USER_NAME, qh.ROLE_NAME, qh.WAREHOUSE_ID, qh.WAREHOUSE_NAME, qh.WAREHOUSE_SIZE, qh.WAREHOUSE_TYPE, qh.CLUSTER_NUMBER, qh.QUERY_TAG, qh.EXECUTION_STATUS, qh.ERROR_CODE, qh.ERROR_MESSAGE, qh.START_TIME, qh.END_TIME, qh.TOTAL_ELAPSED_TIME, qh.BYTES_SCANNED, qh.PERCENTAGE_SCANNED_FROM_CACHE, qh.BYTES_WRITTEN, qh.BYTES_WRITTEN_TO_RESULT, qh.BYTES_READ_FROM_RESULT, qh.ROWS_PRODUCED, qh.ROWS_INSERTED, qh.ROWS_UPDATED, qh.ROWS_DELETED, qh.ROWS_UNLOADED, qh.BYTES_DELETED, qh.PARTITIONS_SCANNED, qh.PARTITIONS_TOTAL, qh.BYTES_SPILLED_TO_LOCAL_STORAGE, qh.BYTES_SPILLED_TO_REMOTE_STORAGE, qh.BYTES_SENT_OVER_THE_NETWORK, qh.COMPILATION_TIME, qh.EXECUTION_TIME, qh.QUEUED_PROVISIONING_TIME, qh.QUEUED_REPAIR_TIME, qh.QUEUED_OVERLOAD_TIME, qh.TRANSACTION_BLOCKED_TIME, qh.OUTBOUND_DATA_TRANSFER_CLOUD, qh.OUTBOUND_DATA_TRANSFER_REGION, qh.OUTBOUND_DATA_TRANSFER_BYTES, qh.INBOUND_DATA_TRANSFER_CLOUD, qh.INBOUND_DATA_TRANSFER_REGION, qh.INBOUND_DATA_TRANSFER_BYTES, qh.LIST_EXTERNAL_FILES_TIME, qh.CREDITS_USED_CLOUD_SERVICES, qh.RELEASE_VERSION, qh.EXTERNAL_FUNCTION_TOTAL_INVOCATIONS, qh.EXTERNAL_FUNCTION_TOTAL_SENT_ROWS, qh.EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS, qh.EXTERNAL_FUNCTION_TOTAL_SENT_BYTES, qh.EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES, qh.QUERY_LOAD_PERCENT, qh.IS_CLIENT_GENERATED_STATEMENT, qh.QUERY_ACCELERATION_BYTES_SCANNED, qh.QUERY_ACCELERATION_PARTITIONS_SCANNED, qh.QUERY_ACCELERATION_UPPER_LIMIT_SCALE_FACTOR, qh.TRANSACTION_ID, qh.CHILD_QUERIES_WAIT_TIME, qh.ROLE_TYPE, s.CLIENT_APPLICATION_ID, s.CLIENT_ENVIRONMENT FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY AS qh INNER JOIN SNOWFLAKE.ACCOUNT_USAGE.SESSIONS AS s ON qh.session_id = s.session_id WHERE START_TIME > dateadd(day, -15, current_timestamp) ORDER BY start_time) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/replication_usage_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/roles.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES WHERE CREATED_ON > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY CREATED_ON) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/schemata.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA WHERE CREATED > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/search_optimization_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/sessions.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS WHERE CREATED_ON > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY CREATED_ON) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/stage_storage_usage_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.STAGE_STORAGE_USAGE_HISTORY WHERE USAGE_DATE > DATEADD(DAY, -15, CURRENT_DATE) ORDER BY USAGE_DATE) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/stages.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.STAGES WHERE CREATED > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/storage_usage.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE WHERE USAGE_DATE > DATEADD(DAY, -15, CURRENT_DATE) ORDER BY USAGE_DATE) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/table_storage_metrics.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS WHERE TABLE_CREATED > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY TABLE_CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/tables.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES ORDER BY CREATED DESC LIMIT 10000) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/task_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY WHERE QUERY_START_TIME > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY QUERY_START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/users.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.USERS WHERE CREATED_ON > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY CREATED_ON) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/views.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.VIEWS WHERE CREATED > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/warehouse_events_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY WHERE TIMESTAMP > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY TIMESTAMP) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/warehouse_load_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/warehouse_metering_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CURRENT_TIMESTAMP) ORDER BY START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;


GET @&{stage_name}/access_history.csv file://&{path};
GET @&{stage_name}/automatic_clustering_history.csv file://&{path};
GET @&{stage_name}/columns.csv file://&{path};
GET @&{stage_name}/copy_history.csv file://&{path};
GET @&{stage_name}/database_storage_usage_history.csv file://&{path};
GET @&{stage_name}/databases.csv file://&{path};
GET @&{stage_name}/file_formats.csv file://&{path};
GET @&{stage_name}/functions.csv file://&{path};
GET @&{stage_name}/grants_to_roles.csv file://&{path};
GET @&{stage_name}/grants_to_users.csv file://&{path};
GET @&{stage_name}/load_history.csv file://&{path};
GET @&{stage_name}/login_history.csv file://&{path};
GET @&{stage_name}/masking_policies.csv file://&{path};
GET @&{stage_name}/materialized_view_refresh_history.csv file://&{path};
GET @&{stage_name}/metering_daily_history.csv file://&{path};
GET @&{stage_name}/metering_history.csv file://&{path};
GET @&{stage_name}/pipe_usage_history.csv file://&{path};
GET @&{stage_name}/pipes.csv file://&{path};
GET @&{stage_name}/policy_references.csv file://&{path};
GET @&{stage_name}/query_history.csv file://&{path};
GET @&{stage_name}/replication_usage_history.csv file://&{path};
GET @&{stage_name}/roles.csv file://&{path};
GET @&{stage_name}/schemata.csv file://&{path};
GET @&{stage_name}/search_optimization_history.csv file://&{path};
GET @&{stage_name}/sessions.csv file://&{path};
GET @&{stage_name}/stage_storage_usage_history.csv file://&{path};
GET @&{stage_name}/stages.csv file://&{path};
GET @&{stage_name}/storage_usage.csv file://&{path};
GET @&{stage_name}/table_storage_metrics.csv file://&{path};
GET @&{stage_name}/tables.csv file://&{path};
GET @&{stage_name}/task_history.csv file://&{path};
GET @&{stage_name}/users.csv file://&{path};
GET @&{stage_name}/views.csv file://&{path};
GET @&{stage_name}/warehouse_events_history.csv file://&{path};
GET @&{stage_name}/warehouse_load_history.csv file://&{path};
GET @&{stage_name}/warehouse_metering_history.csv file://&{path};
