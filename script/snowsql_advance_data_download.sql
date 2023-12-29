CREATE OR REPLACE STAGE &{stage_name};
CREATE OR REPLACE FILE format &{file_format} type = 'csv' field_delimiter = ',' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '0x22';

COPY INTO @&{stage_name}/access_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY WHERE QUERY_START_TIME > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY QUERY_START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/automatic_clustering_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/columns.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS ORDER BY COLUMN_ID DESC LIMIT 10000) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/copy_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY WHERE LAST_LOAD_TIME > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY LAST_LOAD_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/database_storage_usage_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY WHERE USAGE_DATE > DATEADD(DAY, -15, CURRENT_DATE) ORDER BY USAGE_DATE) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/databases.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASES WHERE CREATED > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/file_formats.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.FILE_FORMATS WHERE CREATED > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/functions.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.FUNCTIONS WHERE CREATED > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/grants_to_roles.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES WHERE CREATED_ON > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY CREATED_ON) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/grants_to_users.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS WHERE CREATED_ON > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY CREATED_ON) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/load_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY WHERE LAST_LOAD_TIME > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY LAST_LOAD_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/login_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.LOGIN_HISTORY WHERE EVENT_TIMESTAMP > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY EVENT_TIMESTAMP) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/masking_policies.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.MASKING_POLICIES WHERE CREATED > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/materialized_view_refresh_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/metering_daily_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY WHERE USAGE_DATE > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY USAGE_DATE) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/metering_history.csv FROM (SELECT SERVICE_TYPE, START_TIME, END_TIME, ENTITY_ID, NAME, CREDITS_USED_COMPUTE, CREDITS_USED_CLOUD_SERVICES, CREDITS_USED, BYTES, "ROWS", FILES, BUDGET_ID FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/pipe_usage_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/pipes.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.PIPES WHERE CREATED > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/policy_references.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES ORDER BY POLICY_ID DESC LIMIT 10000) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/query_history.csv FROM (SELECT qh.*, s.CLIENT_APPLICATION_ID, s.CLIENT_ENVIRONMENT FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY AS qh INNER JOIN SNOWFLAKE.ACCOUNT_USAGE.SESSIONS AS s ON qh.session_id = s.session_id WHERE START_TIME > dateadd(day, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY start_time) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/replication_usage_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_USAGE_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/roles.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES WHERE CREATED_ON > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY CREATED_ON) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/schemata.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA WHERE CREATED > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/search_optimization_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/sessions.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS WHERE CREATED_ON > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY CREATED_ON) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/stage_storage_usage_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.STAGE_STORAGE_USAGE_HISTORY WHERE USAGE_DATE > DATEADD(DAY, -15, CURRENT_DATE) ORDER BY USAGE_DATE) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/stages.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.STAGES WHERE CREATED > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/storage_usage.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE WHERE USAGE_DATE > DATEADD(DAY, -15, CURRENT_DATE) ORDER BY USAGE_DATE) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/table_storage_metrics.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS WHERE TABLE_CREATED > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY TABLE_CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/tables.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES ORDER BY CREATED DESC LIMIT 10000) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/task_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY WHERE QUERY_START_TIME > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY QUERY_START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/users.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.USERS WHERE CREATED_ON > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY CREATED_ON) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/views.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.VIEWS WHERE CREATED > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY CREATED) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/warehouse_events_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY WHERE TIMESTAMP > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY TIMESTAMP) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/warehouse_load_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY INTO @&{stage_name}/warehouse_metering_history.csv FROM (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' ESCAPE_UNENCLOSED_FIELD = NONE) OVERWRITE=TRUE;
COPY into @&{stage_name}/replication_group_usage_history.csv from (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_USAGE_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY START_TIME DESC ) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
COPY into @&{stage_name}/data_transfer_history.csv from (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY START_TIME DESC ) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
COPY into @&{stage_name}/snowpipe_streaming_file_migration_history.csv from (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY WHERE START_TIME > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY START_TIME DESC ) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
COPY into @&{stage_name}/auto_refresh_registration_history.csv from (SELECT * FROM table(SNOWFLAKE.INFORMATION_SCHEMA.AUTO_REFRESH_REGISTRATION_HISTORY()) WHERE START_TIME > DATEADD(DAY, -15, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY START_TIME DESC ) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
COPY into @&{stage_name}/tag_references.csv from (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES LIMIT 10000) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
COPY into @&{stage_name}/query_profile.csv from (SELECT * FROM QUERY_PROFILE) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;


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
GET @&{stage_name}/replication_group_usage_history.csv file://&{path};
GET @&{stage_name}/data_transfer_history.csv file://&{path};
GET @&{stage_name}/snowpipe_streaming_file_migration_history.csv file://&{path};
GET @&{stage_name}/auto_refresh_registration_history.csv file://&{path};
GET @&{stage_name}/tag_references.csv file://&{path};
GET @&{stage_name}/query_profile.csv file://&{path};
