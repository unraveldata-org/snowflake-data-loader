CREATE OR REPLACE STAGE &{stage_name};
CREATE OR REPLACE FILE format &{file_format} type = 'csv' field_delimiter = ',' skip_header = 0 FIELD_OPTIONALLY_ENCLOSED_BY = '0x22';

PUT file://&{path}/*.gz @&{stage_name};
PUT file://&{path}/*.csv @&{stage_name};

TRUNCATE TABLE IF EXISTS ACCESS_HISTORY;
COPY INTO ACCESS_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'access_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM ACCESS_HISTORY;

TRUNCATE TABLE IF EXISTS AUTOMATIC_CLUSTERING_HISTORY;
COPY INTO AUTOMATIC_CLUSTERING_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'automatic_clustering_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM AUTOMATIC_CLUSTERING_HISTORY;

TRUNCATE TABLE IF EXISTS COLUMNS;
COPY INTO COLUMNS FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'columns.*.gz' on_error='continue';
SELECT COUNT(*) FROM COLUMNS;

TRUNCATE TABLE IF EXISTS COPY_HISTORY;
COPY INTO COPY_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'COPY_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM COPY_HISTORY;

TRUNCATE TABLE IF EXISTS DATABASE_STORAGE_USAGE_HISTORY;
COPY INTO DATABASE_STORAGE_USAGE_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'database_storage_usage_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM DATABASE_STORAGE_USAGE_HISTORY;

TRUNCATE TABLE IF EXISTS DATABASES;
COPY INTO DATABASES FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'databases.*.gz' on_error='continue';
SELECT COUNT(*) FROM DATABASES;

TRUNCATE TABLE IF EXISTS FILE_FORMATS;
COPY INTO FILE_FORMATS FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'file_formats.*.gz' on_error='continue';
SELECT COUNT(*) FROM FILE_FORMATS;

TRUNCATE TABLE IF EXISTS FUNCTIONS;
COPY INTO FUNCTIONS FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'functions.*.gz' on_error='continue';
SELECT COUNT(*) FROM FUNCTIONS;

TRUNCATE TABLE IF EXISTS GRANTS_TO_ROLES;
COPY INTO GRANTS_TO_ROLES FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'grants_to_roles.*.gz' on_error='continue';
SELECT COUNT(*) FROM GRANTS_TO_ROLES;

TRUNCATE TABLE IF EXISTS GRANTS_TO_USERS;
COPY INTO GRANTS_TO_USERS FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'grants_to_users.*.gz' on_error='continue';
SELECT COUNT(*) FROM GRANTS_TO_USERS;

TRUNCATE TABLE IF EXISTS LOAD_HISTORY;
COPY INTO LOAD_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'load_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM LOAD_HISTORY;

TRUNCATE TABLE IF EXISTS LOGIN_HISTORY;
COPY INTO LOGIN_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'login_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM LOGIN_HISTORY;

TRUNCATE TABLE IF EXISTS MASKING_POLICIES;
COPY INTO MASKING_POLICIES FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'masking_policies.*.gz' on_error='continue';
SELECT COUNT(*) FROM MASKING_POLICIES;

TRUNCATE TABLE IF EXISTS MATERIALIZED_VIEW_REFRESH_HISTORY;
COPY INTO MATERIALIZED_VIEW_REFRESH_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'materialized_view_refresh_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM MATERIALIZED_VIEW_REFRESH_HISTORY;

TRUNCATE TABLE IF EXISTS METERING_DAILY_HISTORY;
COPY INTO METERING_DAILY_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'metering_daily_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM METERING_DAILY_HISTORY;

TRUNCATE TABLE IF EXISTS METERING_HISTORY;
COPY INTO METERING_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'metering_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM METERING_HISTORY;

TRUNCATE TABLE IF EXISTS PIPE_USAGE_HISTORY;
COPY INTO PIPE_USAGE_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'pipe_usage_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM PIPE_USAGE_HISTORY;

TRUNCATE TABLE IF EXISTS PIPES;
COPY INTO PIPES FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'pipes.*.gz' on_error='continue';
SELECT COUNT(*) FROM PIPES;

TRUNCATE TABLE IF EXISTS POLICY_REFERENCES;
COPY INTO POLICY_REFERENCES FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'policy_references.*.gz' on_error='continue';
SELECT COUNT(*) FROM POLICY_REFERENCES;

TRUNCATE TABLE IF EXISTS QUERY_HISTORY;
COPY INTO QUERY_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'query_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM QUERY_HISTORY;

TRUNCATE TABLE IF EXISTS REPLICATION_USAGE_HISTORY;
COPY INTO REPLICATION_USAGE_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'replication_usage_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM REPLICATION_USAGE_HISTORY;

TRUNCATE TABLE IF EXISTS ROLES;
COPY INTO ROLES FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'roles.*.gz' on_error='continue';
SELECT COUNT(*) FROM ROLES;

TRUNCATE TABLE IF EXISTS SCHEMATA;
COPY INTO SCHEMATA FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'schemata.*.gz' on_error='continue';
SELECT COUNT(*) FROM SCHEMATA;

TRUNCATE TABLE IF EXISTS SEARCH_OPTIMIZATION_HISTORY;
COPY INTO SEARCH_OPTIMIZATION_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'search_optimization_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM SEARCH_OPTIMIZATION_HISTORY;

TRUNCATE TABLE IF EXISTS SESSIONS;
COPY INTO SESSIONS FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'sessions.*.gz' on_error='continue';
SELECT COUNT(*) FROM SESSIONS;

TRUNCATE TABLE IF EXISTS STAGE_STORAGE_USAGE_HISTORY;
COPY INTO STAGE_STORAGE_USAGE_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'stage_storage_usage_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM STAGE_STORAGE_USAGE_HISTORY;

TRUNCATE TABLE IF EXISTS STAGES;
COPY INTO STAGES FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'stages.*.gz' on_error='continue';
SELECT COUNT(*) FROM STAGES;

TRUNCATE TABLE IF EXISTS STORAGE_USAGE;
COPY INTO STORAGE_USAGE FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'storage_usage.*.gz' on_error='continue';
SELECT COUNT(*) FROM STORAGE_USAGE;

TRUNCATE TABLE IF EXISTS TABLE_STORAGE_METRICS;
COPY INTO TABLE_STORAGE_METRICS FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'table_storage_metrics.*.gz' on_error='continue';
SELECT COUNT(*) FROM TABLE_STORAGE_METRICS;

TRUNCATE TABLE IF EXISTS TABLES;
COPY INTO TABLES FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'tables.*.gz' on_error='continue';
SELECT COUNT(*) FROM TABLES;

TRUNCATE TABLE IF EXISTS TASK_HISTORY;
COPY INTO TASK_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'task_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM TASK_HISTORY;

TRUNCATE TABLE IF EXISTS USERS;
COPY INTO USERS FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'users.*.gz' on_error='continue';
SELECT COUNT(*) FROM USERS;

TRUNCATE TABLE IF EXISTS VIEWS;
COPY INTO VIEWS FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'views.*.gz' on_error='continue';
SELECT COUNT(*) FROM VIEWS;

TRUNCATE TABLE IF EXISTS WAREHOUSE_EVENTS_HISTORY;
COPY INTO WAREHOUSE_EVENTS_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'warehouse_events_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM WAREHOUSE_EVENTS_HISTORY;

TRUNCATE TABLE IF EXISTS WAREHOUSE_LOAD_HISTORY;
COPY INTO WAREHOUSE_LOAD_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'warehouse_load_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM WAREHOUSE_LOAD_HISTORY;

TRUNCATE TABLE IF EXISTS WAREHOUSE_METERING_HISTORY;
COPY INTO WAREHOUSE_METERING_HISTORY FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'warehouse_metering_history.*.gz' on_error='continue';
SELECT COUNT(*) FROM WAREHOUSE_METERING_HISTORY;

TRUNCATE TABLE IF EXISTS WAREHOUSE_PARAMETERS;
COPY INTO WAREHOUSE_PARAMETERS FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO) pattern = 'warehouse_parameters.*.gz' on_error='continue';
SELECT COUNT(*) FROM WAREHOUSE_PARAMETERS;

TRUNCATE TABLE IF EXISTS WAREHOUSES;
COPY INTO WAREHOUSES FROM @&{stage_name}/ file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '0x22' skip_header = 0 COMPRESSION = AUTO NULL_IF = ('NULL','null','')) pattern = 'warehouses.*.gz' on_error='continue';
SELECT COUNT(*) FROM WAREHOUSES;

TRUNCATE table if exists replication_group_usage_history;
COPY into replication_group_usage_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'replication_group_usage_history.*.gz' on_error='continue';
SELECT count(*) from replication_group_usage_history;
TRUNCATE table if exists data_transfer_history;
COPY into data_transfer_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'data_transfer_history.*.gz' on_error='continue';
SELECT count(*) from data_transfer_history;
TRUNCATE table if exists snowpipe_streaming_file_migration_history;
COPY into snowpipe_streaming_file_migration_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'snowpipe_streaming_file_migration_history.*.gz' on_error='continue';
SELECT count(*) from snowpipe_streaming_file_migration_history;
TRUNCATE table if exists auto_refresh_registration_history;
COPY into auto_refresh_registration_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'auto_refresh_registration_history.*.gz' on_error='continue';
SELECT count(*) from auto_refresh_registration_history;
TRUNCATE table if exists tag_references;
COPY into tag_references from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'tag_references.*.gz' on_error='continue';
SELECT count(*) from tag_references;

TRUNCATE table if exists QUERY_PROFILE;
COPY into QUERY_PROFILE from @unravel_stage/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'query_profile.csv.*.gz' on_error='continue';
SELECT count(*) from QUERY_PROFILE;