create or replace stage &{stage_name};
create or replace file format &{file_format} type = 'csv' field_delimiter = ',' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ;
put file://&{path}/*.gz @&{stage_name};
put file://&{path}/*.csv @&{stage_name};
truncate table if exists metering_history;
copy into metering_history from @&{stage_name}/ file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 COMPRESSION = AUTO ) pattern = 'metering_history.*.csv.gz' on_error='continue';
select count(*) from metering_history;
truncate table if exists metering_daily_history;
copy into metering_daily_history from @&{stage_name}/ file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 COMPRESSION = AUTO ) pattern = 'metering_daily_history.*.csv.gz' on_error='continue';
select count(*) from metering_daily_history;
truncate table if exists tables;
copy into tables from @&{stage_name}/ file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 COMPRESSION = AUTO ) pattern = 'tables.*.csv.gz' on_error='continue';
select count(*) from tables;
truncate table if exists warehouse_load_history;
copy into warehouse_load_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'warehouse_load_history.*.gz' on_error='continue';
select count(*) from warehouse_load_history;
truncate table if exists warehouse_events_history;
copy into warehouse_events_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'warehouse_events_history.*.gz' on_error='continue';
select count(*) from warehouse_events_history;
truncate table if exists warehouse_metering_history;
copy into warehouse_metering_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'warehouse_metering_history.*.gz' on_error='continue';
select count(*) from warehouse_metering_history;
truncate table if exists query_history;
copy into query_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'query_history.*.gz' on_error='continue';
select count(*) from query_history;
truncate table if exists access_history;
copy into access_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'access_history.*.gz' on_error='continue';
select count(*) from access_history;
truncate table if exists warehouses;
copy into warehouses from @&{stage_name}/  file_format = (type = 'csv' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = ('NULL','null','')) PATTERN = 'warehouses.*.gz' on_error='continue';
select count(*) from warehouses;
truncate table if exists warehouse_parameters;
copy into warehouse_parameters from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'warehouse_parameters.*.gz' on_error='continue';
select count(*) from warehouse_parameters;
truncate table if exists database_replication_usage_history;
copy into database_replication_usage_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'database_replication_usage_history.*.gz' on_error='continue';
select count(*) from database_replication_usage_history;
truncate table if exists replication_group_usage_history;
copy into replication_group_usage_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'replication_group_usage_history.*.gz' on_error='continue';
select count(*) from replication_group_usage_history;
truncate table if exists database_storage_usage_history;
copy into database_storage_usage_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'database_storage_usage_history.*.gz' on_error='continue';
select count(*) from database_storage_usage_history;
truncate table if exists stage_storage_usage_history;
copy into stage_storage_usage_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'stage_storage_usage_history.*.gz' on_error='continue';
select count(*) from stage_storage_usage_history;
truncate table if exists search_optimization_history;
copy into search_optimization_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'search_optimization_history.*.gz' on_error='continue';
select count(*) from search_optimization_history;
truncate table if exists data_transfer_history;
copy into data_transfer_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'data_transfer_history.*.gz' on_error='continue';
select count(*) from data_transfer_history;
truncate table if exists automatic_clustering_history;
copy into automatic_clustering_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'automatic_clustering_history.*.gz' on_error='continue';
select count(*) from automatic_clustering_history;
truncate table if exists snowpipe_streaming_file_migration_history;
copy into snowpipe_streaming_file_migration_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'snowpipe_streaming_file_migration_history.*.gz' on_error='continue';
select count(*) from snowpipe_streaming_file_migration_history;
truncate table if exists auto_refresh_registration_history;
copy into auto_refresh_registration_history from @&{stage_name}/  file_format = (type = csv FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1) PATTERN = 'auto_refresh_registration_history.*.gz' on_error='continue';
select count(*) from auto_refresh_registration_history;
