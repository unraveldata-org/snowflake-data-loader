create or replace stage &{stage_name};
create or replace file format &{file_format} type = 'csv' field_delimiter = ',' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ;

copy into @&{stage_name}/query_history.csv from (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE START_TIME > dateadd(day, -15, current_timestamp) order by start_time) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
copy into @&{stage_name}/warehouse_load_history.csv from (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY WHERE START_TIME > dateadd(day, -15, current_timestamp) order by start_time) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
copy into @&{stage_name}/warehouse_events_history.csv from (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY WHERE timestamp > dateadd(day, -15, current_timestamp) order by timestamp) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
copy into @&{stage_name}/warehouse_metering_history.csv from (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WHERE START_TIME > dateadd(day, -15, current_timestamp) order by start_time) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
copy into @&{stage_name}/access_history.csv from (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY WHERE QUERY_START_TIME > dateadd(day, -15, current_timestamp) order by QUERY_START_TIME) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
copy into @&{stage_name}/metering_history.csv from (SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY WHERE START_TIME > dateadd(day, -15, current_timestamp) order by start_time) FILE_FORMAT=(TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE ) OVERWRITE=TRUE;
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