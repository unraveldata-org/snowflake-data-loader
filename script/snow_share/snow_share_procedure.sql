/**
   Update/Set these below fields
   DATABASE_TO_SHARE, SCHEMA_TO_SHARE, SHARE_NAME, PROFILE_QUERY_CREDIT, ACCOUNT_ID,
   R_DAYS(Real time query to poll), H_DAYS(History Query to poll),
   DAYS_TO_KEEP(query and access history table data to keep), WAREHOUSE_NAME(to run tasks),
   Task Schedule -> (REPLICATE_METADATA, REPLICATE_STORAGE_METADATA, REPLICATE_HISTORY_QUERY, CREATE_PROFILE_TABLE, REPLICATE_WAREHOUSE_AND_REALTIME_QUERY, CLEANUP_DATA_TASK)
*/

SET DATABASE_TO_SHARE = 'UNRAVEL_DB_SHARE';
SET SCHEMA_TO_SHARE = 'UNRAVEL_SCHEMA_SHARE';
SET SHARE_NAME = 'UNRAVEL_SHARE';
SET PROFILE_QUERY_CREDIT = '1';
SET ACCOUNT_ID = '<UNRAVEL_ACCOUNT>';

/**
  Number of days data will be polled ;
*/
SET R_DAYS = '1';
SET H_DAYS = '2';
SET DAYS_TO_KEEP = '5';
SET WAREHOUSE_NAME = '<WAREHOUSE_NAME>';
SET REPLICATE_METADATA = 'USING CRON 30 * * * * UTC';
SET REPLICATE_STORAGE_METADATA = '720 MINUTE';
SET REPLICATE_HISTORY_QUERY = 'USING CRON 30 * * * * UTC';
SET CREATE_PROFILE_TABLE = 'USING CRON 30 * * * * UTC';
SET REPLICATE_WAREHOUSE_AND_REALTIME_QUERY = '30 MINUTE';
SET CLEANUP_DATA_TASK = '1440 MINUTE';

CREATE DATABASE IF NOT EXISTS IDENTIFIER($DATABASE_TO_SHARE);
USE IDENTIFIER($DATABASE_TO_SHARE);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER($SCHEMA_TO_SHARE);
USE SCHEMA IDENTIFIER($SCHEMA_TO_SHARE);

CREATE OR REPLACE TABLE config_parameters (
    DATE DATE DEFAULT CURRENT_DATE,
    CONFIG_ID VARCHAR(255) ,
    VALUE VARCHAR(255),
    IS_VALID BOOLEAN
);

INSERT INTO config_parameters (CONFIG_ID, VALUE, IS_VALID)
VALUES
('DATABASE_TO_SHARE', $DATABASE_TO_SHARE , TRUE),
('SCHEMA_TO_SHARE', $SCHEMA_TO_SHARE , TRUE),
('SHARE_NAME', $SHARE_NAME, TRUE),
('PROFILE_QUERY_CREDIT', $PROFILE_QUERY_CREDIT, TRUE),
('ACCOUNT_ID', $ACCOUNT_ID, TRUE),
('R_DAYS', $R_DAYS, TRUE),
('H_DAYS', $H_DAYS, TRUE),
('DAYS_TO_KEEP', $DAYS_TO_KEEP, TRUE),
('WAREHOUSE_NAME', $WAREHOUSE_NAME, TRUE),
('REPLICATE_METADATA', $REPLICATE_METADATA, TRUE),
('REPLICATE_STORAGE_METADATA', $REPLICATE_STORAGE_METADATA, TRUE),
('REPLICATE_HISTORY_QUERY', $REPLICATE_HISTORY_QUERY, TRUE),
('CREATE_PROFILE_TABLE', $CREATE_PROFILE_TABLE, TRUE),
('REPLICATE_WAREHOUSE_AND_REALTIME_QUERY', $REPLICATE_WAREHOUSE_AND_REALTIME_QUERY, TRUE),
('CLEANUP_DATA_TASK', $CLEANUP_DATA_TASK, TRUE);

CREATE OR REPLACE PROCEDURE create_table_from_snowflake(DATABASE_NAME STRING, SCHEMA_NAME STRING, TABLE_NAME STRING)
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$
  try {
    var col_list = "";

    // Query to columns and data type for TABLE_NAME
    var sql_command = `
      SELECT COLUMN_NAME || ' ' || DATA_TYPE AS column_definition
      FROM SNOWFLAKE.INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_NAME = :1
        AND TABLE_SCHEMA = 'ACCOUNT_USAGE'
        AND TABLE_CATALOG = 'SNOWFLAKE'
    `;

    var statement = snowflake.createStatement({
      sqlText: sql_command,
      binds: [TABLE_NAME]  // Binding the TABLE_NAME parameter
    });

    var result = statement.execute();

    while (result.next()) {
      var column_definition = result.getColumnValue(1);
      col_list = col_list ? col_list + ', ' + column_definition : column_definition;
    }

    col_list = col_list + ', INSERT_TIME TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()';

    var create_table_sql = `
      CREATE OR REPLACE TRANSIENT TABLE ${DATABASE_NAME}.${SCHEMA_NAME}.${TABLE_NAME} (${col_list}) DATA_RETENTION_TIME_IN_DAYS = 0; `;

    // Execute the CREATE TABLE statement
    var create_statement = snowflake.createStatement({sqlText: create_table_sql});
    create_statement.execute();

    return 'Table ' + DATABASE_NAME + '.' + SCHEMA_NAME + '.' + TABLE_NAME + ' created successfully.';
  } catch (err) {
    return 'Failed to create table: ' + err.message;
  }
$$;


CREATE OR REPLACE PROCEDURE CREATE_TABLES(DB STRING, SCHEMA STRING)
RETURNS STRING NOT NULL
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
use_statement VARCHAR;
res RESULTSET;
BEGIN

use_statement := 'USE ' || DB || '.' || SCHEMA;
res := (EXECUTE IMMEDIATE :use_statement);

CREATE OR REPLACE TRANSIENT TABLE replication_log (
  eventDate  TIMESTAMP_TZ(9) DEFAULT to_timestamp_tz(current_timestamp),
  executionStatus VARCHAR(1000) DEFAULT NULL,
  remarks VARCHAR(1000),
  taskName VARCHAR(500) DEFAULT NULL
);

CREATE OR REPLACE TRANSIENT TABLE WAREHOUSE_METERING_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE WAREHOUSE_EVENTS_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE WAREHOUSE_LOAD_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE TABLES WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.TABLES;
CREATE OR REPLACE TRANSIENT TABLE TABLE_STORAGE_METRICS WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;
CREATE OR REPLACE TRANSIENT TABLE VIEWS WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.VIEWS;
CREATE OR REPLACE TRANSIENT TABLE METERING_DAILY_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE METERING_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE DATABASE_REPLICATION_USAGE_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.DATABASE_REPLICATION_USAGE_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE REPLICATION_GROUP_USAGE_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_USAGE_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE SESSIONS WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.SESSIONS;
CREATE OR REPLACE TRANSIENT TABLE IS_QUERY_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY()) WHERE 1=0;
CREATE OR REPLACE TRANSIENT TABLE DATABASE_STORAGE_USAGE_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE STAGE_STORAGE_USAGE_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.STAGE_STORAGE_USAGE_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE SEARCH_OPTIMIZATION_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE DATA_TRANSFER_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE AUTOMATIC_CLUSTERING_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE COLUMNS WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.COLUMNS;
CREATE OR REPLACE TRANSIENT TABLE TAGS WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.TAGS;
CREATE OR REPLACE TRANSIENT TABLE TAG_REFERENCES WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES;
CREATE OR REPLACE TRANSIENT TABLE AUTO_REFRESH_REGISTRATION_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT * FROM TABLE(INFORMATION_SCHEMA.AUTO_REFRESH_REGISTRATION_HISTORY()) WHERE 1=0;
RETURN 'SUCCESS';
END;

-- PROCEDURE FOR REPLICATE ACCOUNT_USAGE
CREATE OR REPLACE PROCEDURE REPLICATE_ACCOUNT_USAGE(DBNAME STRING, SCHEMANAME STRING, LOOK_BACK_DAYS STRING)
    returns VARCHAR(25200)
    LANGUAGE javascript
    EXECUTE AS CALLER
AS
$$

var taskDetails = "replicate_metadata_task ---> Getting metadata ";
var task="replicate_metadata_task";
function logError(err, taskName)
{
    var fail_sql = "INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp),'FAILED', "+"'"+ err +"'"+", "+"'"+ taskName +"'"+");" ;
    sql_command1 = snowflake.createStatement({sqlText: fail_sql} );
    sql_command1.execute();
}

function insertToReplicationLog(status, message, taskName)
{
    var query_profile_status = "INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), "+"'"+status  +"'"+", "+"'"+ message +"'"+", "+"'"+ taskName +"'"+");" ;
    sql_command1 = snowflake.createStatement({sqlText: query_profile_status} );
    sql_command1.execute();
}
var schemaName = SCHEMANAME;
var dbName = DBNAME;
var lookBackDays = -parseInt(LOOK_BACK_DAYS);
var error = "";
var returnVal = "SUCCESS";

function truncateTable(tableName)
{
   try
    {
      var truncateQuery = "TRUNCATE TABLE IF EXISTS "+ dbName + "." + schemaName + "." +tableName +" ;";
      var stmt = snowflake.createStatement({sqlText:truncateQuery});
      stmt.execute();
    }
    catch (err)
    {
        logError(err, taskDetails)
        error += "Failed: " + err;
    }
}

function getColumns(tableName)
{
    var columns = "";
    var columnQuery = "SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) as ALL_COLUMNS FROM "+ DBNAME + ".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "+"'"+tableName+"'"+" AND TABLE_SCHEMA = "+"'"+SCHEMANAME+"'"+ ";";
    var stmt = snowflake.createStatement({sqlText:columnQuery});
    try
    {
         var res = stmt.execute();
         res.next();
         columns = res.getColumnValue(1)
    }
    catch (err)
    {
        logError(err, taskDetails)
        error += "Failed: " + err;
    }
   return columns;
}

function insertToTable(tableName, isDate, dateCol, columns){
try{
    var insertQuery = "";
    if (isDate){
    insertQuery = "INSERT INTO " + dbName + "." + schemaName + "." +tableName+ " SELECT "+columns +" FROM SNOWFLAKE.ACCOUNT_USAGE."+ tableName +" WHERE "+ dateCol +" > dateadd(day, "+ lookBackDays +", current_date);";
    }
    else
    {
    insertQuery = "INSERT INTO " + dbName + "." + schemaName + "." +tableName+ " SELECT "+columns +" FROM SNOWFLAKE.ACCOUNT_USAGE."+ tableName +";";
    }

    var insertStmt = snowflake.createStatement({sqlText:insertQuery});
    var res = insertStmt.execute();
}
catch (err)
{
    logError(err, taskDetails)
    error += "Failed: " + err;
}
}

function replicateData(tableName, isDate, dateCol)
{
truncateTable(tableName);
var columns = getColumns(tableName);
columns = columns.split(',').map(item => `"${item.trim()}"`).join(',');
insertToTable(tableName, isDate, dateCol, columns )
return true;
}
insertToReplicationLog("started", "replicate_metadata_task started", task);

replicateData("WAREHOUSE_METERING_HISTORY", true, "START_TIME");
replicateData("WAREHOUSE_EVENTS_HISTORY", true, "TIMESTAMP");
replicateData("WAREHOUSE_LOAD_HISTORY", true, "START_TIME");
replicateData("METERING_DAILY_HISTORY", true, "USAGE_DATE");
replicateData("METERING_HISTORY", true, "START_TIME");
replicateData("DATABASE_REPLICATION_USAGE_HISTORY", true, "START_TIME");
replicateData("REPLICATION_GROUP_USAGE_HISTORY", true, "START_TIME");
replicateData("SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY", true, "START_TIME");
replicateData("DATABASE_STORAGE_USAGE_HISTORY", true, "USAGE_DATE");
replicateData("STAGE_STORAGE_USAGE_HISTORY", true, "USAGE_DATE");
replicateData("SEARCH_OPTIMIZATION_HISTORY", true, "START_TIME");
replicateData("DATA_TRANSFER_HISTORY", true, "START_TIME");
replicateData("AUTOMATIC_CLUSTERING_HISTORY", true, "START_TIME");
replicateData("TAGS", false, "");
replicateData("TAG_REFERENCES", false, "");


try
{
    truncateTable("AUTO_REFRESH_REGISTRATION_HISTORY");
    var columns = getColumns("AUTO_REFRESH_REGISTRATION_HISTORY");
    var insertQuery = "INSERT INTO "+ dbName + "." + schemaName + ".AUTO_REFRESH_REGISTRATION_HISTORY  SELECT "+ columns +" FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.AUTO_REFRESH_REGISTRATION_HISTORY())  WHERE START_TIME > dateadd(day, "+ lookBackDays +", current_date) ;";
    var insertStmt = snowflake.createStatement({sqlText:insertQuery});
    var res = insertStmt.execute();
}catch (err) {
	logError(err, taskDetails);
    error += "Failed: " + err;
}

if(error.length > 0 ) {
    return error;
}
insertToReplicationLog("completed", "replicate_metadata_task completed", task);
return returnVal;
$$;

-- Procedure to get table data replication

CREATE OR REPLACE PROCEDURE REPLICATE_STORAGE_METADATA(DBNAME STRING, SCHEMANAME STRING, LOOK_BACK_DAYS STRING)
    returns VARCHAR(25200)
    LANGUAGE javascript
    EXECUTE AS CALLER
AS
$$

var taskDetails = "replicate_storage_metadata_task ---> Getting metadata ";
var task="replicate_storage_metadata_task";
function logError(err, taskName)
{
    var fail_sql = "INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp),'FAILED', "+"'"+ err +"'"+", "+"'"+ taskName +"'"+");" ;
    sql_command1 = snowflake.createStatement({sqlText: fail_sql} );
    sql_command1.execute();
}

function insertToReplicationLog(status, message, taskName)
{
    var query_profile_status = "INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), "+"'"+status  +"'"+", "+"'"+ message +"'"+", "+"'"+ taskName +"'"+");" ;
    sql_command1 = snowflake.createStatement({sqlText: query_profile_status} );
    sql_command1.execute();
}
var schemaName = SCHEMANAME;
var dbName = DBNAME;
var lookBackDays = -parseInt(LOOK_BACK_DAYS);
var error = "";
var returnVal = "SUCCESS";

function truncateTable(tableName)
{
   try
    {
      var truncateQuery = "TRUNCATE TABLE IF EXISTS "+ dbName + "." + schemaName + "." +tableName +" ;";
      var stmt = snowflake.createStatement({sqlText:truncateQuery});
      stmt.execute();
    }
    catch (err)
    {
        logError(err, taskDetails)
        error += "Failed: " + err;
    }
}

function getColumns(tableName)
{
    var columns = "";
    var columnQuery = "SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) as ALL_COLUMNS FROM "+ DBNAME + ".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "+"'"+tableName+"'"+" AND TABLE_SCHEMA = "+"'"+SCHEMANAME+"'"+ ";";
    var stmt = snowflake.createStatement({sqlText:columnQuery});
    try
    {
         var res = stmt.execute();
         res.next();
         columns = res.getColumnValue(1)
    }
    catch (err)
    {
        logError(err, taskDetails)
        error += "Failed: " + err;
    }
   return columns;
}

function insertToTable(tableName, isDate, dateCol, columns){
try{
    var insertQuery = "";
    if (isDate){
    insertQuery = "INSERT INTO " + dbName + "." + schemaName + "." +tableName+ " SELECT "+columns +" FROM SNOWFLAKE.ACCOUNT_USAGE."+ tableName +" WHERE "+ dateCol +" > dateadd(day, "+ lookBackDays +", current_date);";
    }
    else
    {
    insertQuery = "INSERT INTO " + dbName + "." + schemaName + "." +tableName+ " SELECT "+columns +" FROM SNOWFLAKE.ACCOUNT_USAGE."+ tableName +";";
    }

    var insertStmt = snowflake.createStatement({sqlText:insertQuery});
    var res = insertStmt.execute();
}
catch (err)
{
    logError(err, taskDetails)
    error += "Failed: " + err;
}
}

function replicateData(tableName, isDate, dateCol)
{
truncateTable(tableName);
var columns = getColumns(tableName);
columns = columns.split(',').map(item => `"${item.trim()}"`).join(',');
insertToTable(tableName, isDate, dateCol, columns )
return true;
}
insertToReplicationLog("started", "replicate_metadata_task started", task);

replicateData("TABLES", false, "");
replicateData("TABLE_STORAGE_METRICS", false, "");
replicateData("VIEWS", false, "");
replicateData("COLUMNS", false, "");


insertToReplicationLog("completed", "replicate_storage_metadata_task completed", task);
return returnVal;
$$;

--PROCEDURE FOR REPLICATE HISTORY QUERY
CREATE OR REPLACE PROCEDURE REPLICATE_HISTORY_QUERY(DBNAME STRING, SCHEMANAME STRING, LOOK_BACK_DAYS STRING)
    returns VARCHAR(25200)
    LANGUAGE javascript
    EXECUTE AS CALLER

AS
$$

var taskDetails = "history_query_task ---> Getting history query data ";
var task= "history_query_task";

function logError(err, taskName)
{
    var fail_sql = "INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp),'FAILED', "+"'"+ err +"'"+", "+"'"+ taskName +"'"+");" ;
    sql_command1 = snowflake.createStatement({sqlText: fail_sql} );
    sql_command1.execute();
}

function insertToReplicationLog(status, message, taskName)
{
    var query_profile_status = "INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), "+"'"+status  +"'"+", "+"'"+ message +"'"+", "+"'"+ taskName +"'"+");" ;
    sql_command1 = snowflake.createStatement({sqlText: query_profile_status} );
    sql_command1.execute();
}
var schemaName = SCHEMANAME;
var dbName = DBNAME;
var lookBackDays = -parseInt(LOOK_BACK_DAYS);
var error = "";
var returnVal = "SUCCESS";

function truncateTable(tableName)
{
   try
    {
      var truncateQuery = "TRUNCATE TABLE IF EXISTS "+ dbName + "." + schemaName + "." +tableName +" ;";
      var stmt = snowflake.createStatement({sqlText:truncateQuery});
      stmt.execute();
    }
    catch (err)
    {
        logError(err, taskDetails)
        error += "Failed: " + err;
    }
}

function getColumns(tableName)
{
    var columns = "";
    var columnQuery = "SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) as ALL_COLUMNS FROM "+ DBNAME + ".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "+"'"+tableName+"'"+" AND TABLE_SCHEMA = "+"'"+SCHEMANAME+"'"+ " AND column_name != 'INSERT_TIME';";
    var stmt = snowflake.createStatement({sqlText:columnQuery});
    try
    {
         var res = stmt.execute();
         res.next();
         columns = res.getColumnValue(1)
    }
    catch (err)
    {
        logError(err, taskDetails)
        error += "Failed: " + err;
    }
   return columns;
}

function insertToTable(tableName, isDate, dateCol, columns, isSession){
    var insertQuery = "";
try{

    if (isDate && !isSession){
    insertQuery = "INSERT INTO " + dbName + "." + schemaName + "." +tableName+"("+columns +") SELECT "+columns +" FROM SNOWFLAKE.ACCOUNT_USAGE."+ tableName +" as t1 WHERE t1."
    + dateCol +" > dateadd(day, "+ lookBackDays +", current_date) AND NOT EXISTS ( SELECT 1 FROM " + dbName + "." + schemaName + "." +tableName +" as t2 WHERE t2.query_id = t1.query_id ) order by " + dateCol +"; ";
    }
    else if (isDate && isSession){
     insertQuery = "INSERT INTO " + dbName + "." + schemaName + "." +tableName+"("+columns +")  SELECT "+columns +" FROM SNOWFLAKE.ACCOUNT_USAGE."+ tableName +" WHERE "+ dateCol +" > dateadd(day, "+ lookBackDays +", current_date);";
    }
    else
    {
    insertQuery = "INSERT INTO " + dbName + "." + schemaName + "." +tableName+ " SELECT "+columns +" FROM SNOWFLAKE.ACCOUNT_USAGE."+ tableName +";";
    }

    var insertStmt = snowflake.createStatement({sqlText:insertQuery});
    var res = insertStmt.execute();
}
catch (err)
{
    logError(err, taskDetails)
    error += "Failed: " + err;
}
}

function replicateData(tableName, isDate, dateCol, isSession)
{
    if(isSession)
     {
     truncateTable(tableName);
     }
    var columns = getColumns(tableName);
    columns = columns.split(',').map(item => `"${item.trim()}"`).join(',');
    insertToTable(tableName, isDate, dateCol, columns, isSession)
return true;
}

insertToReplicationLog("started", "history_query_task started", task);

replicateData("QUERY_HISTORY", true, "START_TIME", false);
replicateData("SESSIONS", true, "CREATED_ON", true);
replicateData("ACCESS_HISTORY", true, "QUERY_START_TIME", false);

if(error.length > 0 ) {
    return error;
}

insertToReplicationLog("completed", "history_query_task completed", task);

return returnVal;
$$;


--PROCEDURE FOR REPLICATE REALTIME QUERY BY WAREHOUSE
CREATE OR REPLACE PROCEDURE REPLICATE_REALTIME_QUERY_BY_WAREHOUSE(DBNAME STRING, SCHEMANAME STRING, LOOK_BACK_HOURS STRING)
  RETURNS VARCHAR(25200)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$

var realtime_proc_task = "realtime_query_task ---> REPLICATE_REALTIME_QUERY_BY_WAREHOUSE Table Creation";
var task = "realtime_query_task";
var taskDetails = "realtime_query_task started ---> Getting realtime data ";
var schemaName = SCHEMANAME;
var dbName = DBNAME;
var lookBackHours = -parseInt(LOOK_BACK_HOURS);
var error = "";

function logError(err, taskName)
{
    var fail_sql = "INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp),'FAILED', "+"'"+ err +"'"+", "+"'"+ taskName +"'"+");" ;
    sql_command1 = snowflake.createStatement({sqlText: fail_sql} );
    sql_command1.execute();
}

function insertToReplicationLog(status, message, taskName)
{
    var query_status = "INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), "+"'"+status  +"'"+", "+"'"+ message +"'"+", "+"'"+ taskName +"'"+");" ;
    sql_command1 = snowflake.createStatement({sqlText: query_status} );
    sql_command1.execute();
}

function truncateAndGetColumns(tableName)
{
const queries = [];
queries[0] = "TRUNCATE TABLE IF EXISTS "+ dbName + "." + schemaName + "." +tableName +" ;";

queries[1] = "SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) as ALL_COLUMNS FROM "+ dbName + ".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "+"'"+tableName+"'"+" AND TABLE_SCHEMA = "+"'"+schemaName+"'"+ ";";

var columns = "";
var failed_query_count = 0;
for (let i = 0; i < 2; i++) {

    var stmt = snowflake.createStatement({sqlText:queries[i]});
    try
    {
        var res = stmt.execute();
        if(i == 1)
         {
         res.next();
         columns = res.getColumnValue(1)
         }

    }
    catch (err)
    {
        logError(err, taskDetails)
        error += "Failed: " + err;
    }
}
 return columns;
}

function insertRealtimeQuery(){
   var returnVal = "Insert real time query done.";
   var columns = truncateAndGetColumns("IS_QUERY_HISTORY");
    try
    {
        var insertQuery = "INSERT INTO "+ dbName + "." + schemaName + ".IS_QUERY_HISTORY  SELECT "+ columns +" FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(dateadd('hours',"+ lookBackHours +",current_timestamp()),null,10000)) order by start_time ;";
        var insertStmt = snowflake.createStatement({sqlText:insertQuery});
        var res = insertStmt.execute();
    }
    catch (err)
    {
        logError(err, taskDetails)
        error += "Failed: " + err;
    }
    if(error.length > 0 ) {
        return error;
    }
  return returnVal;
}

function getColumns(tableName)
{
var columns = "";
var columnQuery = "SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) as ALL_COLUMNS FROM "+ DBNAME + ".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "+"'"+tableName+"'"+" AND TABLE_SCHEMA = "+"'"+SCHEMANAME+"'"+ ";";
var stmt = snowflake.createStatement({sqlText:columnQuery});
try
{
     var res = stmt.execute();
     res.next();
     columns = res.getColumnValue(1)
}
catch (err)
{
    logError(err, taskDetails)
    error += "Failed: " + err;
}
 return columns;
}

function insertRealtimeQueryByWarehouse()
{
var returnVal = "Insert real time query by warehouse is done.";
var error = "";
try {
   // 1. run show warehouses
    var showWarehouse = 'SHOW WAREHOUSES;';
	var showWarehouseStmt = snowflake.createStatement({
		sqlText: showWarehouse
	});
    var resultSet = showWarehouseStmt.execute();
    var count =0;
    while (resultSet.next()) {
       // 2. Delete IS_QUERY_HISTORY table by warehouse name
		var whName = resultSet.getColumnValue(1);
		var deleteRealtimeQueryByWh = "DELETE FROM " + DBNAME + '.' + SCHEMANAME + ".IS_QUERY_HISTORY WHERE WAREHOUSE_NAME = "+ "'"+whName+"';";

		var deleteRealtimeQueryByWhStmt = snowflake.createStatement({
		sqlText: deleteRealtimeQueryByWh });

        deleteRealtimeQueryByWhStmt.execute();

      // 3. Insert to IS_QUERY_HISTORY table by warehouse name
        var columns = getColumns("IS_QUERY_HISTORY");
        var insertRealtimeQuery ="INSERT INTO " + DBNAME + '.' + SCHEMANAME + ".IS_QUERY_HISTORY  SELECT "+ columns +" FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY_BY_WAREHOUSE("+"'"+whName+"'"+",dateadd(hours,"+ lookBackHours +", current_timestamp()),null,10000)) order by start_time";

        var insertRealtimeQueryStmt = snowflake.createStatement({
			sqlText: insertRealtimeQuery
		});

		insertRealtimeQueryStmt.execute();
        count++;
        }

} catch (err) {
	logError(err, realtime_proc_task);
    error += "Failed: " + err;
}

if (error.length > 0) {
	return error;
}
return returnVal;
}

function getRealTimeQueryCount() {
    var countQuery = "SELECT count(1) FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(dateadd('hours', " + lookBackHours + ", current_timestamp()), null, 10000));";
    var recordCount = 0;
    try {
        var stmt = snowflake.createStatement({sqlText: countQuery});
        var res = stmt.execute();
        if (res.next()) {
            recordCount = res.getColumnValue(1);
        }
    } catch (err) {
     logError(err, realtime_proc_task);
     error += "Failed: " + err;
    }
    return recordCount;
}

insertToReplicationLog("started", "realtime_query_task started", task);
var queryCount = getRealTimeQueryCount();
var result = "";
if(queryCount == 10000)
{
result = insertRealtimeQueryByWarehouse();
}
else
{
result = insertRealtimeQuery();
}
insertToReplicationLog("completed", "realtime_query_task completed", task);

return result;
$$;

-- PROCEDURE FOR REPLICATE QUERY PROFILE
CREATE OR REPLACE PROCEDURE create_query_profile(dbname string, schemaname string, credit string, days String)
    returns VARCHAR(25200)
    LANGUAGE javascript

AS
$$

var create_query_profile_task = "create_query_profile ---> Getting Query Profile data and inserting into Query_profile table";
var task="profile_task";
function logError(err, taskName)
{
    var fail_sql = "INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp),'FAILED', "+"'"+ err +"'"+", "+"'"+ taskName +"'"+");" ;
    sql_command1 = snowflake.createStatement({sqlText: fail_sql} );
    sql_command1.execute();
}

function insertToReplicationLog(status, message, taskName)
{
    var query_profile_status = "INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), "+"'"+status  +"'"+", "+"'"+ message +"'"+", "+"'"+ taskName +"'"+");" ;
    sql_command1 = snowflake.createStatement({sqlText: query_profile_status} );
    sql_command1.execute();
}

function getColumns(tableName)
{
var columns = "";
var columnQuery = "SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) as ALL_COLUMNS FROM "+ DBNAME + ".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "+"'"+tableName+"'"+" AND TABLE_SCHEMA = "+"'"+SCHEMANAME+"'"+ ";";
var stmt = snowflake.createStatement({sqlText:columnQuery});
try
{
 var res = stmt.execute();
 res.next();
 columns = res.getColumnValue(1)
}
catch (err)
{
    logError(err, taskDetails)
    error += "Failed: " + err;
}
 return columns;
}

var schemaName = SCHEMANAME;
var dbName = DBNAME;
var cost = parseFloat(CREDIT);
var lookBackDays = -parseInt(DAYS);
const queries = [];
queries[0] = 'CREATE TRANSIENT TABLE IF NOT EXISTS ' + dbName + '.' + schemaName + '.QUERY_PROFILE (QUERY_ID VARCHAR(16777216),STEP_ID NUMBER(38, 0),OPERATOR_ID NUMBER(38,0),PARENT_OPERATORS ARRAY, OPERATOR_TYPE VARCHAR(16777216),OPERATOR_STATISTICS VARIANT,EXECUTION_TIME_BREAKDOWN VARIANT, OPERATOR_ATTRIBUTES VARIANT);';

queries[1] = "CREATE OR REPLACE TEMPORARY TABLE "+ dbName + "." + schemaName + ".query_history_temp AS SELECT query_id, unit * execution_time * query_load_percent / 100 / (3600 * 1000) as cost from( SELECT query_id, query_load_percent, CASE WHEN WAREHOUSE_SIZE = 'X-Small' THEN 1 WHEN WAREHOUSE_SIZE = 'Small' THEN 2 WHEN WAREHOUSE_SIZE = 'Medium' THEN 4 WHEN WAREHOUSE_SIZE = 'Large' THEN 6 WHEN WAREHOUSE_SIZE = 'X-Large' THEN 8 WHEN WAREHOUSE_SIZE = '2X-Large' THEN 10 WHEN WAREHOUSE_SIZE = '3X-Large' THEN 12 WHEN WAREHOUSE_SIZE = '4X-Large' THEN 14 WHEN WAREHOUSE_SIZE = '5X-Large' THEN 16 WHEN WAREHOUSE_SIZE = '6X-Large' THEN 18 ELSE 1 END as unit, execution_time FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE START_TIME > dateadd(day, "+ lookBackDays +", current_date) ORDER BY start_time) where cost is not null AND cost > " +cost+";";


queries[2] = "SELECT count(1) FROM "+ dbName + "." + schemaName + ".query_history_temp";


var returnVal = "SUCCESS";
var error = "";
var total_query_count = 0;
var failed_query_count = 0;
var columns = getColumns("QUERY_PROFILE");
columns = columns.split(',').map(item => `"${item.trim()}"`).join(',');

for (let i = 0; i < queries.length; i++) {
    var stmt = snowflake.createStatement({sqlText:queries[i]});
    try
    {
        var res = stmt.execute();

        if(i==2)
        {
         res.next();
         total_query_count = res.getColumnValue(1)
         var message ="Total records = "+ total_query_count;
         insertToReplicationLog("started",message,task);
        }


    }
    catch (err)
    {
        logError(err, create_query_profile_task)
        error += "Failed: " + err;
    }
}
if(error.length > 0 ) {
    return error;
}

var actualQueryId = 'SELECT tmp.query_id FROM '+ dbName + '.' + schemaName +  '.query_history_temp tmp WHERE NOT EXISTS (SELECT query_id FROM QUERY_PROFILE WHERE query_id = tmp.query_id);';


var profileInsert = 'INSERT INTO ' + dbName + '.' + schemaName + '.QUERY_PROFILE  select '+ columns+' from table(get_query_operator_stats(?));';
var stmt = snowflake.createStatement({sqlText: actualQueryId});
 var query_count = 0;
    try
    {
       var result_set1 = stmt.execute();
       while (result_set1.next())  {
       var queryId = result_set1.getColumnValue(1);
       var profileInsertStmt = snowflake.createStatement({sqlText: profileInsert, binds:[queryId]});
       profileInsertStmt.execute();
       query_count++;
       if (query_count % 100 == 0){
        var message ="Total records = "+ total_query_count +", completed = "+query_count+", failed = "+failed_query_count;
        insertToReplicationLog("running", message, task);
        }
       }

    }
    catch (err)
    {
        logError(err, create_query_profile_task)
        error += "Failed: " + err;
    }

var message ="Total records = "+ total_query_count +", completed = "+query_count+", failed = "+failed_query_count;
insertToReplicationLog("completed", message, task);

return returnVal;
$$;

-- PROCEDURE FOR REPLICATE WAREHOUSE INFO
CREATE OR REPLACE PROCEDURE warehouse_proc(dbname STRING, schemaname STRING)
  RETURNS VARCHAR(252)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$

var warehouse_proc_task = "warehouse_proc ---> Warehouses and Warehouse_Parameter Table Creation";
var task = "warehouse_task";

function logError(err, taskName)
{
    var fail_sql = "INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp),'FAILED', "+"'"+ err +"'"+", "+"'"+ taskName +"'"+");" ;
    sql_command1 = snowflake.createStatement({sqlText: fail_sql} );
    sql_command1.execute();
}

function insertToReplicationLog(status, message, taskName)
{
    var query_profile_status = "INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), "+"'"+status  +"'"+", "+"'"+ message +"'"+", "+"'"+ taskName +"'"+");" ;
    sql_command1 = snowflake.createStatement({sqlText: query_profile_status} );
    sql_command1.execute();
}

function getColumns(tableName)
{
var columns = "";
var columnQuery = "SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) as ALL_COLUMNS FROM "+ DBNAME + ".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = "+"'"+tableName+"'"+" AND TABLE_SCHEMA = "+"'"+SCHEMANAME+"'"+ ";";
var stmt = snowflake.createStatement({sqlText:columnQuery});
try
{
 var res = stmt.execute();
 res.next();
 columns = res.getColumnValue(1)
}
catch (err)
{
    logError(err, taskDetails)
    error += "Failed: " + err;
}
 return columns;
}

insertToReplicationLog("started", "warehouse_task started", task);
var returnVal = "SUCCESS";
var error = "";

try {
   // 1. SHOW WAREHOUSES
    var showWarehouse = snowflake.createStatement({sqlText: "SHOW WAREHOUSES"});
    showWarehouse.execute();

    // 2. Get LAST_QUERY_ID
    var query_id_stmt = snowflake.createStatement({sqlText: "SELECT LAST_QUERY_ID()"});
    var query_id_result = query_id_stmt.execute();
    query_id_result.next();
    var query_id = query_id_result.getColumnValue(1);

    // 3. DESCRIBE RESULT
    var describe_sql = `DESCRIBE RESULT '${query_id}'`;
    var describe_stmt = snowflake.createStatement({sqlText: describe_sql});
    var describe_result = describe_stmt.execute();

    var column_defs = [];
    var column_names = [];

    while (describe_result.next()) {
        var col_name = describe_result.getColumnValue("name");
        var data_type = describe_result.getColumnValue("type");
        column_names.push(`"${col_name}"`);
        column_defs.push(`"${col_name}" ${data_type}`);
    }

    // 4. CREATE TABLE IF NOT EXISTS
    var create_table_sql = `CREATE TRANSIENT TABLE IF NOT EXISTS "${DBNAME}"."${SCHEMANAME}".WAREHOUSES (
        ${column_defs.join(",\n    ")}
    );`;
    var create_stmt = snowflake.createStatement({sqlText: create_table_sql});
    create_stmt.execute();

     // 5. TRUNCATE TABLE
    var truncate_sql = `TRUNCATE TABLE IF EXISTS "${DBNAME}"."${SCHEMANAME}".WAREHOUSES;`;
    var truncate_stmt = snowflake.createStatement({sqlText: truncate_sql});
    truncate_stmt.execute();

   // 6. INSERT INTO
    var insert_sql_wh = `INSERT INTO "${DBNAME}"."${SCHEMANAME}".WAREHOUSES (${column_names.join(", ")})
                      SELECT ${column_names.join(", ")} FROM TABLE(RESULT_SCAN('${query_id}'));`;
    var insert_stmt_wh = snowflake.createStatement({sqlText: insert_sql_wh});
    insert_stmt_wh.execute();

} catch (err) {
	logError(err, warehouse_proc_task);
    error += "Failed: " + err;
}

try {

    //1. create warehouse parameters table
	var createWP = 'CREATE TRANSIENT TABLE IF NOT EXISTS ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSE_PARAMETERS (WAREHOUSE VARCHAR(1000), KEY VARCHAR(1000), VALUE VARCHAR(1000), DEFAULT VARCHAR(1000),LEVEL VARCHAR(1000), DESCRIPTION VARCHAR(10000),TYPE VARCHAR(100));';

	var createWPStmt = snowflake.createStatement({
		sqlText: createWP
	});
	createWPStmt.execute();

    //2. trunate warehouse parameter tables
    var truncateWarehouseParameter = 'TRUNCATE TABLE IF EXISTS ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSE_PARAMETERS;';
    var truncateWarehouseParameterStmt = snowflake.createStatement({
		sqlText: truncateWarehouseParameter
	});
    truncateWarehouseParameterStmt.execute();

} catch (err) {
	logError(err, warehouse_proc_task);
    error += "Failed: " + err;
}


try {
    //get columns
    var columns = getColumns("WAREHOUSE_PARAMETERS");
    columns = columns.split(',').slice(1).map(item => `"${item.trim()}"`).join(',').toLowerCase();

    //3.Get warehouse details
	var wn = 'SELECT * FROM ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSES;';
	var wnStmt = snowflake.createStatement({
		sqlText: wn
	});
	var resultSet1 = wnStmt.execute();
	while (resultSet1.next()) {
		var whName = resultSet1.getColumnValue(1);
       //4. show warehouse parameters
		var showWP = 'SHOW PARAMETERS IN WAREHOUSE ' + whName + ';';
		var showWPStmt = snowflake.createStatement({
			sqlText: showWP
		});
		showWPStmt.execute();

        //5. insert into WAREHOUSE_PARAMETERS table
		var wpInsert = 'INSERT INTO ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSE_PARAMETERS SELECT ' + "'" + whName + "'" + ', '+ columns + ' FROM TABLE (result_scan(last_query_id()));';

        var wpInsertStmt = snowflake.createStatement({
			sqlText: wpInsert
		});
		wpInsertStmt.execute();

        }


} catch (err) {

  error += "Failed: " + err;
  return logError(err, warehouse_proc_task);

}

if (error.length > 0) {
	return error;
}

insertToReplicationLog("completed", "warehouse_task completed", task);
return returnVal;
$$;


/**
 PROCEDURE to share data.
*/

CREATE OR REPLACE PROCEDURE SHARE_TO_ACCOUNT(ACCOUNTID STRING, SHARE_NAME STRING, DATABASE_TO_SHARE STRING, SCHEMA_TO_SHARE STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    // Create share
    var use_statement = 'CREATE SHARE ' + SHARE_NAME;
    var statement = snowflake.createStatement({sqlText: use_statement});
    statement.execute();

    // Grant usage on the database to the share
    use_statement = 'GRANT USAGE ON DATABASE ' + DATABASE_TO_SHARE + ' TO SHARE ' + SHARE_NAME;
    statement = snowflake.createStatement({sqlText: use_statement});
    statement.execute();

    // Grant usage on the schema to the share
    use_statement = 'GRANT USAGE ON SCHEMA ' + SCHEMA_TO_SHARE + ' TO SHARE ' + SHARE_NAME;
    statement = snowflake.createStatement({sqlText: use_statement});
    statement.execute();

    // Grant select on various tables to the share
    var tables = [
        'WAREHOUSE_METERING_HISTORY',
        'WAREHOUSE_EVENTS_HISTORY',
        'WAREHOUSE_LOAD_HISTORY',
        'COLUMNS',
        'TAGS',
        'TAG_REFERENCES',
        'TABLES',
        'TABLE_STORAGE_METRICS',
        'VIEWS',
        'METERING_DAILY_HISTORY',
        'METERING_HISTORY',
        'DATABASE_REPLICATION_USAGE_HISTORY',
        'REPLICATION_GROUP_USAGE_HISTORY',
        'SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY',
        'QUERY_HISTORY',
        'SESSIONS',
        'ACCESS_HISTORY',
        'IS_QUERY_HISTORY',
        'WAREHOUSE_PARAMETERS',
        'WAREHOUSES',
        'QUERY_PROFILE',
        'DATABASE_STORAGE_USAGE_HISTORY',
        'STAGE_STORAGE_USAGE_HISTORY',
        'SEARCH_OPTIMIZATION_HISTORY',
        'DATA_TRANSFER_HISTORY',
        'AUTOMATIC_CLUSTERING_HISTORY',
        'AUTO_REFRESH_REGISTRATION_HISTORY',
        'REPLICATION_LOG'
    ];

    for (var i = 0; i < tables.length; i++) {
        use_statement = 'GRANT SELECT ON TABLE ' + tables[i] + ' TO SHARE ' + SHARE_NAME;
        statement = snowflake.createStatement({sqlText: use_statement});
        statement.execute();
    }

    // Alter the share to add the account ID
    use_statement = 'ALTER SHARE ' + SHARE_NAME + ' ADD ACCOUNTS = ' + ACCOUNTID;
    statement = snowflake.createStatement({sqlText: use_statement});
    statement.execute();

    return 'SUCCESS';
} catch (err) {
    return 'FAILED: ' + err.message;
}
$$;

/**
Procedure to cleaning the data
*/

CREATE OR REPLACE PROCEDURE CLEANUP_DATA(DB STRING, SCHEMA STRING, DAYS_TO_KEEP STRING)
RETURNS STRING NOT NULL
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    use_statement VARCHAR;
BEGIN

    use_statement := 'USE ' || DB || '.' || SCHEMA;
    EXECUTE IMMEDIATE use_statement;

    -- Clean up QUERY_HISTORY and ACCESS_HISTORY with configurable days
    EXECUTE IMMEDIATE '
        DELETE FROM QUERY_HISTORY
        WHERE START_TIME < DATEADD(DAY, -' || DAYS_TO_KEEP || ', CURRENT_TIMESTAMP())';

    EXECUTE IMMEDIATE '
        DELETE FROM ACCESS_HISTORY
        WHERE QUERY_START_TIME < DATEADD(DAY, -' || DAYS_TO_KEEP || ', CURRENT_TIMESTAMP())';

    RETURN 'SUCCESS';
END;

/**
Procedure to create_tasks_with_schedule
*/

CREATE OR REPLACE PROCEDURE create_tasks_with_schedule(
    WAREHOUSE_NAME STRING,
    REPLICATE_METADATA_SC STRING,
    REPLICATE_STORAGE_METADATA_SC STRING,
    REPLICATE_HISTORY_QUERY_SC STRING,
    CREATE_PROFILE_TABLE_SC STRING,
    REPLICATE_WAREHOUSE_AND_REALTIME_QUERY_SC STRING,
    CLEANUP_DATA_TASK_SC STRING
)
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$
try {
    var sql_command = "";

    // Create tasks dynamically with the input schedules
    // Task 1 replicate_metadata

    sql_command = `CREATE OR REPLACE TASK replicate_metadata
                   WAREHOUSE = ${WAREHOUSE_NAME}
                   SCHEDULE = '${REPLICATE_METADATA_SC}'
                   AS
                   CALL REPLICATE_ACCOUNT_USAGE(
                       (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'DATABASE_TO_SHARE'),
                       (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'SCHEMA_TO_SHARE'),
                       (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'H_DAYS')
                   );`;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    //Task 2 replicate_storage_metadata
    sql_command = `CREATE OR REPLACE TASK replicate_storage_metadata
                   WAREHOUSE = ${WAREHOUSE_NAME}
                   SCHEDULE = '${REPLICATE_STORAGE_METADATA_SC}'
                   AS
                   CALL REPLICATE_STORAGE_METADATA(
                       (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'DATABASE_TO_SHARE'),
                       (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'SCHEMA_TO_SHARE'),
                       (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'H_DAYS')
                   );`;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    //Task 3 replicate_history_query
    sql_command = `CREATE OR REPLACE TASK replicate_history_query
                   WAREHOUSE = ${WAREHOUSE_NAME}
                   SCHEDULE = '${REPLICATE_HISTORY_QUERY_SC}'
                   AS
                   CALL REPLICATE_HISTORY_QUERY(
                       (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'DATABASE_TO_SHARE'),
                       (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'SCHEMA_TO_SHARE'),
                       (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'H_DAYS')
                   );`;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    //Task 4 createProfileTable
    sql_command = `CREATE OR REPLACE TASK createProfileTable
                   WAREHOUSE = ${WAREHOUSE_NAME}
                   SCHEDULE = '${CREATE_PROFILE_TABLE_SC}'
                   AS
                   CALL CREATE_QUERY_PROFILE(
                       dbname => (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'DATABASE_TO_SHARE'),
                       schemaname => (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'SCHEMA_TO_SHARE'),
                       credit => (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'PROFILE_QUERY_CREDIT'),
                       days => (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'H_DAYS')
                   );`;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

   //Task 5 replicate_warehouse_and_realtime_query
    sql_command = `CREATE OR REPLACE TASK replicate_warehouse_and_realtime_query
                   WAREHOUSE = ${WAREHOUSE_NAME}
                   SCHEDULE = '${REPLICATE_WAREHOUSE_AND_REALTIME_QUERY_SC}'
                   AS
                   BEGIN
                       CALL warehouse_proc(
                           (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'DATABASE_TO_SHARE'),
                           (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'SCHEMA_TO_SHARE')
                       );
                       CALL REPLICATE_REALTIME_QUERY_BY_WAREHOUSE(
                           (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'DATABASE_TO_SHARE'),
                           (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'SCHEMA_TO_SHARE'),
                           (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'R_DAYS')
                       );
                   END;`;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    //Task 6 cleanup_data_task
    sql_command = `CREATE OR REPLACE TASK cleanup_data_task
                   WAREHOUSE = ${WAREHOUSE_NAME}
                   SCHEDULE = '${CLEANUP_DATA_TASK_SC}'
                   AS
                   CALL CLEANUP_DATA(
                       (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'DATABASE_TO_SHARE'),
                       (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'SCHEMA_TO_SHARE'),
                       (SELECT VALUE FROM config_parameters WHERE CONFIG_ID = 'DAYS_TO_KEEP')
                   );`;
    stmt = snowflake.createStatement({sqlText: sql_command});
    stmt.execute();

    return "Tasks created successfully with configurable schedules and warehouse.";
} catch (err) {
    return "Error creating tasks: " + err.message;
}
$$;

/**
Step-1 (One time execution for POV for X days)
*/
CALL create_table_from_snowflake((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'), 'QUERY_HISTORY');
CALL create_table_from_snowflake((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE') , 'ACCESS_HISTORY');
CALL CREATE_TABLES((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'));
CALL REPLICATE_ACCOUNT_USAGE((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'H_DAYS'));
CALL REPLICATE_STORAGE_METADATA((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'H_DAYS'));
CALL REPLICATE_HISTORY_QUERY((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'H_DAYS'));
CALL WAREHOUSE_PROC((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'));
CALL CREATE_QUERY_PROFILE(dbname => (SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), schemaname =>  (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'), credit => (SELECT VALUE FROM config_parameters where CONFIG_ID = 'PROFILE_QUERY_CREDIT'), days => (SELECT VALUE FROM config_parameters where CONFIG_ID = 'H_DAYS'));

/**
Select and run REPLICATE_REALTIME_QUERY_BY_WAREHOUSE procedure if you wish to get real-time queries by warehouse name.It will select a maximum of 10,000 real-time queries for each warehouse at intervals of 1 hours.
*/
CALL REPLICATE_REALTIME_QUERY_BY_WAREHOUSE((SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'R_DAYS'));


/**
Create task using procedure
*/
CALL create_tasks_with_schedule((SELECT VALUE FROM config_parameters where CONFIG_ID = 'WAREHOUSE_NAME'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'REPLICATE_METADATA'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'REPLICATE_STORAGE_METADATA'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'REPLICATE_HISTORY_QUERY'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'CREATE_PROFILE_TABLE'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'REPLICATE_WAREHOUSE_AND_REALTIME_QUERY'),
(SELECT VALUE FROM config_parameters where CONFIG_ID = 'CLEANUP_DATA_TASK'));

/**
 Step-3 (START ALL THE TASKS)
 */
ALTER TASK replicate_metadata RESUME;
ALTER TASK replicate_storage_metadata RESUME;
ALTER TASK replicate_history_query RESUME;
ALTER TASK createProfileTable RESUME;
ALTER TASK replicate_warehouse_and_realtime_query RESUME;
ALTER TASK cleanup_data_task RESUME;

/**
 SHARE tables to given accountId
*/
CALL SHARE_TO_ACCOUNT((SELECT VALUE FROM config_parameters where CONFIG_ID = 'ACCOUNT_ID'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SHARE_NAME'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'DATABASE_TO_SHARE'), (SELECT VALUE FROM config_parameters where CONFIG_ID = 'SCHEMA_TO_SHARE'));
