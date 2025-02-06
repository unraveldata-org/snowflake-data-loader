/**
Set these below fields (DATABASE_TO_SHARE, SCHEMA_TO_SHARE, SHARE_NAME, REPLICATION_DAYS, ACCOUNT_ID)
*/

SET DATABASE_TO_SHARE = 'UNRAVEL_SHARE';
SET SCHEMA_TO_SHARE = 'UNRAVEL_SHARE';
SET SHARE_NAME = 'DEV_SHARE';
SET ACCOUNT_ID = 'YSB87488';
SET PROFILE_QUERY_CREDIT = '1';

CREATE SCHEMA IF NOT EXISTS SCHEMA_4823_T;
USE UNRAVEL_SHARE.SCHEMA_4823_T;

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
CREATE OR REPLACE TRANSIENT TABLE QUERY_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE SESSIONS WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.SESSIONS;
CREATE OR REPLACE TRANSIENT TABLE ACCESS_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY;
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

function insertToTable(tableName, isDate, dateCol, columns, isSession){
    var insertQuery = "";
try{

    if (isDate && !isSession){
    insertQuery = "INSERT INTO " + dbName + "." + schemaName + "." +tableName+ " SELECT "+columns +" FROM SNOWFLAKE.ACCOUNT_USAGE."+ tableName +" as t1 WHERE t1."
    + dateCol +" > dateadd(day, "+ lookBackDays +", current_date) AND NOT EXISTS ( SELECT 1 FROM " + dbName + "." + schemaName + "." +tableName +" as t2 WHERE t2.query_id = t1.query_id ); ";
    }
    else if (isDate && isSession){
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
    // 1. create warehouse table if not exist
    var createWarehouseTable = 'CREATE TRANSIENT TABLE IF NOT EXISTS ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSES("name" VARCHAR(16777216), "state" VARCHAR(16777216), "type" VARCHAR(16777216), "size" VARCHAR(16777216), "min_cluster_count" NUMBER(38,0), "max_cluster_count" NUMBER(38,0), "started_clusters" NUMBER(38,0), "running" NUMBER(38,0), "queued" NUMBER(38,0), "is_default" VARCHAR(1), "is_current" VARCHAR(1), "auto_suspend" NUMBER(38,0), "auto_resume" VARCHAR(16777216), "available" VARCHAR(16777216), "provisioning" VARCHAR(16777216), "quiescing" VARCHAR(16777216), "other"  VARCHAR(16777216), "created_on" TIMESTAMP_LTZ(9), 	"resumed_on" TIMESTAMP_LTZ(9),"updated_on" TIMESTAMP_LTZ(9), "owner" VARCHAR(16777216), "comment" VARCHAR(16777216), "enable_query_acceleration" VARCHAR(16777216), "query_acceleration_max_scale_factor" NUMBER(38,0), "resource_monitor" VARCHAR(16777216),"actives" NUMBER(38,0), "pendings" NUMBER(38,0), "failed" NUMBER(38,0), "suspended" NUMBER(38,0), "uuid" VARCHAR(16777216), "scaling_policy" VARCHAR(16777216), "budget" VARCHAR(16777216));';


var createWarehouseTableStmt = snowflake.createStatement({
		sqlText: createWarehouseTable
	});
    createWarehouseTableStmt.execute();

    // 2. truncate table
    var truncateWarehouse = 'TRUNCATE TABLE IF EXISTS ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSES;';
    var truncateWarehouseStmt = snowflake.createStatement({
		sqlText: truncateWarehouse
	});
    truncateWarehouseStmt.execute();

   // 3. run show warehouses
    var showWarehouse = 'SHOW WAREHOUSES;';
	var showWarehouseStmt = snowflake.createStatement({
		sqlText: showWarehouse
	});
    var resultSet = showWarehouseStmt.execute();

    // 4. insert to warehouse
    var insertToWarehouse = 'INSERT INTO ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSES  SELECT "name", "state", "type", "size","min_cluster_count","max_cluster_count", "started_clusters", "running", "queued","is_default","is_current", "auto_suspend","auto_resume","available","provisioning", "quiescing", "other","created_on","resumed_on","updated_on","owner","comment","enable_query_acceleration", "query_acceleration_max_scale_factor","resource_monitor","actives","pendings","failed","suspended","uuid","scaling_policy","budget" FROM TABLE(result_scan(last_query_id()));';
    var insertToWarehouseStmt = snowflake.createStatement({
			sqlText: insertToWarehouse
		});
	insertToWarehouseStmt.execute();

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

CREATE OR REPLACE PROCEDURE CLEANUP_DATA(DB STRING, SCHEMA STRING)
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

DELETE FROM QUERY_HISTORY WHERE START_TIME < DATEADD(DAY, -2, CURRENT_TIMESTAMP());
DELETE FROM ACCESS_HISTORY WHERE QUERY_START_TIME < DATEADD(DAY, -2, CURRENT_TIMESTAMP());

RETURN 'SUCCESS';
END;

/**
Step-1 (One time execution for POV for 2 days)
*/

CALL CREATE_TABLES($DATABASE_TO_SHARE, $SCHEMA_TO_SHARE);
CALL REPLICATE_ACCOUNT_USAGE($DATABASE_TO_SHARE, $SCHEMA_TO_SHARE ,2);
CALL REPLICATE_STORAGE_METADATA($DATABASE_TO_SHARE, $SCHEMA_TO_SHARE, 2);
CALL REPLICATE_HISTORY_QUERY($DATABASE_TO_SHARE, $SCHEMA_TO_SHARE ,2);
CALL WAREHOUSE_PROC($DATABASE_TO_SHARE, $SCHEMA_TO_SHARE);
CALL CREATE_QUERY_PROFILE(dbname => $DATABASE_TO_SHARE, schemaname => $SCHEMA_TO_SHARE, credit => $PROFILE_QUERY_CREDIT, days => '2');

/**
Select and run REPLICATE_REALTIME_QUERY_BY_WAREHOUSE procedure if you wish to get real-time queries by warehouse name.
It will select a maximum of 10,000 real-time queries for each warehouse at intervals of 1 hours.
*/
CALL REPLICATE_REALTIME_QUERY_BY_WAREHOUSE($DATABASE_TO_SHARE, $SCHEMA_TO_SHARE, 1);




/**
 Step-2 Create Tasks
 create account usage tables Task
*/

CREATE OR REPLACE TASK replicate_metadata
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = 'USING CRON 30 * * * * UTC'
AS
CALL REPLICATE_ACCOUNT_USAGE($DATABASE_TO_SHARE, $SCHEMA_TO_SHARE, 2);

/**
create storage metadata table task
*/
CREATE OR REPLACE TASK replicate_storage_metadata
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = '720 MINUTE'
AS
CALL REPLICATE_STORAGE_METADATA($DATABASE_TO_SHARE, $SCHEMA_TO_SHARE,2);

/**
create history query Task
*/

CREATE OR REPLACE TASK replicate_history_query
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = 'USING CRON 30 * * * * UTC'
AS
CALL REPLICATE_HISTORY_QUERY($DATABASE_TO_SHARE, $SCHEMA_TO_SHARE, 2);

/**
create profile replicate task
*/

CREATE OR REPLACE TASK createProfileTable
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = 'USING CRON 30 * * * * UTC'
AS
CALL create_query_profile(dbname => $DATABASE_TO_SHARE, schemaname => $SCHEMA_TO_SHARE, credit => $PROFILE_QUERY_CREDIT, days => '2');

/**
create Task for replicating information schema query history sync with warehouse
*/

CREATE OR REPLACE TASK replicate_warehouse_and_realtime_query
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = '30 MINUTE'
AS
BEGIN
    CALL warehouse_proc($DATABASE_TO_SHARE, $SCHEMA_TO_SHARE);
    CALL REPLICATE_REALTIME_QUERY_BY_WAREHOUSE($DATABASE_TO_SHARE, $SCHEMA_TO_SHARE, 1);
END;

/**
create Task for cleaning data
*/

CREATE OR REPLACE TASK cleanup_data_task
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = '1440 MINUTE'
AS
CALL CLEANUP_DATA($DATABASE_TO_SHARE, $SCHEMA_TO_SHARE);


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
CALL SHARE_TO_ACCOUNT($ACCOUNT_ID, $SHARE_NAME, $DATABASE_TO_SHARE, $SCHEMA_TO_SHARE);
