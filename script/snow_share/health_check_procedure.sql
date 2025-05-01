CREATE DATABASE IF NOT EXISTS UNRAVEL_SHARE;
USE UNRAVEL_SHARE;

CREATE SCHEMA IF NOT EXISTS SCHEMA_4825;
USE UNRAVEL_SHARE.SCHEMA_4825;

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
DATA_RETENTION_TIME_IN_DAYS=0 LIKE
SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE WAREHOUSE_EVENTS_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE
SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE WAREHOUSE_LOAD_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE
SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE COLUMNS WITH DATA_RETENTION_TIME_IN_DAYS=0
LIKE SNOWFLAKE.ACCOUNT_USAGE.COLUMNS;
CREATE OR REPLACE TRANSIENT TABLE TAGS WITH DATA_RETENTION_TIME_IN_DAYS=0
LIKE SNOWFLAKE.ACCOUNT_USAGE.TAGS;
CREATE OR REPLACE TRANSIENT TABLE TAG_REFERENCES WITH DATA_RETENTION_TIME_IN_DAYS=0
LIKE SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES;
CREATE OR REPLACE TRANSIENT TABLE TABLES WITH DATA_RETENTION_TIME_IN_DAYS=0 LIKE
SNOWFLAKE.ACCOUNT_USAGE.TABLES;
CREATE OR REPLACE TRANSIENT TABLE TABLE_STORAGE_METRICS WITH DATA_RETENTION_TIME_IN_DAYS=0 LIKE
SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;
CREATE OR REPLACE TRANSIENT TABLE METERING_DAILY_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE METERING_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0
LIKE SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE DATABASE_REPLICATION_USAGE_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE
SNOWFLAKE.ACCOUNT_USAGE.DATABASE_REPLICATION_USAGE_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE REPLICATION_GROUP_USAGE_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_USAGE_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE QUERY_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0
LIKE SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE SESSIONS WITH DATA_RETENTION_TIME_IN_DAYS=0
LIKE SNOWFLAKE.ACCOUNT_USAGE.SESSIONS;
CREATE OR REPLACE TRANSIENT TABLE ACCESS_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0
LIKE SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE IS_QUERY_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0
AS SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY()) WHERE 1=0;
CREATE OR REPLACE TRANSIENT TABLE DATABASE_STORAGE_USAGE_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0
LIKE SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE STAGE_STORAGE_USAGE_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0
LIKE SNOWFLAKE.ACCOUNT_USAGE.STAGE_STORAGE_USAGE_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE SEARCH_OPTIMIZATION_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0
LIKE SNOWFLAKE.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE DATA_TRANSFER_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0
LIKE SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE AUTOMATIC_CLUSTERING_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0
LIKE SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY;
CREATE OR REPLACE TRANSIENT TABLE AUTO_REFRESH_REGISTRATION_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0
AS SELECT * FROM TABLE(INFORMATION_SCHEMA.AUTO_REFRESH_REGISTRATION_HISTORY()) WHERE 1=0;
RETURN 'SUCCESS';
END;

-- PROCEDURE FOR REPLICATE ACCOUNT_USAGE
CREATE OR REPLACE PROCEDURE REPLICATE_ACCOUNT_USAGE(DB STRING, SCHEMA STRING,
LOOK_BACK_DAYS INTEGER)
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
INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), 'started', 'replicate_metadata_task started ', 'replicate_metadata_task');
TRUNCATE TABLE IF EXISTS WAREHOUSE_METERING_HISTORY;
INSERT INTO WAREHOUSE_METERING_HISTORY SELECT * FROM
SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY HIS WHERE HIS.START_TIME >
DATEADD(Day ,-:LOOK_BACK_DAYS, current_date) ;
TRUNCATE TABLE IF EXISTS WAREHOUSE_EVENTS_HISTORY ;
INSERT INTO WAREHOUSE_EVENTS_HISTORY SELECT * FROM
SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY HIS WHERE HIS.TIMESTAMP >
DATEADD(Day ,-:LOOK_BACK_DAYS, current_date) ;

TRUNCATE TABLE IF EXISTS WAREHOUSE_LOAD_HISTORY ;
INSERT INTO WAREHOUSE_LOAD_HISTORY SELECT * FROM
SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY HIS WHERE HIS.START_TIME >
DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);
TRUNCATE TABLE IF EXISTS COLUMNS ;
INSERT INTO COLUMNS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS;
TRUNCATE TABLE IF EXISTS TAGS ;
INSERT INTO TAGS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAGS;
TRUNCATE TABLE IF EXISTS TAG_REFERENCES ;
INSERT INTO TAG_REFERENCES SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES;
TRUNCATE TABLE IF EXISTS TABLES ;
INSERT INTO TABLES SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES;
TRUNCATE TABLE IF EXISTS TABLE_STORAGE_METRICS ;
INSERT INTO TABLE_STORAGE_METRICS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;
TRUNCATE TABLE IF EXISTS METERING_DAILY_HISTORY ;
INSERT INTO METERING_DAILY_HISTORY SELECT * FROM
SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY HIS WHERE HIS.USAGE_DATE >
DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);
TRUNCATE TABLE IF EXISTS METERING_HISTORY ;
INSERT INTO METERING_HISTORY SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
HIS WHERE HIS.START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);
TRUNCATE TABLE IF EXISTS DATABASE_REPLICATION_USAGE_HISTORY ;
INSERT INTO DATABASE_REPLICATION_USAGE_HISTORY SELECT * FROM
SNOWFLAKE.ACCOUNT_USAGE.DATABASE_REPLICATION_USAGE_HISTORY HIS WHERE HIS.START_TIME
> DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);
TRUNCATE TABLE IF EXISTS REPLICATION_GROUP_USAGE_HISTORY ;
INSERT INTO REPLICATION_GROUP_USAGE_HISTORY SELECT * FROM
SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_USAGE_HISTORY HIS WHERE HIS.START_TIME >
DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);

TRUNCATE TABLE IF EXISTS AUTOMATIC_CLUSTERING_HISTORY ;
INSERT INTO AUTOMATIC_CLUSTERING_HISTORY SELECT * FROM
SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY HIS WHERE HIS.START_TIME >
DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);
TRUNCATE TABLE IF EXISTS SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY ;
INSERT INTO SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY SELECT * FROM
SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY HIS WHERE
HIS.START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);

TRUNCATE TABLE IF EXISTS DATABASE_STORAGE_USAGE_HISTORY ;
INSERT INTO DATABASE_STORAGE_USAGE_HISTORY SELECT * FROM
SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY HIS WHERE HIS.USAGE_DATE >DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);
TRUNCATE TABLE IF EXISTS STAGE_STORAGE_USAGE_HISTORY ;
INSERT INTO STAGE_STORAGE_USAGE_HISTORY SELECT * FROM
SNOWFLAKE.ACCOUNT_USAGE.STAGE_STORAGE_USAGE_HISTORY HIS WHERE HIS.USAGE_DATE > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);
TRUNCATE TABLE IF EXISTS SEARCH_OPTIMIZATION_HISTORY ;
INSERT INTO SEARCH_OPTIMIZATION_HISTORY SELECT * FROM
SNOWFLAKE.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY HIS WHERE HIS.START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);
TRUNCATE TABLE IF EXISTS DATA_TRANSFER_HISTORY ;
INSERT INTO DATA_TRANSFER_HISTORY SELECT * FROM
SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY HIS WHERE HIS.START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);
TRUNCATE TABLE IF EXISTS AUTO_REFRESH_REGISTRATION_HISTORY ;
INSERT INTO AUTO_REFRESH_REGISTRATION_HISTORY SELECT * FROM
TABLE(SNOWFLAKE.INFORMATION_SCHEMA.AUTO_REFRESH_REGISTRATION_HISTORY()) WHERE START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);

INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), 'completed', 'replicate_metadata_task completed', 'replicate_metadata_task');
RETURN 'SUCCESS';
EXCEPTION
WHEN EXPRESSION_ERROR THEN
INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), 'Failed', TO_VARCHAR(:sqlerrm ), 'replicate_metadata_task');
return object_construct('error type','expression exception','sqlcode', sqlcode,'sqlerrm', sqlerrm,'sqlstate',sqlstate);
WHEN STATEMENT_ERROR THEN
INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), 'Failed', TO_VARCHAR(:sqlerrm ) , 'replicate_metadata_task');
return object_construct('error type','expression exception','sqlcode', sqlcode,'sqlerrm', sqlerrm,'sqlstate',sqlstate);
WHEN OTHER THEN
INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), 'Failed', TO_VARCHAR(:sqlerrm ) , 'replicate_metadata_task');
return object_construct('error type','expression exception','sqlcode', sqlcode,'sqlerrm', sqlerrm,'sqlstate',sqlstate);
END;

--PROCEDURE FOR REPLICATE HISTORY QUERY
CREATE OR REPLACE PROCEDURE REPLICATE_HISTORY_QUERY(DB STRING, SCHEMA STRING,
LOOK_BACK_DAYS INTEGER)
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
INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), 'started', 'history_query_task started', 'history_query_task');
TRUNCATE TABLE IF EXISTS QUERY_HISTORY ;
INSERT INTO QUERY_HISTORY SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY HIS
WHERE HIS.START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);
TRUNCATE TABLE IF EXISTS ACCESS_HISTORY ;
INSERT INTO ACCESS_HISTORY SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY HIS
WHERE HIS.QUERY_START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);
TRUNCATE TABLE IF EXISTS SESSIONS ;
INSERT INTO SESSIONS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.SESSIONS S
WHERE S.CREATED_ON > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);
INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), 'completed', 'history_query_task completed', 'history_query_task');
RETURN 'SUCCESS';
EXCEPTION
WHEN EXPRESSION_ERROR THEN
INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), 'Failed', TO_VARCHAR(:sqlerrm ), 'history_query_task');
return object_construct('error type','expression exception','sqlcode', sqlcode,'sqlerrm', sqlerrm,'sqlstate',sqlstate);
WHEN STATEMENT_ERROR THEN
INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), 'Failed', TO_VARCHAR(:sqlerrm ) , 'history_query_task');
return object_construct('error type','expression exception','sqlcode', sqlcode,'sqlerrm', sqlerrm,'sqlstate',sqlstate);
WHEN OTHER THEN
INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), 'Failed', TO_VARCHAR(:sqlerrm ) , 'history_query_task');
return object_construct('error type','expression exception','sqlcode', sqlcode,'sqlerrm', sqlerrm,'sqlstate',sqlstate);
END;


--PROCEDURE FOR REPLICATE REALTIME QUERY
CREATE OR REPLACE PROCEDURE REPLICATE_REALTIME_QUERY(DB STRING, SCHEMA STRING,
LOOK_BACK_HOURS INTEGER)
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
INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), 'started', 'realtime_query_task started', 'realtime_query_task');
TRUNCATE TABLE IF EXISTS IS_QUERY_HISTORY ;
INSERT INTO IS_QUERY_HISTORY SELECT * FROM
TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(dateadd('hours',-:LOOK_BACK_HOURS
,current_timestamp()),null,10000)) order by start_time ;
INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), 'completed', 'realtime_query_task completed', 'realtime_query_task');
RETURN 'SUCCESS';
EXCEPTION
WHEN EXPRESSION_ERROR THEN
INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), 'Failed', TO_VARCHAR(:sqlerrm ), 'realtime_query_task');
return object_construct('error type','expression exception','sqlcode', sqlcode,'sqlerrm', sqlerrm,'sqlstate',sqlstate);
WHEN STATEMENT_ERROR THEN
INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), 'Failed', TO_VARCHAR(:sqlerrm ) , 'realtime_query_task');
return object_construct('error type','expression exception','sqlcode', sqlcode,'sqlerrm', sqlerrm,'sqlstate',sqlstate);
WHEN OTHER THEN
INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), 'Failed', TO_VARCHAR(:sqlerrm ) , 'realtime_query_task');
return object_construct('error type','expression exception','sqlcode', sqlcode,'sqlerrm', sqlerrm,'sqlstate',sqlstate);
END;

--PROCEDURE FOR REPLICATE REALTIME QUERY BY WAREHOUSE
CREATE OR REPLACE PROCEDURE REPLICATE_REALTIME_QUERY_BY_WAREHOUSE(DBNAME STRING, SCHEMANAME STRING, LOOK_BACK_HOURS String)
  RETURNS VARCHAR(25200)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$

var warehouse_proc_task = "realtime_query_task ---> REPLICATE_REALTIME_QUERY_BY_WAREHOUSE Table Creation";
var task = "realtime_query_task";

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
insertToReplicationLog("started", "realtime_query_task started", task);
var returnVal = "SUCCESS";
var error = "";
var lookBackDays = -parseInt(LOOK_BACK_HOURS);

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
        var insertRealtimeQuery ="INSERT INTO " + DBNAME + '.' + SCHEMANAME + ".IS_QUERY_HISTORY  SELECT * FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY_BY_WAREHOUSE("+"'"+whName+"'"+",dateadd(hours,"+ lookBackDays +", current_timestamp()),null,10000)) order by start_time";

        var insertRealtimeQueryStmt = snowflake.createStatement({
			sqlText: insertRealtimeQuery
		});

		insertRealtimeQueryStmt.execute();
        count++;
        }

} catch (err) {
	logError(err, warehouse_proc_task);
    error += "Failed: " + err;
}

if (error.length > 0) {
	return error;
}
insertToReplicationLog("completed", "realtime_query_task completed", task);
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
insertToReplicationLog("started", "warehouse_task started", task);
var returnVal = "SUCCESS";
var error = "";

try {
    // 1. truncate table
    var truncateWarehouse = 'TRUNCATE TABLE IF EXISTS ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSES;';
    var truncateWarehouseStmt = snowflake.createStatement({
        sqlText: truncateWarehouse
    });
    truncateWarehouseStmt.execute();

    // 2. run show warehouses
    var showWarehouse = 'SHOW WAREHOUSES;';
    var showWarehouseStmt = snowflake.createStatement({
        sqlText: showWarehouse
    });
    var resultSet = showWarehouseStmt.execute();

    var cols = [];
    var typeMap = {
        "string": "VARCHAR(16777216)",
        "number": "NUMBER(38,0)",
        "date": "TIMESTAMP_LTZ(9)"
    };

    function getValueIfExists(obj, key) {
        if (obj && Object.hasOwn(obj, key)) {
            return obj[key];
        }
        return "VARCHAR(16000000)";
    }

    while (resultSet.next()) {
        var colCount = resultSet.getColumnCount();
        for (var i = 1; i <= colCount; i++) {
            var colName = resultSet.getColumnName(i);
            var colValue = resultSet.getColumnValue(i);
            var colType = resultSet.getColumnType(i);
            obj = {};
            obj["type"] = getValueIfExists(typeMap, colType);
            obj["index"] = i;
            obj["name"] = colName;
            obj["value"] = colValue;
            cols.push(obj);
        }
        break;
    }


    // 3. create warehouse table if not exist
    var colStatement = "";
    for (var i = 0; i < cols.length; i++) {
        var colName1 = cols[i].name;
        var colType1 = cols[i].type;
        if (i === cols.length - 1) {
            colStatement += '"' + colName1 + '" ' + colType1;
        } else {
            colStatement += '"' + colName1 + '" ' + colType1 + ',';
        }
    }

    var createWarehouseTable = 'CREATE TRANSIENT TABLE IF NOT EXISTS ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSES(' + colStatement + ');';

    var createWarehouseTableStmt = snowflake.createStatement({
        sqlText: createWarehouseTable
    });
    createWarehouseTableStmt.execute();

    // 4. insert to warehouse
    var insertSelectColStatement = "";
    for (var i = 0; i < cols.length; i++) {
        var colName2 = cols[i].name;
        if (i === cols.length - 1) {
            insertSelectColStatement += '"' + colName2 + '"';
        } else {
            insertSelectColStatement += '"' + colName2 + '",';
        }
    }

    var insertToWarehouse = 'INSERT INTO ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSES SELECT ' + insertSelectColStatement + ' FROM TABLE(result_scan(last_query_id()));';

    var showWarehouse = 'SHOW WAREHOUSES;';
    var showWarehouseStmt = snowflake.createStatement({
        sqlText: showWarehouse
    });
    var resultSet0 = showWarehouseStmt.execute();

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
    var createWP = 'CREATE TRANSIENT TABLE IF NOT EXISTS ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSE_PARAMETERS (WAREHOUSE VARCHAR(1000), KEY VARCHAR(1000), VALUE VARCHAR(1000), DEFUALT VARCHAR(1000),LEVEL VARCHAR(1000), DESCRIPTION VARCHAR(10000),TYPE VARCHAR(100));';

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
        var wpInsert = 'INSERT INTO ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSE_PARAMETERS SELECT ' + "'" + whName + "'" + ',* FROM TABLE (result_scan(last_query_id()));';

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

var profileInsert = 'INSERT INTO ' + dbName + '.' + schemaName + '.QUERY_PROFILE  select * from table(get_query_operator_stats(?));';
var stmt = snowflake.createStatement({sqlText: actualQueryId});
 var query_count = 0;
    try
    {
       var result_set1 = stmt.execute();

       while (result_set1.next())  {
        query_count++;
        var queryId = result_set1.getColumnValue(1);
       try{
       var profileInsertStmt = snowflake.createStatement({sqlText: profileInsert, binds:[queryId]});
       profileInsertStmt.execute();

       if (query_count % 100 == 0){
        var message ="Total records = "+ total_query_count +", completed = "+(query_count-failed_query_count)+", failed = "+failed_query_count;
        insertToReplicationLog("running", message, task);
        }
        }catch (ignore)
         {
         var failedQueryMessage ="For QueryId = "+ queryId +", failed to get query profile";
         insertToReplicationLog("failed ", failedQueryMessage, task);
         failed_query_count++;
         }
     }
    }
    catch (err)
    {
        logError(err, create_query_profile_task)
        error += "Failed: " + err;
    }

var message ="Total records = "+ total_query_count +", completed = "+(query_count-failed_query_count)+", failed = "+failed_query_count++;;
insertToReplicationLog("completed", message, task);

return returnVal;
$$;

/**
 PROCEDURE to share data.
*/

CREATE OR REPLACE PROCEDURE SHARE_TO_ACCOUNT(ACCOUNTID VARCHAR)
RETURNS STRING NOT NULL
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
use_statement VARCHAR;
res RESULTSET;
BEGIN
CREATE SHARE S_UNRAVEL_SHARE;
GRANT USAGE ON DATABASE UNRAVEL_SHARE to share S_UNRAVEL_SHARE;
GRANT USAGE ON SCHEMA SCHEMA_4825 to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE WAREHOUSE_METERING_HISTORY to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE WAREHOUSE_EVENTS_HISTORY to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE WAREHOUSE_LOAD_HISTORY to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE COLUMNS to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE TAGS to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE TAG_REFERENCES to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE TABLES to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE TABLE_STORAGE_METRICS to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE METERING_DAILY_HISTORY to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE METERING_HISTORY to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE DATABASE_REPLICATION_USAGE_HISTORY to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE REPLICATION_GROUP_USAGE_HISTORY to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE QUERY_HISTORY to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE SESSIONS to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE ACCESS_HISTORY to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE IS_QUERY_HISTORY to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE WAREHOUSE_PARAMETERS to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE WAREHOUSES to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE QUERY_PROFILE to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE DATABASE_STORAGE_USAGE_HISTORY to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE STAGE_STORAGE_USAGE_HISTORY to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE SEARCH_OPTIMIZATION_HISTORY to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE DATA_TRANSFER_HISTORY to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE AUTOMATIC_CLUSTERING_HISTORY to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE AUTO_REFRESH_REGISTRATION_HISTORY to share S_UNRAVEL_SHARE;
GRANT SELECT ON TABLE REPLICATION_LOG to share S_UNRAVEL_SHARE;
use_statement := 'ALTER SHARE S_UNRAVEL_SHARE add accounts = ' || ACCOUNTID::VARIANT::VARCHAR;
res := (EXECUTE IMMEDIATE :use_statement);
RETURN 'SUCCESS';
END;
