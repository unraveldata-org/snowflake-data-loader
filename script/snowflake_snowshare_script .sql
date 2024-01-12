CREATE DATABASE IF NOT EXISTS UNRAVEL_SHARE;

USE UNRAVEL_SHARE;

CREATE SCHEMA IF NOT EXISTS SCHEMA_4823;

USE UNRAVEL_SHARE.SCHEMA_4823;


CREATE OR REPLACE TABLE replication_log (
  eventDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  executionStatus VARCHAR(1000) DEFAULT NULL,
  remarks VARCHAR(1000),
  taskName VARCHAR(500) DEFAULT NULL
);

CREATE OR REPLACE PROCEDURE REPLICATE_ACCOUNT_USAGE(DB STRING, SCHEMA STRING, LOOK_BACK_DAYS INTEGER)
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

DROP TABLE IF EXISTS WAREHOUSE_METERING_HISTORY ;
CREATE TRANSIENT TABLE WAREHOUSE_METERING_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY HIS WHERE HIS.START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date)  ;

DROP TABLE IF EXISTS WAREHOUSE_EVENTS_HISTORY ;
CREATE TRANSIENT TABLE WAREHOUSE_EVENTS_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_EVENTS_HISTORY HIS WHERE HIS.TIMESTAMP > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date)  ;

DROP TABLE IF EXISTS WAREHOUSE_LOAD_HISTORY ;
CREATE TRANSIENT TABLE WAREHOUSE_LOAD_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY HIS WHERE HIS.START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);

DROP TABLE IF EXISTS TABLES ;
CREATE TRANSIENT TABLE TABLES WITH DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES;

DROP TABLE IF EXISTS METERING_DAILY_HISTORY ;
CREATE TRANSIENT TABLE METERING_DAILY_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY HIS WHERE HIS.USAGE_DATE > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);

DROP TABLE IF EXISTS METERING_HISTORY ;
CREATE TRANSIENT TABLE METERING_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY HIS WHERE HIS.START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);

DROP TABLE IF EXISTS DATABASE_REPLICATION_USAGE_HISTORY ;
CREATE TRANSIENT TABLE DATABASE_REPLICATION_USAGE_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_REPLICATION_USAGE_HISTORY HIS WHERE HIS.START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);

DROP TABLE IF EXISTS REPLICATION_GROUP_USAGE_HISTORY ;
CREATE TRANSIENT TABLE REPLICATION_GROUP_USAGE_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.REPLICATION_GROUP_USAGE_HISTORY HIS WHERE HIS.START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);

DROP TABLE IF EXISTS DATABASE_STORAGE_USAGE_HISTORY ;
CREATE TRANSIENT TABLE DATABASE_STORAGE_USAGE_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY HIS WHERE HIS.USAGE_DATE > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);

DROP TABLE IF EXISTS STAGE_STORAGE_USAGE_HISTORY ;
CREATE TRANSIENT TABLE STAGE_STORAGE_USAGE_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.STAGE_STORAGE_USAGE_HISTORY HIS WHERE HIS.USAGE_DATE > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);

DROP TABLE IF EXISTS SEARCH_OPTIMIZATION_HISTORY ;
CREATE TRANSIENT TABLE SEARCH_OPTIMIZATION_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.SEARCH_OPTIMIZATION_HISTORY HIS WHERE HIS.START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);

DROP TABLE IF EXISTS DATA_TRANSFER_HISTORY ;
CREATE TRANSIENT TABLE DATA_TRANSFER_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_TRANSFER_HISTORY HIS WHERE HIS.START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);

DROP TABLE IF EXISTS AUTOMATIC_CLUSTERING_HISTORY ;
CREATE TRANSIENT TABLE AUTOMATIC_CLUSTERING_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY HIS WHERE HIS.START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);

DROP TABLE IF EXISTS SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY ;
CREATE TRANSIENT TABLE SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0  AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY HIS WHERE HIS.START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);

DROP TABLE IF EXISTS TAG_REFERENCES ;
CREATE TRANSIENT TABLE TAG_REFERENCES WITH DATA_RETENTION_TIME_IN_DAYS=0  AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES ;


DROP TABLE IF EXISTS QUERY_HISTORY ;
CREATE TRANSIENT TABLE QUERY_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0  AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY HIS WHERE HIS.START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);

DROP TABLE IF EXISTS ACCESS_HISTORY ;
CREATE TRANSIENT TABLE ACCESS_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0  AS SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY HIS WHERE HIS.QUERY_START_TIME > DATEADD(Day ,-:LOOK_BACK_DAYS, current_date);

RETURN 'SUCCESS';

END;

CREATE OR REPLACE PROCEDURE REPLICATE_REALTIME_QUERY(DB STRING, SCHEMA STRING, LOOK_BACK_HOURS INTEGER)
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

DROP TABLE IF EXISTS IS_QUERY_HISTORY ;
CREATE TRANSIENT TABLE IS_QUERY_HISTORY WITH DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(dateadd('hours',-:LOOK_BACK_HOURS ,current_timestamp()),current_timestamp(),10000)) order by start_time ;


RETURN 'SUCCESS';

END;




CREATE OR REPLACE PROCEDURE create_query_profile(dbname string, schemaname string)
    returns VARCHAR(25200)
    LANGUAGE javascript

AS
$$

function logError(err)
{
    var fail_sql = "INSERT INTO REPLICATION_LOG VALUES (current_date,'FAIL', " + "'"+ err +"'"+");" ;
    sql_command1 = snowflake.createStatement({sqlText: fail_sql} );
    sql_command1.execute();
}

try
{
    var query = 'CREATE DATABASE IF NOT EXISTS ' + DBNAME + ';';
    var stmt = snowflake.createStatement({sqlText:query})
    stmt.execute();
    result = "Database: " + DBNAME + " creation is success";
}
catch (err)
{
    logError(err)
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
{   logError(err)
    return "Failed to create the schema "+ SCHEMANAME + ", error: " + err;
}

var schemaName = SCHEMANAME;
var dbName = DBNAME;

const queries = [];
queries[0] = 'CREATE TRANSIENT TABLE IF NOT EXISTS ' + dbName + '.' + schemaName + '.QUERY_PROFILE (QUERY_ID VARCHAR(16777216),STEP_ID NUMBER(38, 0),OPERATOR_ID NUMBER(38,0),PARENT_OPERATORS ARRAY, OPERATOR_TYPE VARCHAR(16777216),OPERATOR_STATISTICS VARIANT,EXECUTION_TIME_BREAKDOWN VARIANT, OPERATOR_ATTRIBUTES VARIANT);';

queries[1] = "CREATE OR REPLACE TEMPORARY TABLE "+ dbName + "." + schemaName + ".query_history_temp AS SELECT query_id, unit * execution_time * query_load_percent / 100 / (3600 * 1000) as cost from( SELECT query_id, query_load_percent, CASE WHEN WAREHOUSE_SIZE = 'X-Small' THEN 1 WHEN WAREHOUSE_SIZE = 'Small' THEN 2 WHEN WAREHOUSE_SIZE = 'Medium' THEN 4 WHEN WAREHOUSE_SIZE = 'Large' THEN 6 WHEN WAREHOUSE_SIZE = 'X-Large' THEN 8 WHEN WAREHOUSE_SIZE = '2X-Large' THEN 10 WHEN WAREHOUSE_SIZE = '3X-Large' THEN 12 WHEN WAREHOUSE_SIZE = '4X-Large' THEN 14 WHEN WAREHOUSE_SIZE = '5X-Large' THEN 16 WHEN WAREHOUSE_SIZE = '6X-Large' THEN 18 ELSE 1 END as unit, execution_time FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE START_TIME > dateadd(day, -1, current_date) ORDER BY start_time) where cost is not null AND cost > 0.1;";


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
        logError(err)
        error += "Failed: " + err;
    }
}
if(error.length > 0 ) {
    return error;
}

var actualQueryId = 'SELECT tmp.query_id FROM '+ dbName + '.' + schemaName +  '.query_history_temp tmp WHERE NOT EXISTS (SELECT query_id FROM QUERY_PROFILE WHERE query_id = tmp.query_id);';

var profileInsert = 'INSERT INTO ' + dbName + '.' + schemaName + '.QUERY_PROFILE  select * from table(get_query_operator_stats(?));';
var stmt = snowflake.createStatement({sqlText: actualQueryId});

    try
    {
       var result_set1 = stmt.execute();
       while (result_set1.next())  {
       var queryId = result_set1.getColumnValue(1);
       var profileInsertstmt = snowflake.createStatement({sqlText: profileInsert, binds:[queryId]});
       profileInsertstmt.execute();

       }
    }
    catch (err)
    {
        logError(err)
        error += "Failed: " + err;
    }

return returnVal;
$$;

CREATE OR REPLACE PROCEDURE warehouse_proc(dbname STRING, schemaname STRING)
  RETURNS VARCHAR(252)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$

function logError(err)
{
    var fail_sql = "INSERT INTO REPLICATION_LOG VALUES (current_date,'FAIL', " + "'"+ err +"'"+");" ;
    sql_command1 = snowflake.createStatement({sqlText: fail_sql} );
    sql_command1.execute();
}

try {
	var query = 'CREATE DATABASE IF NOT EXISTS ' + DBNAME + ';';
	var stmt = snowflake.createStatement({
		sqlText: query
	})
	stmt.execute();
	result = "Database: " + DBNAME + " creation is success";
} catch (err) {
     logError(err)
	return "Failed to create DB " + DBNAME + ", error: " + err;
}

try {
	var query = 'CREATE SCHEMA IF NOT EXISTS ' + DBNAME + '.' + SCHEMANAME + ';';
	var stmt = snowflake.createStatement({
		sqlText: query
	})
	stmt.execute();
	result += "\nSchema: " + SCHEMANAME + " creation is success";
} catch (err) {
    logError(err)
	return "Failed to create the schema " + SCHEMANAME + ", error: " + err;
}

var showWarehouse = 'SHOW WAREHOUSES;';
var createWarehouseTable = 'CREATE OR REPLACE TRANSIENT TABLE  ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSES AS SELECT * FROM TABLE(result_scan(last_query_id()));';
var returnVal = "SUCCESS";
var error = "";

try {
	var warehouseStmt = snowflake.createStatement({
		sqlText: showWarehouse
	});
	var resultSet = warehouseStmt.execute();
	// Checking if the SHOW WAREHOUSES statement returned any rows before creating the table
	if (resultSet.next() == false) {
		error += "No warehouses found.";
	} else {
		var warehouseTableStmt = snowflake.createStatement({
			sqlText: createWarehouseTable
		});
		warehouseTableStmt.execute();
	}
} catch (err) {
	logError(err)
    error += "Failed: " + err;
}

try {
	var createWP = 'CREATE OR REPLACE TRANSIENT TABLE  ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSE_PARAMETERS (WAREHOUSE VARCHAR(1000), KEY VARCHAR(1000), VALUE VARCHAR(1000), DEFUALT VARCHAR(1000),LEVEL VARCHAR(1000), DESCRIPTION VARCHAR(10000),TYPE VARCHAR(100));';

	var createWPStmt = snowflake.createStatement({
		sqlText: createWP
	});
	createWPStmt.execute();
} catch (err) {
	logError(err)
    error += "Failed: " + err;
}
var showWP = '';
try {

	var wn = 'SELECT * FROM ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSES;';
	var wnStmt = snowflake.createStatement({
		sqlText: wn
	});
	var resultSet1 = wnStmt.execute();
	while (resultSet1.next()) {
		var whName = resultSet1.getColumnValue('name');

		showWP = 'SHOW PARAMETERS IN WAREHOUSE ' + whName + ';';

		var showWPStmt = snowflake.createStatement({
			sqlText: showWP
		});
		showWPStmt.execute();

		var wpInsert = 'INSERT INTO ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSE_PARAMETERS SELECT ' + "'" + whName + "'" + ',* FROM TABLE (result_scan(last_query_id()));';

		var wpInsertStmt = snowflake.createStatement({
			sqlText: wpInsert
		});
		wpInsertStmt.execute();

	}
} catch (err) {

  error += "Failed: " + err;
  return logError(err)

}

if (error.length > 0) {
	return error;
}

return returnVal;
$$;