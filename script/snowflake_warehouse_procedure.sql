CREATE OR REPLACE PROCEDURE warehouse_proc(dbname STRING, schemaname STRING)
  RETURNS VARCHAR(252)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$

var warehouse_proc_task = "warehouse_proc ---> Warehouses and Warehouse_Parameter Table Creation";
function logError(err, taskName)
{
    var fail_sql = "INSERT INTO REPLICATION_LOG VALUES (current_timestamp,'FAILED', "+"'"+ err +"'"+", "+"'"+ taskName +"'"+");" ;
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
     logError(err, warehouse_proc_task);
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
    logError(err, warehouse_proc_task);
	return "Failed to create the schema " + SCHEMANAME + ", error: " + err;
}


var returnVal = "SUCCESS";
var error = "";

try {
    // 1. create warehouse table if not exist
    var createWarehouseTable = 'CREATE TRANSIENT TABLE IF NOT EXISTS ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSES(NAME VARCHAR(16777216), STATE VARCHAR(16777216), TYPE VARCHAR(16777216), SIZE VARCHAR(16777216), MIN_CLUSTER_COUNT NUMBER(38,0), MAX_CLUSTER_COUNT NUMBER(38,0), STARTED_CLUSTERS NUMBER(38,0), RUNNING NUMBER(38,0), QUEUED NUMBER(38,0), IS_DEFAULT VARCHAR(1), IS_CURRENT VARCHAR(1), AUTO_SUSPEND NUMBER(38,0), AUTO_RESUME VARCHAR(16777216), AVAILABLE VARCHAR(16777216), PROVISIONING VARCHAR(16777216), QUIESCING VARCHAR(16777216), OTHER VARCHAR(16777216), CREATED_ON TIMESTAMP_LTZ(9), RESUMED_ON TIMESTAMP_LTZ(9), UPDATED_ON TIMESTAMP_LTZ(9), OWNER VARCHAR(16777216), COMMENT VARCHAR(16777216), ENABLE_QUERY_ACCELERATION VARCHAR(16777216), QUERY_ACCELERATION_MAX_SCALE_FACTOR NUMBER(38,0), RESOURCE_MONITOR VARCHAR(16777216), ACTIVES NUMBER(38,0), PENDINGS NUMBER(38,0), FAILED NUMBER(38,0), SUSPENDED NUMBER(38,0), UUID VARCHAR(16777216), SCALING_POLICY VARCHAR(16777216), BUDGET VARCHAR(16777216));';


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
    var insertToWarehouse = 'INSERT INTO ' + DBNAME + '.' + SCHEMANAME + '.WAREHOUSES  SELECT * FROM TABLE(result_scan(last_query_id()));';
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

return returnVal;
$$;

-- create Task
CREATE OR REPLACE TASK createWarehouseTable
  WAREHOUSE = UNRAVELDATA
  SCHEDULE = '60 MINUTE'
AS
call warehouse_proc('UNRAVEL_SHARE','SCHEMA_4823');

--To start Task execution
ALTER TASK createWarehouseTable RESUME;