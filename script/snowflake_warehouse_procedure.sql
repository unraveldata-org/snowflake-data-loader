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
call warehouse_proc(dbname => 'UNRAVEL_SHARE',schemaname => 'SCHEMA_4823');

--To start Task execution
ALTER TASK createWarehouseTable RESUME;