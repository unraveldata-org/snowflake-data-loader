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

-- create Task
CREATE OR REPLACE TASK createWarehouseTable
  WAREHOUSE = UNRAVELDATA
  SCHEDULE = '60 MINUTE'
AS
call warehouse_proc('UNRAVEL_SHARE','SCHEMA_4823');

--To start Task execution
ALTER TASK createWarehouseTable RESUME;