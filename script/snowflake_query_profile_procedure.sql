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

-- create task
CREATE OR REPLACE TASK createProfileTable
  WAREHOUSE = UNRAVELDATA
  SCHEDULE = '60 MINUTE'
AS
call create_query_profile('UNRAVEL_SHARE','SCHEMA_4823');

--To start Task execution
ALTER TASK createProfileTable RESUME;