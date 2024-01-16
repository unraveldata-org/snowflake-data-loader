CREATE OR REPLACE PROCEDURE create_query_profile(dbname string, schemaname string, cost string, days String)
    returns VARCHAR(25200)
    LANGUAGE javascript

AS
$$

var create_query_profile_task = "create_query_profile ---> Getting Query Profile data and inserting into Query_profile table";
var task="profile_task";
function logError(err, taskName)
{
    var fail_sql = "INSERT INTO REPLICATION_LOG VALUES (current_timestamp,'FAILED', "+"'"+ err +"'"+", "+"'"+ taskName +"'"+");" ;
    sql_command1 = snowflake.createStatement({sqlText: fail_sql} );
    sql_command1.execute();
}

function insertToReplicationLog(status, message, taskName)
{
    var query_profile_status = "INSERT INTO REPLICATION_LOG VALUES (current_timestamp, "+"'"+status  +"'"+", "+"'"+ message +"'"+", "+"'"+ taskName +"'"+");" ;
    sql_command1 = snowflake.createStatement({sqlText: query_profile_status} );
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
    logError(err, create_query_profile_task)
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
{   logError(err, create_query_profile_task)
    return "Failed to create the schema "+ SCHEMANAME + ", error: " + err;
}

var schemaName = SCHEMANAME;
var dbName = DBNAME;
var cost = parseFloat(COST);
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

-- create task
CREATE OR REPLACE TASK createProfileTable
  WAREHOUSE = UNRAVELDATA
  SCHEDULE = '60 MINUTE'
AS
call create_query_profile(dbname => 'UNRAVEL_SHARE',schemaname => 'SCHEMA_4823', cost => '1', days => '5');

--To start Task execution
ALTER TASK createProfileTable RESUME;