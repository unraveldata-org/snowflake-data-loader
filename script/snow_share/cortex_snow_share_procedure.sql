/**
    Cortex / Snowpark Container Services metadata replication for Unravel.

    This script is an ADDENDUM to the original UNRAVEL_SHARE replication script.
    It assumes the following objects already exist (created by the parent script):
        - DATABASE  : UNRAVEL_SHARE
        - SCHEMA    : SCHEMA_4827_T
        - TABLE     : REPLICATION_LOG       (reused for status / error logging)
        - SHARE     : S_SECURE_SHARE        (new tables are added to this share)

    Tables covered by this script (date column used for incremental filtering):
        1. CORTEX_AISQL_USAGE_HISTORY                 -> USAGE_TIME
        2. CORTEX_ANALYST_USAGE_HISTORY               -> START_TIME
        3. CORTEX_FINE_TUNING_USAGE_HISTORY           -> START_TIME
        4. CORTEX_PROVISIONED_THROUGHPUT_USAGE_HISTORY-> INTERVAL_START_TIME
        5. CORTEX_REST_API_USAGE_HISTORY              -> START_TIME
        6. CORTEX_SEARCH_DAILY_USAGE_HISTORY          -> USAGE_DATE
        7. CORTEX_SEARCH_SERVING_USAGE_HISTORY        -> START_TIME
        8. CORTEX_SEARCH_REFRESH_HISTORY              -> DATA_TIMESTAMP   *INFORMATION_SCHEMA only, 7d max*
        9. SNOWPARK_CONTAINER_SERVICES_HISTORY        -> START_TIME

    NOTE on CORTEX_SEARCH_REFRESH_HISTORY:
        Unlike the other 8 tables, this is NOT exposed as a SNOWFLAKE.ACCOUNT_USAGE view.
        It is only available as the INFORMATION_SCHEMA.CORTEX_SEARCH_REFRESH_HISTORY()
        table function, which only returns refreshes whose DATA_TIMESTAMP is within
        the last 7 days. The replication procedure handles it the same way the parent
        script handles AUTO_REFRESH_REGISTRATION_HISTORY / IS_QUERY_HISTORY, and the
        lookback for it is automatically capped at 7 days regardless of the
        LOOK_BACK_DAYS argument value.
**/

USE UNRAVEL_SHARE.SCHEMA_4827_T;

/**
    Step-1a: Started (procedures creation started.)
**/

-- PROCEDURE FOR CREATING THE CORTEX / SNOWPARK CONTAINER SERVICES TABLES
CREATE OR REPLACE PROCEDURE CREATE_CORTEX_TABLES(DB STRING, SCHEMA STRING)
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

-- ACCOUNT_USAGE-backed tables: cloned LIKE the source view
CREATE OR REPLACE TRANSIENT TABLE CORTEX_AISQL_USAGE_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AISQL_USAGE_HISTORY;

CREATE OR REPLACE TRANSIENT TABLE CORTEX_ANALYST_USAGE_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY;

CREATE OR REPLACE TRANSIENT TABLE CORTEX_FINE_TUNING_USAGE_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FINE_TUNING_USAGE_HISTORY;

CREATE OR REPLACE TRANSIENT TABLE CORTEX_PROVISIONED_THROUGHPUT_USAGE_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.CORTEX_PROVISIONED_THROUGHPUT_USAGE_HISTORY;

CREATE OR REPLACE TRANSIENT TABLE CORTEX_REST_API_USAGE_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.CORTEX_REST_API_USAGE_HISTORY;

CREATE OR REPLACE TRANSIENT TABLE CORTEX_SEARCH_DAILY_USAGE_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_DAILY_USAGE_HISTORY;

CREATE OR REPLACE TRANSIENT TABLE CORTEX_SEARCH_SERVING_USAGE_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_SERVING_USAGE_HISTORY;

CREATE OR REPLACE TRANSIENT TABLE SNOWPARK_CONTAINER_SERVICES_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.SNOWPARK_CONTAINER_SERVICES_HISTORY;

-- INFORMATION_SCHEMA-backed table function (no ACCOUNT_USAGE equivalent).
-- Schema is derived by running the function with WHERE 1=0 to discover its columns.
CREATE OR REPLACE TRANSIENT TABLE CORTEX_SEARCH_REFRESH_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 AS
SELECT * FROM TABLE(INFORMATION_SCHEMA.CORTEX_SEARCH_REFRESH_HISTORY()) WHERE 1=0;

RETURN 'SUCCESS';
END;


-- PROCEDURE FOR REPLICATING CORTEX / SNOWPARK CONTAINER SERVICES USAGE
CREATE OR REPLACE PROCEDURE REPLICATE_CORTEX_USAGE(DBNAME STRING, SCHEMANAME STRING, LOOK_BACK_DAYS STRING)
    RETURNS VARCHAR(25200)
    LANGUAGE javascript
    EXECUTE AS CALLER
AS
$$

var taskDetails = "replicate_cortex_task ---> Getting cortex / snowpark container usage data ";
var task = "replicate_cortex_task";

function logError(err, taskName)
{
     try {
        var errStr = (err && err.message) ? err.message : String(err);
        var taskStr = taskName ? String(taskName) : '';
        var sql_command1 = snowflake.createStatement({
                sqlText: "INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp),'FAILED', ?, ?)",
                binds: [errStr, taskStr]
        });
        sql_command1.execute();
     }
     catch (e) {
        // ignore-resort logging (avoid recursive failure)
     }
}

function insertToReplicationLog(status, message, taskName)
{
    try
    {
        var statusStr = status ? String(status) : '';
        var messageStr = message ? String(message) : '';
        var taskStr = taskName ? String(taskName) : '';
        var sql_command1 = snowflake.createStatement({
                  sqlText: "INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), ?, ?, ?)",
                  binds: [statusStr, messageStr, taskStr]
        });
        sql_command1.execute();
    }
    catch (e) {
        // ignore-resort logging (avoid recursive failure)
    }
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
        logError(err, taskDetails);
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
         columns = res.getColumnValue(1);
    }
    catch (err)
    {
        logError(err, taskDetails);
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
    logError(err, taskDetails);
    error += "Failed: " + err;
}
}

function replicateData(tableName, isDate, dateCol)
{
    truncateTable(tableName);
    var columns = getColumns(tableName);
    columns = columns.split(',').map(item => `"${item.trim()}"`).join(',');
    insertToTable(tableName, isDate, dateCol, columns);
    return true;
}

insertToReplicationLog("started", "replicate_cortex_task started", task);

// --- ACCOUNT_USAGE-backed views (each with its appropriate timestamp column) ---
replicateData("CORTEX_AISQL_USAGE_HISTORY",                  true, "USAGE_TIME");
replicateData("CORTEX_ANALYST_USAGE_HISTORY",                true, "START_TIME");
replicateData("CORTEX_FINE_TUNING_USAGE_HISTORY",            true, "START_TIME");
replicateData("CORTEX_PROVISIONED_THROUGHPUT_USAGE_HISTORY", true, "INTERVAL_START_TIME");
replicateData("CORTEX_REST_API_USAGE_HISTORY",               true, "START_TIME");
replicateData("CORTEX_SEARCH_DAILY_USAGE_HISTORY",           true, "USAGE_DATE");
replicateData("CORTEX_SEARCH_SERVING_USAGE_HISTORY",         true, "START_TIME");
replicateData("SNOWPARK_CONTAINER_SERVICES_HISTORY",         true, "START_TIME");

// --- INFORMATION_SCHEMA table function (max 7 days history) ---
// Handled the same way as AUTO_REFRESH_REGISTRATION_HISTORY in the parent script.
try
{
    truncateTable("CORTEX_SEARCH_REFRESH_HISTORY");
    var columns = getColumns("CORTEX_SEARCH_REFRESH_HISTORY");
    columns = columns.split(',').map(item => `"${item.trim()}"`).join(',');

    // INFORMATION_SCHEMA.CORTEX_SEARCH_REFRESH_HISTORY() only returns refreshes
    // whose DATA_TIMESTAMP is within 7 days of current_timestamp. Cap accordingly.
    var effectiveLookBack = (lookBackDays < -7) ? -7 : lookBackDays;

    var insertQuery = "INSERT INTO " + dbName + "." + schemaName + ".CORTEX_SEARCH_REFRESH_HISTORY  SELECT " + columns +
        " FROM TABLE(INFORMATION_SCHEMA.CORTEX_SEARCH_REFRESH_HISTORY(DATA_TIMESTAMP_START => dateadd(day, " + effectiveLookBack + ", current_timestamp()), RESULT_LIMIT => 10000));";
    var insertStmt = snowflake.createStatement({sqlText:insertQuery});
    var res = insertStmt.execute();
}
catch (err)
{
    logError(err, taskDetails);
    error += "Failed: " + err;
}

if(error.length > 0 ) {
    return error;
}

insertToReplicationLog("completed", "replicate_cortex_task completed", task);

return returnVal;
$$;


-- PROCEDURE TO ADD THE NEW CORTEX / SNOWPARK CONTAINER SERVICES TABLES
-- TO THE EXISTING S_SECURE_SHARE.
CREATE OR REPLACE PROCEDURE ADD_CORTEX_TABLES_TO_SHARE()
RETURNS STRING NOT NULL
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
    GRANT SELECT ON TABLE CORTEX_AISQL_USAGE_HISTORY                  TO SHARE S_SECURE_SHARE;
    GRANT SELECT ON TABLE CORTEX_ANALYST_USAGE_HISTORY                TO SHARE S_SECURE_SHARE;
    GRANT SELECT ON TABLE CORTEX_FINE_TUNING_USAGE_HISTORY            TO SHARE S_SECURE_SHARE;
    GRANT SELECT ON TABLE CORTEX_PROVISIONED_THROUGHPUT_USAGE_HISTORY TO SHARE S_SECURE_SHARE;
    GRANT SELECT ON TABLE CORTEX_REST_API_USAGE_HISTORY               TO SHARE S_SECURE_SHARE;
    GRANT SELECT ON TABLE CORTEX_SEARCH_DAILY_USAGE_HISTORY           TO SHARE S_SECURE_SHARE;
    GRANT SELECT ON TABLE CORTEX_SEARCH_SERVING_USAGE_HISTORY         TO SHARE S_SECURE_SHARE;
    GRANT SELECT ON TABLE CORTEX_SEARCH_REFRESH_HISTORY               TO SHARE S_SECURE_SHARE;
    GRANT SELECT ON TABLE SNOWPARK_CONTAINER_SERVICES_HISTORY         TO SHARE S_SECURE_SHARE;
    RETURN 'SUCCESS';
END;
/**
    Step-1a: ENDED (procedure creation done.)
**/


/**
    Step-1b: One-time execution to create the tables and back-fill 180 days of history.
             (CORTEX_SEARCH_REFRESH_HISTORY will only contain the last 7 days because
              of an upstream INFORMATION_SCHEMA constraint.)
**/
CALL CREATE_CORTEX_TABLES('UNRAVEL_SHARE','SCHEMA_4827_T');
CALL REPLICATE_CORTEX_USAGE('UNRAVEL_SHARE','SCHEMA_4827_T', 180);
/**
    Step-1b: ENDED (One time execution for history data for 180 days done.)
**/


/**
    Step-1c: Add the new tables to the existing S_SECURE_SHARE.
             (S_SECURE_SHARE must already exist from the parent script's
              SHARE_TO_ACCOUNT procedure.)
**/
CALL ADD_CORTEX_TABLES_TO_SHARE();
/**
    Step-1c: ENDED (New Cortex tables added to the existing share.)
**/


/**
    Step-2 : Once step-1a, step-1b and step-1c is done, then customer to inform Unravel
             for polling the new tables on SaaS.
**/


/**
    Step-3 : One-time delta load between history end date and current date.
             Assuming history load is done and continuous polling starts N days later,
             pass N as the LOOK_BACK_DAYS argument. Example uses 3.
**/
CALL REPLICATE_CORTEX_USAGE('UNRAVEL_SHARE','SCHEMA_4827_T', 3);
/**
    Step-3 : ENDED (Delta load done.)
**/


/**
    Step-4 : Once step-3 is done, then customer to inform Unravel for polling the
             delta data on SaaS.
**/


/**
    Step-5 : Create task for incremental Cortex / Snowpark container services data load.
             Schedule mirrors the parent script's replicate_metadata task (every 6 hrs)
             with a 2-day lookback to absorb ACCOUNT_USAGE latency. The cron is offset
             by one hour from replicate_metadata to avoid warehouse contention.
**/
CREATE OR REPLACE TASK replicate_cortex_usage
  WAREHOUSE = UNRAVELDATA
  SCHEDULE = 'USING CRON 0 4,10,16,22 * * * UTC'
AS
CALL REPLICATE_CORTEX_USAGE('UNRAVEL_SHARE','SCHEMA_4827_T', 2);

/**
    (Resume the task)
**/
ALTER TASK replicate_cortex_usage RESUME;
/**
    Step-5: ENDED Task created and resumed.
**/


/**
    Step-6 : Once step-5 is done, then customer to inform Unravel to monitor the
             continuous secure share data loading for the new Cortex / Snowpark
             container services tables on SaaS.
**/