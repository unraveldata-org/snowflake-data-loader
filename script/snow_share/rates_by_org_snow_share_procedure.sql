/**
    ORGANIZATION_USAGE metadata replication for Unravel.

    This script is an ADDENDUM to the original UNRAVEL_SHARE replication script.
    It assumes the following objects already exist (created by the parent script):
        - DATABASE  : UNRAVEL_SHARE
        - SCHEMA    : SCHEMA_4827_T
        - TABLE     : REPLICATION_LOG       (reused for status / error logging)
        - SHARE     : S_SECURE_SHARE        (new tables are added to this share)

    Tables covered by this script (date column used for incremental filtering):
        1. USAGE_IN_CURRENCY_DAILY   -> USAGE_DATE
        2. RATE_SHEET_DAILY          -> "DATE"
        3. REMAINING_BALANCE_DAILY   -> "DATE"

    ============================================================================
    IMPORTANT PREREQUISITES — please read before executing
    ============================================================================

    1. ROLE REQUIREMENT:
       The procedures must be executed by ORGADMIN (or a role with SELECT on
       SNOWFLAKE.ORGANIZATION_USAGE.* delegated to it). EXECUTE AS CALLER is used
       intentionally so that the caller's privileges apply. ACCOUNTADMIN alone
       will NOT see these views.

       Suggested one-time setup if a non-ORGADMIN role will run the task:
           USE ROLE ORGADMIN;
           GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <your_role>;
           GRANT ROLE ORGADMIN TO USER <task_owner_user>;     -- if needed
       Then create / alter the task with that role and warehouse.

    2. RESELLER LIMITATION:
       Customers who signed a Snowflake contract through a reseller CANNOT
       access these three views. If that's the case, this script should not be
       run — the procedures will fail with permission errors.

    3. PRIMARY / FUNDING ORG LIMITATION:
       REMAINING_BALANCE_DAILY is only accessible from the PRIMARY (funding)
       organization. If this account is a secondary org drawing down on a shared
       capacity contract, the REMAINING_BALANCE_DAILY replication step will
       fail; the other two will still work.

    4. MUTABLE HISTORY:
       Until month close (typically the 3rd-4th of the following month) the
       per-day values in these views can change to reflect adjustments,
       credits, contract amendments, and inter-org account transfers. The
       steady-state task therefore uses a 35-day lookback (vs 2 days for the
       ACCOUNT_USAGE tables) to ensure prior-month revisions get picked up
       on the next scheduled run.

    5. LATENCY:
       USAGE_IN_CURRENCY_DAILY: up to 72h
       RATE_SHEET_DAILY:        up to 24h
       REMAINING_BALANCE_DAILY: up to 72h
       The 35-day lookback already absorbs all of these comfortably.
**/

USE UNRAVEL_SHARE.SCHEMA_4827_T;

/**
    Step-1a: Started (procedure creation started.)
**/

-- PROCEDURE FOR CREATING THE ORGANIZATION_USAGE TABLES
CREATE OR REPLACE PROCEDURE CREATE_ORG_USAGE_TABLES(DB STRING, SCHEMA STRING)
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

-- All three are ORGANIZATION_USAGE-backed; cloned LIKE the source view.
CREATE OR REPLACE TRANSIENT TABLE USAGE_IN_CURRENCY_DAILY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY;

CREATE OR REPLACE TRANSIENT TABLE RATE_SHEET_DAILY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ORGANIZATION_USAGE.RATE_SHEET_DAILY;

CREATE OR REPLACE TRANSIENT TABLE REMAINING_BALANCE_DAILY WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ORGANIZATION_USAGE.REMAINING_BALANCE_DAILY;

RETURN 'SUCCESS';
END;


-- PROCEDURE FOR REPLICATING ORGANIZATION_USAGE DATA
CREATE OR REPLACE PROCEDURE REPLICATE_ORG_USAGE(DBNAME STRING, SCHEMANAME STRING, LOOK_BACK_DAYS STRING)
    RETURNS VARCHAR(25200)
    LANGUAGE javascript
    EXECUTE AS CALLER
AS
$$

var taskDetails = "replicate_org_usage_task ---> Getting ORGANIZATION_USAGE data ";
var task = "replicate_org_usage_task";

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

// dateCol may already contain double-quotes (e.g. '"DATE"' for the reserved word).
// We DO NOT add additional quoting here; the caller is responsible for passing
// the column name in the exact form it should appear in the generated SQL.
function insertToTable(tableName, dateCol, columns){
try{
    var insertQuery = "INSERT INTO " + dbName + "." + schemaName + "." +tableName+
        " SELECT "+ columns +
        " FROM SNOWFLAKE.ORGANIZATION_USAGE."+ tableName +
        " WHERE "+ dateCol +" > dateadd(day, "+ lookBackDays +", current_date);";

    var insertStmt = snowflake.createStatement({sqlText:insertQuery});
    var res = insertStmt.execute();
}
catch (err)
{
    logError(err, taskDetails);
    error += "Failed: " + err;
}
}

function replicateData(tableName, dateCol)
{
    truncateTable(tableName);
    var columns = getColumns(tableName);
    columns = columns.split(',').map(item => `"${item.trim()}"`).join(',');
    insertToTable(tableName, dateCol, columns);
    return true;
}

insertToReplicationLog("started", "replicate_org_usage_task started", task);

// USAGE_DATE — non-reserved, no quoting needed
replicateData("USAGE_IN_CURRENCY_DAILY", "USAGE_DATE");

// "DATE" — column literally named DATE; must be double-quoted in the generated SQL
replicateData("RATE_SHEET_DAILY",        '"DATE"');
replicateData("REMAINING_BALANCE_DAILY", '"DATE"');

if(error.length > 0 ) {
    return error;
}

insertToReplicationLog("completed", "replicate_org_usage_task completed", task);

return returnVal;
$$;


-- PROCEDURE TO ADD THE NEW ORGANIZATION_USAGE TABLES TO THE EXISTING SHARE
CREATE OR REPLACE PROCEDURE ADD_ORG_USAGE_TABLES_TO_SHARE()
RETURNS STRING NOT NULL
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
    GRANT SELECT ON TABLE USAGE_IN_CURRENCY_DAILY   TO SHARE S_SECURE_SHARE;
    GRANT SELECT ON TABLE RATE_SHEET_DAILY          TO SHARE S_SECURE_SHARE;
    GRANT SELECT ON TABLE REMAINING_BALANCE_DAILY   TO SHARE S_SECURE_SHARE;
    RETURN 'SUCCESS';
END;
/**
    Step-1a: ENDED (procedure creation done.)
**/


/**
    Step-1b: One-time history back-fill.
             ORGANIZATION_USAGE retains data indefinitely back to June 2020,
             so the lookback here only caps how much we copy over for the
             initial load. 180 = roughly 6 months; adjust to taste.
**/
CALL CREATE_ORG_USAGE_TABLES('UNRAVEL_SHARE','SCHEMA_4827_T');
CALL REPLICATE_ORG_USAGE('UNRAVEL_SHARE','SCHEMA_4827_T', 180);
/**
    Step-1b: ENDED
**/


/**
    Step-1c: Add the new tables to the existing S_SECURE_SHARE.
**/
CALL ADD_ORG_USAGE_TABLES_TO_SHARE();
/**
    Step-1c: ENDED
**/


