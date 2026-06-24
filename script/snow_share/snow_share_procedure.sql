/**
    Step-1a: Started (procedures creation started.)
**/

CREATE DATABASE IF NOT EXISTS UNRAVEL_SHARE;
USE UNRAVEL_SHARE;

CREATE SCHEMA IF NOT EXISTS SCHEMA_4827_T;
USE UNRAVEL_SHARE.SCHEMA_4827_T;

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
CREATE OR REPLACE TRANSIENT TABLE QUERY_INSIGHTS WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.QUERY_INSIGHTS;
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
CREATE OR REPLACE TRANSIENT TABLE GRANTS_TO_ROLES WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES;
CREATE OR REPLACE TRANSIENT TABLE GRANTS_TO_SHARES WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_SHARES;
CREATE OR REPLACE TRANSIENT TABLE GRANTS_TO_USERS WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS;
CREATE OR REPLACE TRANSIENT TABLE ROLES WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.ROLES;
CREATE OR REPLACE TRANSIENT TABLE USERS WITH
DATA_RETENTION_TIME_IN_DAYS=0 LIKE SNOWFLAKE.ACCOUNT_USAGE.USERS;
CREATE OR REPLACE TRANSIENT TABLE AUTO_REFRESH_REGISTRATION_HISTORY WITH
DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT * FROM TABLE(INFORMATION_SCHEMA.AUTO_REFRESH_REGISTRATION_HISTORY()) WHERE 1=0;

ALTER TABLE TABLES ADD COLUMN LATEST_WRITE_TIME          TIMESTAMP_TZ;
ALTER TABLE TABLES ADD COLUMN LATEST_ACCESS_TIME         TIMESTAMP_TZ;
ALTER TABLE TABLES ADD COLUMN ACCESS_COUNT_LAST_15_DAYS  INTEGER;
ALTER TABLE TABLES ADD COLUMN ACCESS_COUNT_LAST_30_DAYS  INTEGER;
ALTER TABLE TABLES ADD COLUMN ACCESS_COUNT_LAST_45_DAYS  INTEGER;
ALTER TABLE TABLES ADD COLUMN ACCESS_COUNT_LAST_60_DAYS  INTEGER;
ALTER TABLE TABLES ADD COLUMN ACCESS_COUNT_LAST_90_DAYS  INTEGER;
ALTER TABLE TABLES ADD COLUMN ACCESS_COUNT_LAST_180_DAYS  INTEGER;
ALTER TABLE TABLES ADD COLUMN WRITE_COUNT_LAST_15_DAYS   INTEGER;
ALTER TABLE TABLES ADD COLUMN WRITE_COUNT_LAST_30_DAYS   INTEGER;
ALTER TABLE TABLES ADD COLUMN WRITE_COUNT_LAST_45_DAYS   INTEGER;
ALTER TABLE TABLES ADD COLUMN WRITE_COUNT_LAST_60_DAYS   INTEGER;
ALTER TABLE TABLES ADD COLUMN WRITE_COUNT_LAST_90_DAYS   INTEGER;
ALTER TABLE TABLES ADD COLUMN WRITE_COUNT_LAST_180_DAYS   INTEGER;

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
replicateData("TABLE_STORAGE_METRICS", false, "");
replicateData("DATABASE_STORAGE_USAGE_HISTORY", true, "USAGE_DATE");
replicateData("STAGE_STORAGE_USAGE_HISTORY", true, "USAGE_DATE");
replicateData("SEARCH_OPTIMIZATION_HISTORY", true, "START_TIME");
replicateData("DATA_TRANSFER_HISTORY", true, "START_TIME");
replicateData("AUTOMATIC_CLUSTERING_HISTORY", true, "START_TIME");
replicateData("COLUMNS", false, "");
replicateData("TAGS", false, "");
replicateData("TAG_REFERENCES", false, "");
replicateData("GRANTS_TO_ROLES",  false, "");
replicateData("GRANTS_TO_SHARES",  false, "");
replicateData("GRANTS_TO_USERS",  false, "");
replicateData("ROLES", false, "");
replicateData("USERS", false, "");


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

-- PROCEDURE FOR REPLICATE TABLES WITH ACCESS METRICS
CREATE OR REPLACE PROCEDURE REPLICATE_TABLES_WITH_ACCESS_METRICS(
    DBNAME       STRING,
    SCHEMANAME   STRING
)
    RETURNS VARCHAR(25200)
    LANGUAGE javascript
    EXECUTE AS CALLER
AS
$$

var taskDetails = "replicate_tables_with_access_metrics_task ---> Getting metadata";
var task        = "replicate_tables_with_access_metrics_task";

function logError(err, taskName) {
    try {
        var errStr  = (err && err.message) ? err.message : String(err);
        var taskStr = taskName ? String(taskName) : '';
        snowflake.createStatement({
            sqlText: "INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), 'FAILED', ?, ?)",
            binds: [errStr, taskStr]
        }).execute();
    } catch (e) { }
}

function insertToReplicationLog(status, message, taskName) {
    try {
        var statusStr  = status   ? String(status)   : '';
        var messageStr = message  ? String(message)  : '';
        var taskStr    = taskName ? String(taskName) : '';
        snowflake.createStatement({
            sqlText: "INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), ?, ?, ?)",
            binds: [statusStr, messageStr, taskStr]
        }).execute();
    } catch (e) { }
}

var schemaName   = SCHEMANAME;
var dbName       = DBNAME;
var error        = "";
var returnVal    = "SUCCESS";

function truncateTable(tableName) {
    try {
        snowflake.createStatement({
            sqlText: "TRUNCATE TABLE IF EXISTS " + dbName + "." + schemaName + "." + tableName + ";"
        }).execute();
    } catch (err) {
        logError(err, taskDetails);
        error += "Failed: " + err;
    }
}

function getColumns(tableName) {
    var columns     = "";
    var columnQuery = "SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) AS ALL_COLUMNS " +
                      "FROM " + dbName + ".INFORMATION_SCHEMA.COLUMNS " +
                      "WHERE TABLE_NAME = '"   + tableName  + "' " +
                      "  AND TABLE_SCHEMA = '" + schemaName + "';";
    try {
        var res = snowflake.createStatement({ sqlText: columnQuery }).execute();
        res.next();
        columns = res.getColumnValue(1);
    } catch (err) {
        logError(err, taskDetails);
        error += "Failed: " + err;
    }
    return columns;
}

function replicateTablesWithAccessMetrics() {
    var tableName = "TABLES";
    truncateTable(tableName);

    var columns = getColumns(tableName);
    if (!columns) {
        error += "Failed: could not retrieve columns for TABLES";
        return;
    }

    var excludedColumns = [
        "LATEST_WRITE_TIME",
        "LATEST_ACCESS_TIME",
        "ACCESS_COUNT_LAST_15_DAYS",
        "ACCESS_COUNT_LAST_30_DAYS",
        "ACCESS_COUNT_LAST_45_DAYS",
        "ACCESS_COUNT_LAST_60_DAYS",
        "ACCESS_COUNT_LAST_90_DAYS",
        "ACCESS_COUNT_LAST_180_DAYS",
        "WRITE_COUNT_LAST_15_DAYS",
        "WRITE_COUNT_LAST_30_DAYS",
        "WRITE_COUNT_LAST_45_DAYS",
        "WRITE_COUNT_LAST_60_DAYS",
        "WRITE_COUNT_LAST_90_DAYS",
        "WRITE_COUNT_LAST_180_DAYS"
    ];

    var quotedColumns = columns
        .split(',')
        .map(item => item.trim())
        .filter(item => excludedColumns.indexOf(item.toUpperCase()) === -1)
        .map(item => 's."' + item + '"')
        .join(', ');

    var insertQuery =
        "INSERT INTO " + dbName + "." + schemaName + "." + tableName + "\n" +
        "WITH FilteredAccessHistory AS (\n" +
        "    SELECT\n" +
        "        objects_modified,\n" +
        "        object_modified_by_ddl,\n" +
        "        base_objects_accessed,\n" +
        "        query_start_time\n" +
        "    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY\n" +
        "    WHERE query_start_time <= CURRENT_TIMESTAMP()\n" +
        "      AND query_start_time >  DATEADD(DAY, -180, CURRENT_TIMESTAMP())\n" +
        "),\n" +
        "table_dml_details AS (\n" +
        "    SELECT\n" +
        "        objects_modified.value:objectId::INTEGER AS table_id,\n" +
        "        MAX(query_start_time)                    AS last_dml,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -15, CURRENT_TIMESTAMP()) THEN 1 END) AS dml_15,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -30, CURRENT_TIMESTAMP()) THEN 1 END) AS dml_30,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -45, CURRENT_TIMESTAMP()) THEN 1 END) AS dml_45,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -60, CURRENT_TIMESTAMP()) THEN 1 END) AS dml_60,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -90, CURRENT_TIMESTAMP()) THEN 1 END) AS dml_90,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -180, CURRENT_TIMESTAMP()) THEN 1 END) AS dml_180\n" +
        "    FROM FilteredAccessHistory ah,\n" +
        "         LATERAL FLATTEN(input => ah.objects_modified) objects_modified\n" +
        "    WHERE objects_modified.value:objectDomain::TEXT = 'Table'\n" +
        "    GROUP BY objects_modified.value:objectId::INTEGER\n" +
        "),\n" +
        "table_ddl_details AS (\n" +
        "    SELECT\n" +
        "        object_modified_by_ddl:objectId::INTEGER AS table_id,\n" +
        "        MAX(query_start_time)                    AS last_ddl,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -15, CURRENT_TIMESTAMP()) THEN 1 END) AS ddl_15,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -30, CURRENT_TIMESTAMP()) THEN 1 END) AS ddl_30,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -45, CURRENT_TIMESTAMP()) THEN 1 END) AS ddl_45,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -60, CURRENT_TIMESTAMP()) THEN 1 END) AS ddl_60,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -90, CURRENT_TIMESTAMP()) THEN 1 END) AS ddl_90,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -180, CURRENT_TIMESTAMP()) THEN 1 END) AS ddl_180\n" +
        "    FROM FilteredAccessHistory\n" +
        "    WHERE object_modified_by_ddl:objectDomain::TEXT = 'Table'\n" +
        "    GROUP BY object_modified_by_ddl:objectId::INTEGER\n" +
        "),\n" +
        "table_access_details AS (\n" +
        "    SELECT\n" +
        "        objects_accessed.value:objectId::INTEGER AS table_id,\n" +
        "        MAX(query_start_time)                    AS last_access_time,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -15, CURRENT_TIMESTAMP()) THEN 1 END) AS access_15,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -30, CURRENT_TIMESTAMP()) THEN 1 END) AS access_30,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -45, CURRENT_TIMESTAMP()) THEN 1 END) AS access_45,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -60, CURRENT_TIMESTAMP()) THEN 1 END) AS access_60,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -90, CURRENT_TIMESTAMP()) THEN 1 END) AS access_90,\n" +
        "        COUNT(CASE WHEN query_start_time > DATEADD(DAY, -180, CURRENT_TIMESTAMP()) THEN 1 END) AS access_180\n" +
        "    FROM FilteredAccessHistory ah,\n" +
        "         LATERAL FLATTEN(input => ah.base_objects_accessed) objects_accessed\n" +
        "    WHERE objects_accessed.value:objectDomain::TEXT = 'Table'\n" +
        "    GROUP BY objects_accessed.value:objectId::INTEGER\n" +
        "),\n" +
        "SourceData AS (\n" +
        "    SELECT *\n" +
        "    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES\n" +
        "    WHERE DELETED IS NULL\n" +
        "      AND TABLE_CATALOG NOT ILIKE 'snowflake'\n" +
        ")\n" +
        "SELECT\n" +
        "    " + quotedColumns + ",\n" +
        "    GREATEST(\n" +
        "        COALESCE(dml.last_dml, TO_TIMESTAMP('1900-01-01')),\n" +
        "        COALESCE(ddl.last_ddl, TO_TIMESTAMP('1900-01-01'))\n" +
        "    )                                                        AS LATEST_WRITE_TIME,\n" +
        "    GREATEST(\n" +
        "        COALESCE(dml.last_dml,         TO_TIMESTAMP('1900-01-01')),\n" +
        "        COALESCE(ddl.last_ddl,         TO_TIMESTAMP('1900-01-01')),\n" +
        "        COALESCE(acc.last_access_time, TO_TIMESTAMP('1900-01-01'))\n" +
        "    )                                                        AS LATEST_ACCESS_TIME,\n" +
        "    COALESCE(acc.access_15, 0)                               AS ACCESS_COUNT_LAST_15_DAYS,\n" +
        "    COALESCE(acc.access_30, 0)                               AS ACCESS_COUNT_LAST_30_DAYS,\n" +
        "    COALESCE(acc.access_45, 0)                               AS ACCESS_COUNT_LAST_45_DAYS,\n" +
        "    COALESCE(acc.access_60, 0)                               AS ACCESS_COUNT_LAST_60_DAYS,\n" +
        "    COALESCE(acc.access_90, 0)                               AS ACCESS_COUNT_LAST_90_DAYS,\n" +
        "    COALESCE(acc.access_180, 0)                               AS ACCESS_COUNT_LAST_180_DAYS,\n" +
        "    COALESCE(dml.dml_15, 0) + COALESCE(ddl.ddl_15, 0)       AS WRITE_COUNT_LAST_15_DAYS,\n" +
        "    COALESCE(dml.dml_30, 0) + COALESCE(ddl.ddl_30, 0)       AS WRITE_COUNT_LAST_30_DAYS,\n" +
        "    COALESCE(dml.dml_45, 0) + COALESCE(ddl.ddl_45, 0)       AS WRITE_COUNT_LAST_45_DAYS,\n" +
        "    COALESCE(dml.dml_60, 0) + COALESCE(ddl.ddl_60, 0)       AS WRITE_COUNT_LAST_60_DAYS,\n" +
        "    COALESCE(dml.dml_90, 0) + COALESCE(ddl.ddl_90, 0)       AS WRITE_COUNT_LAST_90_DAYS,\n" +
        "    COALESCE(dml.dml_180, 0) + COALESCE(ddl.ddl_180, 0)       AS WRITE_COUNT_LAST_180_DAYS\n" +
        "FROM SourceData s\n" +
        "LEFT JOIN table_dml_details    dml ON s.TABLE_ID = dml.table_id\n" +
        "LEFT JOIN table_ddl_details    ddl ON s.TABLE_ID = ddl.table_id\n" +
        "LEFT JOIN table_access_details acc ON s.TABLE_ID = acc.table_id;";

    try {
        snowflake.createStatement({ sqlText: insertQuery }).execute();
    } catch (err) {
        logError(err, taskDetails);
        error += "Failed: " + err;
    }
}

insertToReplicationLog("started", "replicate_tables_with_access_metrics_task started", task);

replicateTablesWithAccessMetrics();

if (error.length > 0) {
    insertToReplicationLog("failed", error, task);
    return error;
}

insertToReplicationLog("completed", "replicate_tables_with_access_metrics_task completed", task);
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

insertToReplicationLog("started", "history_query_task started", task);

replicateData("QUERY_HISTORY", true, "START_TIME");
replicateData("SESSIONS", true, "CREATED_ON");
replicateData("ACCESS_HISTORY", true, "QUERY_START_TIME");
replicateData("QUERY_INSIGHTS", true, "START_TIME");

if(error.length > 0 ) {
    return error;
}

insertToReplicationLog("completed", "history_query_task completed", task);

return returnVal;
$$;


--PROCEDURE FOR REPLICATE REALTIME QUERY
CREATE OR REPLACE PROCEDURE REPLICATE_REALTIME_QUERY(DBNAME STRING, SCHEMANAME STRING, LOOK_BACK_HOURS STRING)
    returns VARCHAR(25200)
    LANGUAGE javascript
    EXECUTE AS CALLER

AS
$$

var taskDetails = "realtime_query_task started ---> Getting realtime data ";
var task= "realtime_query_task";
var schemaName = SCHEMANAME;
var dbName = DBNAME;
var lookBackHours = -parseInt(LOOK_BACK_HOURS);
var error = "";
var returnVal = "SUCCESS";

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

insertToReplicationLog("started", "realtime_query_task started ", task);

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

insertToReplicationLog("completed", "realtime_query_task completed", task);

return returnVal;
$$;

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

insertToReplicationLog("started", "realtime_query_task started", task);
var returnVal = "SUCCESS";
var error = "";
var lookBackHours = -parseInt(LOOK_BACK_HOURS);

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
	logError(err, warehouse_proc_task);
    error += "Failed: " + err;
}

if (error.length > 0) {
	return error;
}

insertToReplicationLog("completed", "realtime_query_task completed", task);

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
   // 1. SHOW WAREHOUSES
    var showWarehouse = snowflake.createStatement({sqlText: "SHOW WAREHOUSES"});
    showWarehouse.execute();

    // 2. Get LAST_QUERY_ID
    var query_id_stmt = snowflake.createStatement({sqlText: "SELECT LAST_QUERY_ID()"});
    var query_id_result = query_id_stmt.execute();
    query_id_result.next();
    var query_id = query_id_result.getColumnValue(1);

    // 3. DESCRIBE RESULT
    var describe_sql = `DESCRIBE RESULT '${query_id}'`;
    var describe_stmt = snowflake.createStatement({sqlText: describe_sql});
    var describe_result = describe_stmt.execute();

    var column_defs = [];
    var column_names = [];

    while (describe_result.next()) {
        var col_name = describe_result.getColumnValue("name");
        var data_type = describe_result.getColumnValue("type");
        column_names.push(`"${col_name}"`);
        column_defs.push(`"${col_name}" ${data_type}`);
    }

    // 4. CREATE TABLE IF NOT EXISTS
    var create_table_sql = `CREATE TRANSIENT TABLE IF NOT EXISTS "${DBNAME}"."${SCHEMANAME}".WAREHOUSES (
        ${column_defs.join(",\n    ")}
    );`;
    var create_stmt = snowflake.createStatement({sqlText: create_table_sql});
    create_stmt.execute();

     // 5. TRUNCATE TABLE
    var truncate_sql = `TRUNCATE TABLE IF EXISTS "${DBNAME}"."${SCHEMANAME}".WAREHOUSES;`;
    var truncate_stmt = snowflake.createStatement({sqlText: truncate_sql});
    truncate_stmt.execute();

    // 5.1 Add logic to get common columns between SHOW WAREHOUSES and WAREHOUSES table
    var existing_columns = getColumns("WAREHOUSES");
    existing_columns = existing_columns.split(',').map(item => `"${item.trim()}"`);;
    column_names = column_names.filter(col => existing_columns.includes(col));

   // 6. INSERT INTO
    var insert_sql_wh = `INSERT INTO "${DBNAME}"."${SCHEMANAME}".WAREHOUSES (${column_names.join(", ")})
                      SELECT ${column_names.join(", ")} FROM TABLE(RESULT_SCAN('${query_id}'));`;
    var insert_stmt_wh = snowflake.createStatement({sqlText: insert_sql_wh});
    insert_stmt_wh.execute();

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

-- PROCEDURE FOR SHARED DB METADATA
CREATE OR REPLACE PROCEDURE create_shared_db_metadata(DATABASE_NAME STRING, SCHEMA_NAME STRING)
  RETURNS VARIANT
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$
try {
    const status = "success";

    // Helper function to execute SQL and return a single column as an array
    const executeSQL = (sqlText, column_name) => {
        let result = [];
        const res = snowflake.createStatement({ sqlText }).execute();
        while (res.next()) {
            const columnValue = res.getColumnValue(column_name);
            if (columnValue && columnValue !== "SNOWFLAKE") {
                result.push(columnValue);
            }
        }
        return result;
    };

        // 1️ Create SHARED_* tables if they do not exist
    const createTableSQLs = [
        `CREATE TRANSIENT TABLE IF NOT EXISTS ${DATABASE_NAME}.${SCHEMA_NAME}.SHARED_TABLES AS
         SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE 1=0;`,
        `CREATE TRANSIENT TABLE IF NOT EXISTS ${DATABASE_NAME}.${SCHEMA_NAME}.SHARED_VIEWS AS
         SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE 1=0;`,
        `CREATE TRANSIENT TABLE IF NOT EXISTS ${DATABASE_NAME}.${SCHEMA_NAME}.SHARED_COLUMNS AS
         SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE 1=0;`
    ];
    createTableSQLs.forEach(sql => snowflake.createStatement({ sqlText: sql }).execute());

    // Precompute column lists + NOT EXISTS condition for all SHARED_* tables
    const sharedTablesMeta = {};
    const sharedObjects = ["SHARED_TABLES", "SHARED_VIEWS", "SHARED_COLUMNS"];

    for (const tbl of sharedObjects) {
             if(tbl === "SHARED_COLUMNS") {
                sharedTablesMeta[tbl] = {
                notExistsCondition:
                    "m.TABLE_NAME = t.TABLE_NAME AND m.TABLE_SCHEMA = t.TABLE_SCHEMA AND m.TABLE_CATALOG = t.TABLE_CATALOG AND m.COLUMN_NAME = t.COLUMN_NAME"
            };
            } else {
            sharedTablesMeta[tbl] = {

                notExistsCondition:
                    "m.TABLE_NAME = t.TABLE_NAME AND m.TABLE_SCHEMA = t.TABLE_SCHEMA AND m.TABLE_CATALOG = t.TABLE_CATALOG"
            };
            }
    }

    // Function to insert metadata using precomputed column lists
    const insertSharedMetadata = (sharedTable, sourceDB, sourceSchema, sourceTable) => {
        const meta = sharedTablesMeta[sharedTable];
        if (!meta) return;

        let insertSQL = `
        INSERT INTO ${DATABASE_NAME}.${SCHEMA_NAME}.${sharedTable}
        SELECT *
        FROM ${sourceDB}.${sourceSchema}.${sourceTable} t
        WHERE t.TABLE_SCHEMA != 'INFORMATION_SCHEMA'
        AND NOT EXISTS (
            SELECT 1
            FROM ${DATABASE_NAME}.${SCHEMA_NAME}.${sharedTable} m
            WHERE ${meta.notExistsCondition}
        );`;

        snowflake.createStatement({ sqlText: insertSQL }).execute();
    };


    // 2️ Get list of shared databases
    const dbShares = executeSQL("SHOW SHARES;", "database_name");

    // 3️ Loop through shared databases and populate metadata
    for (const shareDB of dbShares) {
        insertSharedMetadata("SHARED_TABLES", shareDB, "INFORMATION_SCHEMA", "TABLES");
        insertSharedMetadata("SHARED_VIEWS", shareDB, "INFORMATION_SCHEMA", "VIEWS");
        insertSharedMetadata("SHARED_COLUMNS", shareDB, "INFORMATION_SCHEMA", "COLUMNS");
    }

    return status;

} catch (err) {
    return {status: "failure", message: err.message};
}
$$;


/**
 PROCEDURE to share data.
**/

CREATE OR REPLACE PROCEDURE SHARE_TO_ACCOUNT(ACCOUNTID VARCHAR)
RETURNS STRING NOT NULL
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
use_statement VARCHAR;
res RESULTSET;
BEGIN
CREATE SHARE S_SECURE_SHARE;
GRANT USAGE ON DATABASE UNRAVEL_SHARE to share S_SECURE_SHARE;
GRANT USAGE ON SCHEMA SCHEMA_4827_T to share S_SECURE_SHARE;
GRANT SELECT ON TABLE WAREHOUSE_METERING_HISTORY to share S_SECURE_SHARE;
GRANT SELECT ON TABLE WAREHOUSE_EVENTS_HISTORY to share S_SECURE_SHARE;
GRANT SELECT ON TABLE WAREHOUSE_LOAD_HISTORY to share S_SECURE_SHARE;
GRANT SELECT ON TABLE COLUMNS to share S_SECURE_SHARE;
GRANT SELECT ON TABLE TAGS to share S_SECURE_SHARE;
GRANT SELECT ON TABLE TAG_REFERENCES to share S_SECURE_SHARE;
GRANT SELECT ON TABLE TABLES to share S_SECURE_SHARE;
GRANT SELECT ON TABLE TABLE_STORAGE_METRICS to share S_SECURE_SHARE;
GRANT SELECT ON TABLE METERING_DAILY_HISTORY to share S_SECURE_SHARE;
GRANT SELECT ON TABLE METERING_HISTORY to share S_SECURE_SHARE;
GRANT SELECT ON TABLE DATABASE_REPLICATION_USAGE_HISTORY to share S_SECURE_SHARE;
GRANT SELECT ON TABLE REPLICATION_GROUP_USAGE_HISTORY to share S_SECURE_SHARE;
GRANT SELECT ON TABLE SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY to share S_SECURE_SHARE;
GRANT SELECT ON TABLE QUERY_HISTORY to share S_SECURE_SHARE;
GRANT SELECT ON TABLE SESSIONS to share S_SECURE_SHARE;
GRANT SELECT ON TABLE ACCESS_HISTORY to share S_SECURE_SHARE;
GRANT SELECT ON TABLE IS_QUERY_HISTORY to share S_SECURE_SHARE;
GRANT SELECT ON TABLE WAREHOUSE_PARAMETERS to share S_SECURE_SHARE;
GRANT SELECT ON TABLE WAREHOUSES to share S_SECURE_SHARE;
GRANT SELECT ON TABLE QUERY_PROFILE to share S_SECURE_SHARE;
GRANT SELECT ON TABLE DATABASE_STORAGE_USAGE_HISTORY to share S_SECURE_SHARE;
GRANT SELECT ON TABLE STAGE_STORAGE_USAGE_HISTORY to share S_SECURE_SHARE;
GRANT SELECT ON TABLE SEARCH_OPTIMIZATION_HISTORY to share S_SECURE_SHARE;
GRANT SELECT ON TABLE DATA_TRANSFER_HISTORY to share S_SECURE_SHARE;
GRANT SELECT ON TABLE AUTOMATIC_CLUSTERING_HISTORY to share S_SECURE_SHARE;
GRANT SELECT ON TABLE AUTO_REFRESH_REGISTRATION_HISTORY to share S_SECURE_SHARE;
GRANT SELECT ON TABLE REPLICATION_LOG to share S_SECURE_SHARE;
GRANT SELECT ON TABLE SHARED_TABLES to share S_SECURE_SHARE;
GRANT SELECT ON TABLE SHARED_VIEWS to share S_SECURE_SHARE;
GRANT SELECT ON TABLE SHARED_COLUMNS to share S_SECURE_SHARE;
GRANT SELECT ON TABLE QUERY_INSIGHTS to share S_SECURE_SHARE;
GRANT SELECT ON TABLE GRANTS_TO_ROLES to share S_SECURE_SHARE;
GRANT SELECT ON TABLE GRANTS_TO_SHARES to share S_SECURE_SHARE;
GRANT SELECT ON TABLE GRANTS_TO_USERS to share S_SECURE_SHARE;
GRANT SELECT ON TABLE ROLES to share S_SECURE_SHARE;
GRANT SELECT ON TABLE USERS to share S_SECURE_SHARE;
use_statement := 'ALTER SHARE S_SECURE_SHARE add accounts = ' || ACCOUNTID::VARIANT::VARCHAR;
res := (EXECUTE IMMEDIATE :use_statement);
RETURN 'SUCCESS';
END;
/**
    Step-1a: ENDED (procedure creation done.)
**/

/**
    Step-1b: (One time execution for 180 days (History data) start)
**/
CALL CREATE_TABLES('UNRAVEL_SHARE','SCHEMA_4827_T');
CALL REPLICATE_ACCOUNT_USAGE('UNRAVEL_SHARE','SCHEMA_4827_T',180);
CALL REPLICATE_TABLES_WITH_ACCESS_METRICS('UNRAVEL_SHARE', 'SCHEMA_4827_T');
CALL REPLICATE_HISTORY_QUERY('UNRAVEL_SHARE','SCHEMA_4827_T',180);
CALL WAREHOUSE_PROC('UNRAVEL_SHARE','SCHEMA_4827_T');
CALL CREATE_QUERY_PROFILE('UNRAVEL_SHARE', 'SCHEMA_4827_T', '1', '14');
CALL create_shared_db_metadata('UNRAVEL_SHARE','SCHEMA_4827_T');
/**
    Step-1b: ENDED (One time execution for history data end date for 180 days done.)
**/


/**
  Step-1c: SHARE tables to unravel accountId
**/
CALL SHARE_TO_ACCOUNT('<Unravel Account Identifier>');
/**
  Step-1c: ENDED SHARE tables to unravel accountId done.
**/


/**
    Step-2 : Once step-1a, step-1b and step-1c is done, then customer to inform Unravel for polling history(180d) the data on SaaS.
**/

/**
    Step-3 : (One time execution for after history data shared,  delta days of data (history data end date to current date) start)
    // Assuming history data load is done. 3 days later you want to start continuous polling, then delta days will be 3.
    // So, you need to run below procedure with delta days value.
**/
CALL REPLICATE_ACCOUNT_USAGE('UNRAVEL_SHARE','SCHEMA_4827_T', 3);
CALL REPLICATE_TABLES_WITH_ACCESS_METRICS('UNRAVEL_SHARE', 'SCHEMA_4827_T');
CALL REPLICATE_HISTORY_QUERY('UNRAVEL_SHARE','SCHEMA_4827_T', 3);
CALL WAREHOUSE_PROC('UNRAVEL_SHARE','SCHEMA_4827_T');
CALL CREATE_QUERY_PROFILE('UNRAVEL_SHARE', 'SCHEMA_4827_T', '1', '3');
CALL create_shared_db_metadata('UNRAVEL_SHARE','SCHEMA_4827_T');
/**
    Step-3: ENDED (One time execution ,  delta days of data (history data end date to current date) start)
**/

/**
    Step-4 : Once step-3 is done, then customer to inform Unravel for polling the delta data on SaaS.
**/

/**
    Step-5: Create Tasks for incremental data load, schedule as per requirement
**/
CREATE OR REPLACE TASK replicate_metadata
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = 'USING CRON 0 3,9,15,21 * * * UTC'
AS
CALL REPLICATE_ACCOUNT_USAGE('UNRAVEL_SHARE','SCHEMA_4827_T',2);

CREATE OR REPLACE TASK REPLICATE_TABLES_WITH_ACCESS_METRICS_TASK
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = 'USING CRON 0 0 * * * UTC'
AS
CALL REPLICATE_TABLES_WITH_ACCESS_METRICS('UNRAVEL_SHARE','SCHEMA_4827_T');

CREATE OR REPLACE TASK replicate_history_query
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = '60 MINUTE'
AS
CALL REPLICATE_HISTORY_QUERY('UNRAVEL_SHARE','SCHEMA_4827_T',2);

CREATE OR REPLACE TASK createProfileTable
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = '60 MINUTE'
AS
CALL create_query_profile('UNRAVEL_SHARE', 'SCHEMA_4827_T', '1', '2');

CREATE OR REPLACE TASK replicate_warehouse_and_realtime_query
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = '720 MINUTE'
AS
BEGIN
    CALL warehouse_proc('UNRAVEL_SHARE','SCHEMA_4827_T');
END;

CREATE OR REPLACE TASK shared_db_metadata_task
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = '720 MINUTE'
AS
BEGIN
    CALL create_shared_db_metadata('UNRAVEL_SHARE','SCHEMA_4827_T');
END;

/**
  (Resume all TASKS)
**/
ALTER TASK replicate_metadata RESUME;
ALTER TASK REPLICATE_TABLES_WITH_ACCESS_METRICS_TASK RESUME;
ALTER TASK replicate_history_query RESUME;
ALTER TASK createProfileTable RESUME;
ALTER TASK replicate_warehouse_and_realtime_query RESUME;
ALTER TASK shared_db_metadata_task RESUME;
/**
    Step-5: ENDED Create Tasks for incremental data load, schedule as per requirement done.
**/


/**
    Step-6 : Once step-5 is done, then customer to inform Unravel to monitor the continuous secure share data loading on SaaS.
**/

