CREATE OR REPLACE PROCEDURE TP_CDC_METADATA(SOURCE_DB STRING, SOURCE_SCHEMA STRING, TARGET_DB STRING, TARGET_SCHEMA STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var res;

    var tablesToInsert = [
        { tableName: "WAREHOUSE_METERING_HISTORY", condition: "START_TIME || WAREHOUSE_ID" },
        { tableName: "WAREHOUSE_EVENTS_HISTORY", condition: "TIMESTAMP || WAREHOUSE_ID" },
        { tableName: "WAREHOUSE_LOAD_HISTORY", condition: "START_TIME || WAREHOUSE_ID" },
        { tableName: "METERING_DAILY_HISTORY", condition: "SERVICE_TYPE || USAGE_DATE" },
        { tableName: "METERING_HISTORY", condition: "SERVICE_TYPE || START_TIME" },
        { tableName: "DATABASE_REPLICATION_USAGE_HISTORY", condition: "START_TIME || DATABASE_NAME" },
        { tableName: "REPLICATION_GROUP_USAGE_HISTORY", condition: "START_TIME || REPLICATION_GROUP_ID" },
        { tableName: "DATABASE_STORAGE_USAGE_HISTORY", condition: "DATABASE_ID || USAGE_DATE" },
        { tableName: "STAGE_STORAGE_USAGE_HISTORY", condition: "USAGE_DATE" },
        { tableName: "SEARCH_OPTIMIZATION_HISTORY", condition: "TABLE_ID || START_TIME" },
        { tableName: "DATA_TRANSFER_HISTORY", condition: "SOURCE_CLOUD || SOURCE_REGION || TARGET_CLOUD || TARGET_REGION || START_TIME" },
        { tableName: "AUTOMATIC_CLUSTERING_HISTORY", condition: "START_TIME || TABLE_ID || INSTANCE_ID" },
        { tableName: "SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY", condition: "START_TIME || TABLE_ID" },
        { tableName: "QUERY_HISTORY", condition: "QUERY_ID" },
        { tableName: "ACCESS_HISTORY", condition: "QUERY_ID" },
        { tableName: "REPLICATION_LOG", condition: "TASKNAME || EVENTDATE" },
        { tableName: "TABLES", condition: "TABLE_ID || LAST_ALTERED || LAST_DDL" },
        { tableName: "TABLE_STORAGE_METRICS", condition: "" },
        { tableName: "COLUMNS", condition: "COLUMN_ID" },
        { tableName: "WAREHOUSE_PARAMETERS", condition: "" },
        { tableName: "WAREHOUSES", condition: "" },
        { tableName: "TAG_REFERENCES", condition: "" },
        { tableName: "TAGS", condition: "" },
        { tableName: "SESSIONS", condition: "SESSION_ID" },
        { tableName: "AUTO_REFRESH_REGISTRATION_HISTORY", condition: "" },
        { tableName: "QUERY_PROFILE", condition: "QUERY_ID" },
        { tableName: "IS_QUERY_HISTORY", condition: "" }
    ];
    var result = "";
    for (var i = 0; i < tablesToInsert.length; i++) {
        var table = tablesToInsert[i];
        var insertSql ="";
        try{
        var condition = table.condition ? table.condition : "1=1";

        insertSql = `INSERT INTO ${TARGET_DB}.${TARGET_SCHEMA}.${table.tableName}
                   SELECT *, current_date as status_date
                   FROM ${SOURCE_DB}.${SOURCE_SCHEMA}.${table.tableName} T
                   WHERE ${condition} NOT IN (SELECT ${condition} FROM ${TARGET_DB}.${TARGET_SCHEMA}.${table.tableName})`;

        res = snowflake.execute({ sqlText: insertSql });
        } catch (err) {
            result += "  Error inserting table " + table.tableName + ": " + err.message + insertSql;
        }
    }
    if(result.length > 0)
    {
       return result;
    }
    return "SUCCESS";
} catch(err) {
    return "Error: " + err.message;
}
$$;


CREATE OR REPLACE PROCEDURE CREATE_BACKUP_DB_TABLES(SOURCE_DB STRING, SOURCE_SCHEMA STRING, TARGET_DB STRING, TARGET_SCHEMA STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var use_statement;
    var res;

    use_statement = "CREATE DATABASE IF NOT EXISTS " + TARGET_DB;
    res = snowflake.execute({ sqlText: use_statement });

    use_statement = "CREATE SCHEMA IF NOT EXISTS " + TARGET_DB + "." + TARGET_SCHEMA;
    res = snowflake.execute({ sqlText: use_statement });

    var tablesToCreate = [
        { tableName: "WAREHOUSE_METERING_HISTORY", condition: "START_TIME || WAREHOUSE_ID" },
        { tableName: "WAREHOUSE_EVENTS_HISTORY", condition: "TIMESTAMP || WAREHOUSE_ID" },
        { tableName: "WAREHOUSE_LOAD_HISTORY", condition: "START_TIME || WAREHOUSE_ID" },
        { tableName: "METERING_DAILY_HISTORY", condition: "SERVICE_TYPE || USAGE_DATE" },
        { tableName: "METERING_HISTORY", condition: "SERVICE_TYPE || START_TIME" },
        { tableName: "DATABASE_REPLICATION_USAGE_HISTORY", condition: "START_TIME || DATABASE_NAME" },
        { tableName: "REPLICATION_GROUP_USAGE_HISTORY", condition: "START_TIME || REPLICATION_GROUP_ID" },
        { tableName: "DATABASE_STORAGE_USAGE_HISTORY", condition: "DATABASE_ID || USAGE_DATE" },
        { tableName: "STAGE_STORAGE_USAGE_HISTORY", condition: "USAGE_DATE" },
        { tableName: "SEARCH_OPTIMIZATION_HISTORY", condition: "TABLE_ID || START_TIME" },
        { tableName: "DATA_TRANSFER_HISTORY", condition: "SOURCE_CLOUD || SOURCE_REGION || TARGET_CLOUD || TARGET_REGION || START_TIME" },
        { tableName: "AUTOMATIC_CLUSTERING_HISTORY", condition: "START_TIME || TABLE_ID || INSTANCE_ID" },
        { tableName: "SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY", condition: "START_TIME || TABLE_ID" },
        { tableName: "QUERY_HISTORY", condition: "QUERY_ID" },
        { tableName: "ACCESS_HISTORY", condition: "QUERY_ID" },
        { tableName: "REPLICATION_LOG", condition: "TASKNAME || EVENTDATE" },
        { tableName: "TABLES", condition: "TABLE_ID || LAST_ALTERED || LAST_DDL" },
        { tableName: "TABLE_STORAGE_METRICS", condition: "" },
        { tableName: "COLUMNS", condition: "COLUMN_ID" },
        { tableName: "WAREHOUSE_PARAMETERS", condition: "" },
        { tableName: "WAREHOUSES", condition: "" },
        { tableName: "TAG_REFERENCES", condition: "" },
        { tableName: "TAGS", condition: "" },
        { tableName: "SESSIONS", condition: "SESSION_ID" },
        { tableName: "AUTO_REFRESH_REGISTRATION_HISTORY", condition: "" },
        { tableName: "QUERY_PROFILE", condition: "QUERY_ID" },
        { tableName: "IS_QUERY_HISTORY", condition: "" }
    ];
    var result = "";

    for (var i = 0; i < tablesToCreate.length; i++) {
        var table = tablesToCreate[i];
    try{
       var createTableQuery = `CREATE TRANSIENT TABLE IF NOT EXISTS  ${TARGET_DB}.${TARGET_SCHEMA}.${table.tableName}
                       WITH DATA_RETENTION_TIME_IN_DAYS = 0 LIKE  ${SOURCE_DB}.${SOURCE_SCHEMA}.${table.tableName}`;
        var createStatement = snowflake.createStatement({sqlText: createTableQuery});
        createStatement.execute();

        var alterTable = `ALTER TABLE ${TARGET_DB}.${TARGET_SCHEMA}.${table.tableName}  ADD COLUMN status_date DATE `;
        var alterTableStatement = snowflake.createStatement({sqlText: alterTable});
        alterTableStatement.execute();

      } catch (err) {
           result += "  Error Creating table " + table.tableName + ": " + err.message;
      }
    }
    if(result.length > 0)
    {
       return result;
    }
    return "SUCCESS";
} catch(err) {
    return "Error: " + err.message;
}
$$;


CALL CREATE_BACKUP_DB_TABLES("SOURCE_DB", "SOURCE_SCHEMA", "TARGET_DB", "TARGET_SCHEMA");
CALL TP_CDC_METADATA("SOURCE_DB", "SOURCE_SCHEMA", "TARGET_DB", "TARGET_SCHEMA");

/**
   Create tasks
*/
CREATE OR REPLACE TASK cdc_metadata
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = 'USING CRON 0 2 * * * UTC'
AS
CALL TP_CDC_METADATA("SOURCE_DB", "SOURCE_SCHEMA", "TARGET_DB", "TARGET_SCHEMA");

ALTER TASK cdc_metadata RESUME;