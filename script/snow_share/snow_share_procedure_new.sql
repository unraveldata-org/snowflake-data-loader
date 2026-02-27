/**
    Step-1a: Started (procedures creation started.)
**/

CREATE DATABASE IF NOT EXISTS UNRAVEL_SHARE;
USE UNRAVEL_SHARE;

CREATE SCHEMA IF NOT EXISTS SCHEMA_4827_T;
USE UNRAVEL_SHARE.SCHEMA_4827_T;

CREATE OR REPLACE PROCEDURE CREATE_ACCOUNT_USAGE_TABLES(
    SOURCE_DB     STRING DEFAULT 'SNOWFLAKE',
    SOURCE_SCHEMA STRING DEFAULT 'ACCOUNT_USAGE',
    TARGET_DB     STRING DEFAULT NULL,
    TARGET_SCHEMA STRING DEFAULT NULL
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var use_statement;
    var res;

    use_statement = `CREATE DATABASE IF NOT EXISTS ${TARGET_DB}`;
    res = snowflake.execute({ sqlText: use_statement });

    use_statement = `CREATE SCHEMA IF NOT EXISTS ${TARGET_DB}.${TARGET_SCHEMA}`;
    res = snowflake.execute({ sqlText: use_statement });

    use_statement = `USE ${TARGET_DB}.${TARGET_SCHEMA}`;
    res = snowflake.execute({ sqlText: use_statement });

    var accountUsageTablesToCreate = [
         {tableName: "WAREHOUSE_METERING_HISTORY"}
        ,{tableName: "WAREHOUSE_LOAD_HISTORY"}
        ,{tableName: "METERING_HISTORY"}
        ,{tableName: "DATABASE_REPLICATION_USAGE_HISTORY"}
        ,{tableName: "REPLICATION_GROUP_USAGE_HISTORY"}
        ,{tableName: "SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY"}
        ,{tableName: "SEARCH_OPTIMIZATION_HISTORY"}
        ,{tableName: "DATA_TRANSFER_HISTORY"}
        ,{tableName: "AUTOMATIC_CLUSTERING_HISTORY"}
        ,{tableName: "WAREHOUSE_EVENTS_HISTORY"}
        ,{tableName: "METERING_DAILY_HISTORY"}
        ,{tableName: "DATABASE_STORAGE_USAGE_HISTORY"}
        ,{tableName: "STAGE_STORAGE_USAGE_HISTORY"}
        ,{tableName: "STORAGE_USAGE"}
        ,{tableName: "TABLES"}
        ,{tableName: "TABLE_STORAGE_METRICS"}
        ,{tableName: "COLUMNS"}
        ,{tableName: "TAGS"}
        ,{tableName: "TAG_REFERENCES"}
        ,{tableName: "SESSIONS"}
        ,{tableName: "ACCESS_HISTORY"}
        ,{tableName: "QUERY_HISTORY"}
        ,{tableName: "QUERY_INSIGHTS"}
        ,{tableName: "GRANTS_TO_USERS"}
        ,{tableName: "GRANTS_TO_ROLES"}
        ,{tableName: "ROLES"}
        ,{tableName: "CORTEX_AISQL_USAGE_HISTORY"}
        ,{tableName: "CORTEX_ANALYST_USAGE_HISTORY"}
        ,{tableName: "CORTEX_FINE_TUNING_USAGE_HISTORY"}
        ,{tableName: "CORTEX_PROVISIONED_THROUGHPUT_USAGE_HISTORY"}
        ,{tableName: "CORTEX_REST_API_USAGE_HISTORY"}
        ,{tableName: "CORTEX_SEARCH_DAILY_USAGE_HISTORY"}
        ,{tableName: "CORTEX_SEARCH_SERVING_USAGE_HISTORY"}
        ,{tableName: "SNOWPARK_CONTAINER_SERVICES_HISTORY"}
        ,{tableName: "USERS"}
        //,{tableName: "STAGES"}
        //,{tableName: "PROCEDURES"}
        //,{tableName: "TASK_HISTORY"}
        //,{tableName: "TABLE_DML_HISTORY"}
        //,{tableName: "TABLE_PRUNING_HISTORY"}
    ];

    var result = "";
    for (var i = 0; i < accountUsageTablesToCreate.length; i++) {
        var table = accountUsageTablesToCreate[i];
        try {
            var createTableQuery = `CREATE TRANSIENT TABLE IF NOT EXISTS  ${TARGET_DB}.${TARGET_SCHEMA}.${table.tableName}
                                    WITH DATA_RETENTION_TIME_IN_DAYS = 0 LIKE  ${SOURCE_DB}.${SOURCE_SCHEMA}.${table.tableName}`;
            var createStatement = snowflake.createStatement({ sqlText: createTableQuery });
            createStatement.execute();

            var alterTable = `ALTER TABLE ${TARGET_DB}.${TARGET_SCHEMA}.${table.tableName} ADD COLUMN IF NOT EXISTS status_date DATE`;
            var alterTableStatement = snowflake.createStatement({ sqlText: alterTable });
            alterTableStatement.execute();
        } catch (err) {
            result += "  Error Creating table " + table.tableName + ": " + err.message;
        }
    }

    if (result.length > 0) {
        return result;
    }
    return "SUCCESS";
} catch (err) {
    return "Error: " + err.message;
}
$$;


CREATE OR REPLACE PROCEDURE CREATE_DERIVED_TABLES(
    SOURCE_DB     STRING DEFAULT NULL,
    SOURCE_SCHEMA STRING DEFAULT NULL,
    TARGET_DB     STRING DEFAULT NULL,
    TARGET_SCHEMA STRING DEFAULT NULL
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var use_statement;
    var res;

    use_statement = `CREATE DATABASE IF NOT EXISTS ${TARGET_DB}`;
    res = snowflake.execute({ sqlText: use_statement });

    use_statement = `CREATE SCHEMA IF NOT EXISTS ${TARGET_DB}.${TARGET_SCHEMA}`;
    res = snowflake.execute({ sqlText: use_statement });

    use_statement = `USE ${TARGET_DB}.${TARGET_SCHEMA}`;
    res = snowflake.execute({ sqlText: use_statement });

    if (!SOURCE_DB && !SOURCE_SCHEMA) {
        var createTableStmt = `CREATE TRANSIENT TABLE IF NOT EXISTS ${TARGET_DB}.${TARGET_SCHEMA}.REPLICATION_LOG (
                                      eventDate  TIMESTAMP_TZ(9) DEFAULT to_timestamp_tz(current_timestamp),
                                      executionStatus VARCHAR(1000) DEFAULT NULL,
                                      remarks VARCHAR(1000),
                                      taskName VARCHAR(500) DEFAULT NULL,
                                      status_date date
                                    )`;
        snowflake.createStatement({ sqlText: createTableStmt }).execute();
        var alterTable = `ALTER TABLE ${TARGET_DB}.${TARGET_SCHEMA}.REPLICATION_LOG ADD COLUMN IF NOT EXISTS status_date DATE `;
        snowflake.createStatement({ sqlText: alterTable }).execute();

        createTableStmt = `CREATE TRANSIENT TABLE IF NOT EXISTS ${TARGET_DB}.${TARGET_SCHEMA}.AUTO_REFRESH_REGISTRATION_HISTORY WITH
                           DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT *, current_date() as status_date FROM TABLE(INFORMATION_SCHEMA.AUTO_REFRESH_REGISTRATION_HISTORY()) WHERE 1=0`;
        snowflake.createStatement({ sqlText: createTableStmt }).execute();
        alterTable = `ALTER TABLE ${TARGET_DB}.${TARGET_SCHEMA}.AUTO_REFRESH_REGISTRATION_HISTORY ADD COLUMN IF NOT EXISTS status_date DATE `;
        snowflake.createStatement({ sqlText: alterTable }).execute();

        createTableStmt = `CREATE TRANSIENT TABLE IF NOT EXISTS ${TARGET_DB}.${TARGET_SCHEMA}.IS_QUERY_HISTORY WITH
                           DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT *, current_date() as status_date FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY()) WHERE 1=0`;
        snowflake.createStatement({sqlText: createTableStmt}).execute();
        alterTable = `ALTER TABLE ${TARGET_DB}.${TARGET_SCHEMA}.IS_QUERY_HISTORY ADD COLUMN IF NOT EXISTS status_date DATE `;
        snowflake.createStatement({sqlText: alterTable}).execute();

        createTableStmt = `CREATE TRANSIENT TABLE IF NOT EXISTS ${TARGET_DB}.${TARGET_SCHEMA}.CORTEX_SEARCH_REFRESH_HISTORY WITH
                           DATA_RETENTION_TIME_IN_DAYS=0 AS SELECT *, current_date() as status_date FROM TABLE(INFORMATION_SCHEMA.CORTEX_SEARCH_REFRESH_HISTORY()) WHERE 1=0`;
        snowflake.createStatement({sqlText: createTableStmt}).execute();
        alterTable = `ALTER TABLE ${TARGET_DB}.${TARGET_SCHEMA}.CORTEX_SEARCH_REFRESH_HISTORY ADD COLUMN IF NOT EXISTS status_date DATE `;
        snowflake.createStatement({sqlText: alterTable}).execute();
    } else {
        var tablesToCreate = [
             {tableName: "REPLICATION_LOG"}
            ,{tableName: "AUTO_REFRESH_REGISTRATION_HISTORY"}
            ,{tableName: "IS_QUERY_HISTORY"}
            ,{tableName: "QUERY_PROFILE"}
            ,{tableName: "WAREHOUSES"}
            ,{tableName: "WAREHOUSE_PARAMETERS"}
            ,{tableName: "SHARED_COLUMNS"}
            ,{tableName: "SHARED_TABLES"}
            ,{tableName: "SHARED_VIEWS"}
            ,{tableName: "CORTEX_SEARCH_REFRESH_HISTORY"}
        ];

        var result = "";
        for (var i = 0; i < tablesToCreate.length; i++) {
            var table = tablesToCreate[i];
            try {
                var createTableQuery = `CREATE TRANSIENT TABLE IF NOT EXISTS  ${TARGET_DB}.${TARGET_SCHEMA}.${table.tableName}
                               WITH DATA_RETENTION_TIME_IN_DAYS = 0 LIKE  ${SOURCE_DB}.${SOURCE_SCHEMA}.${table.tableName}`;
                var createStatement = snowflake.createStatement({ sqlText: createTableQuery });
                createStatement.execute();

                var alterTable = `ALTER TABLE ${TARGET_DB}.${TARGET_SCHEMA}.${table.tableName} ADD COLUMN IF NOT EXISTS status_date DATE `;
                var alterTableStatement = snowflake.createStatement({ sqlText: alterTable });
                alterTableStatement.execute();
            } catch (err) {
                result += "  Error Creating table " + table.tableName + ": " + err.message;
            }
        }

        if (result.length > 0) {
            return result;
        }
    }

    return "SUCCESS";
} catch (err) {
    return "Error: " + err.message;
}
$$;

-- PROCEDURE FOR REPLICATE ACCOUNT_USAGE
CREATE OR REPLACE PROCEDURE REPLICATE_ACCOUNT_USAGE(
  DB_NAME STRING,
  SCHEMA_NAME STRING,
  LOOK_BACK_DAYS STRING,
  SOURCE_DB STRING DEFAULT 'SNOWFLAKE',
  SOURCE_SCHEMA STRING DEFAULT 'ACCOUNT_USAGE'
)
    returns VARCHAR(25200)
    LANGUAGE javascript
    EXECUTE AS CALLER
AS
$$
var taskDetails = "replicate_metadata_task ---> Getting metadata ";
var task = "replicate_metadata_task";
var sourceDb = (SOURCE_DB && String(SOURCE_DB).trim().length > 0) ? String(SOURCE_DB).trim() : "SNOWFLAKE";
var sourceSchema = (SOURCE_SCHEMA && String(SOURCE_SCHEMA).trim().length > 0) ? String(SOURCE_SCHEMA).trim() : "ACCOUNT_USAGE";

function logError(err, taskName)
{
    snowflake.createStatement({
        sqlText: `INSERT INTO REPLICATION_LOG VALUES (TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()), 'FAILED', ?, ?, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`,
        binds: [String(err), String(taskName)]
    }).execute();
}

function insertToReplicationLog(status, message, taskName)
{
    snowflake.createStatement({
        sqlText: `INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), ?, ?, ?, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`,
        binds: [String(status), String(message), String(taskName)]
    }).execute();
}

var SCHEMA_NAME = SCHEMA_NAME;
var DB_NAME = DB_NAME;
var lookBackDays = parseInt(LOOK_BACK_DAYS);
var error = "";
var returnVal = "SUCCESS";

function getColumns(tableName)
{
    let columns = "";
    const columnQuery = `
        SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) AS ALL_COLUMNS
        FROM ${DB_NAME}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '${tableName}'
          AND TABLE_SCHEMA = '${SCHEMA_NAME}'
    `;
    const stmt = snowflake.createStatement({ sqlText: columnQuery });
    try {
        const res = stmt.execute();
        res.next();
        columns = res.getColumnValue(1);
    } catch (err) {
        logError(err, taskDetails);
        error += "Failed: " + err;
    }
    return columns;
}

// Create a join condition based on PK columns for incremental load to avoid duplicates in history tables. If PK columns are not provided,return null.
function getPkJoinCondition(pkCsv, leftAlias, rightAlias)
{
    let cols = (pkCsv || "").split(",").map(s => s.trim()).filter(s => s.length > 0);
    if (cols.length === 0) return null;
    return cols.map(c => `${leftAlias}."${c}" = ${rightAlias}."${c}"`).join(" AND ");
}

function buildSelectLists(targetColumnsCsv, mode) {
    if (!targetColumnsCsv) return null;

    return targetColumnsCsv
        .split(",")
        .map(c => c.trim())
        .filter(c => c.length > 0)
        .map(c => {
            if (c.toUpperCase() === "STATUS_DATE") {
                return `CURRENT_DATE() AS "STATUS_DATE"`;
            }
            if (mode === "LOWER_SOURCE") {
                return `"${c.toLowerCase()}" AS "${c}"`;
            }
            return `"${c}"`;
        })
        .join(",");
}


// For tables with date column, pull data within look back window and do incremental insert based on PK to avoid duplicates.
// For tables without date column, do full load every time.
// NOTE: `columns` param is the raw target column CSV from getColumns (unquoted), not pre-quoted anymore.
function insertToTable(destnTableName, sourceTableName, isDate, startDateCol, endDateCol, columns, primaryKeys)
{
    try {
        let insertQuery = "";
        const insertTable = `${DB_NAME}.${SCHEMA_NAME}.${destnTableName}`;

        // Build aligned select list
        const selectList = buildSelectLists(columns,"AS_IS");
        if (!selectList) {
            throw `No columns found in target table: ${insertTable}`;
        }

        if (isDate) {
            const joinCond = getPkJoinCondition(primaryKeys, "t", "w");

            const windowStart = `DATEADD('day', -${lookBackDays}, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`;
            const windowEnd = `TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP())`;

            let winPred = "";
            if (endDateCol && endDateCol.length > 0) {
                winPred = `(
                    ("${startDateCol}" >= ${windowStart})
                    OR ("${endDateCol}" IS NOT NULL AND "${endDateCol}" >= ${windowStart})
                ) AND "${startDateCol}" <= ${windowEnd}`;
            } else {
                winPred = `("${startDateCol}" >= ${windowStart} AND "${startDateCol}" <= ${windowEnd})`;
            }

            if (!joinCond) {
                insertQuery = `
                    INSERT INTO ${insertTable}
                    SELECT ${selectList}
                    FROM ${sourceTableName}
                    WHERE ${winPred}
                `;
            } else {
                insertQuery = `
                    INSERT INTO ${insertTable}
                    SELECT ${selectList}
                    FROM (
                        SELECT ${selectList}
                        FROM ${sourceTableName}
                        WHERE ${winPred}
                    ) w
                    WHERE NOT EXISTS (
                        SELECT 1 FROM ${insertTable} t WHERE ${joinCond}
                    )
                `;
            }
        } else {
            insertQuery = `
                INSERT INTO ${insertTable}
                SELECT ${selectList}
                FROM ${sourceTableName}
            `;
        }

        snowflake.createStatement({ sqlText: insertQuery }).execute();
    } catch (err) {
        logError(err, taskDetails);
        error += "Failed: " + err;
    }
}

function insertToAccountUsageTable(tableName, isDate, startDateCol, endDateCol, columns, primaryKeys)
{
    let sourceTableName = `${sourceDb}.${sourceSchema}.${tableName}`;
    return insertToTable(tableName, sourceTableName, isDate, startDateCol, endDateCol, columns, primaryKeys);
}

function replicateData(tableName, isDate, startDateCol, endDateCol, primaryKeys)
{
    var columns = getColumns(tableName);
    insertToAccountUsageTable(tableName, isDate, startDateCol, endDateCol, columns, primaryKeys);
    return true;
}

function replicateDataWithSource(tableName, sourceTableName, isDate, startDateCol, endDateCol, primaryKeys)
{
    var columns = getColumns(tableName);
    insertToTable(tableName, sourceTableName, isDate, startDateCol, endDateCol, columns, primaryKeys);
    return true;
}

insertToReplicationLog("started", task + "started", task);

if (sourceDb.toUpperCase() === "SNOWFLAKE" && sourceSchema.toUpperCase() === "ACCOUNT_USAGE") {
    replicateDataWithSource(
        "AUTO_REFRESH_REGISTRATION_HISTORY",
        "TABLE(SNOWFLAKE.INFORMATION_SCHEMA.AUTO_REFRESH_REGISTRATION_HISTORY())",
        true, "START_TIME", "END_TIME", null
    );
    replicateDataWithSource(
        "CORTEX_SEARCH_REFRESH_HISTORY",
        `TABLE(SNOWFLAKE.INFORMATION_SCHEMA.CORTEX_SEARCH_REFRESH_HISTORY(
            DATA_TIMESTAMP_START => DATEADD('day', -${lookBackDays}, CURRENT_TIMESTAMP())
        ))`,
        true, "REFRESH_START_TIME", "REFRESH_END_TIME", "REFRESH_START_TIME, DATA_TIMESTAMP, NAME"
    );
} else {
    replicateDataWithSource(
        "AUTO_REFRESH_REGISTRATION_HISTORY",
        `${sourceDb}.${sourceSchema}.AUTO_REFRESH_REGISTRATION_HISTORY`,
        true, "START_TIME", "END_TIME", null
    );
    replicateDataWithSource(
        "CORTEX_SEARCH_REFRESH_HISTORY",
        `${sourceDb}.${sourceSchema}.CORTEX_SEARCH_REFRESH_HISTORY`,
        true, "REFRESH_START_TIME", "REFRESH_END_TIME", "NAME, INDEX_PREPROCESSING_QUERY_ID, INDEXING_QUERY_ID, REFRESH_START_TIME"
    );
}

//For account usage tables, having start_time column and end_time column
replicateData("WAREHOUSE_METERING_HISTORY", true, "START_TIME", "END_TIME", "WAREHOUSE_ID, START_TIME");
replicateData("WAREHOUSE_LOAD_HISTORY", true, "START_TIME", "END_TIME", "WAREHOUSE_ID, START_TIME");
replicateData("METERING_HISTORY", true, "START_TIME", "END_TIME", "START_TIME, SERVICE_TYPE");
replicateData("DATABASE_REPLICATION_USAGE_HISTORY", true, "START_TIME", "END_TIME", "DATABASE_ID, START_TIME");
replicateData("REPLICATION_GROUP_USAGE_HISTORY", true, "START_TIME", "END_TIME", "REPLICATION_GROUP_ID, START_TIME");
replicateData("SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY", true, "START_TIME", "END_TIME", "START_TIME,TABLE_ID");
replicateData("SEARCH_OPTIMIZATION_HISTORY", true, "START_TIME", "END_TIME", "TABLE_ID, START_TIME");
replicateData("DATA_TRANSFER_HISTORY", true, "START_TIME", "END_TIME", "START_TIME,SOURCE_CLOUD,SOURCE_REGION,TARGET_CLOUD,TARGET_REGION");
replicateData("AUTOMATIC_CLUSTERING_HISTORY", true, "START_TIME", "END_TIME", "TABLE_ID, START_TIME, INSTANCE_ID");
replicateData("CORTEX_ANALYST_USAGE_HISTORY",true,"START_TIME","END_TIME","START_TIME, USERNAME");
replicateData("CORTEX_FINE_TUNING_USAGE_HISTORY",true,"START_TIME","END_TIME","START_TIME, MODEL_NAME");
replicateData("CORTEX_PROVISIONED_THROUGHPUT_USAGE_HISTORY",true,"INTERVAL_START_TIME","INTERVAL_END_TIME","PROVISIONED_THROUGHPUT_ID, MODEL_NAME, CLOUD_SERVICE_PROVIDER, INTERVAL_START_TIME");
replicateData("CORTEX_REST_API_USAGE_HISTORY",true,"START_TIME","END_TIME","START_TIME, REQUEST_ID");
replicateData("CORTEX_SEARCH_SERVING_USAGE_HISTORY",true,"START_TIME","END_TIME","START_TIME, SERVICE_ID");
replicateData("SNOWPARK_CONTAINER_SERVICES_HISTORY",true,"START_TIME","END_TIME","START_TIME, COMPUTE_POOL_NAME, APPLICATION_ID");

// For account usage tables with only one date column, use that for incremental load
replicateData("WAREHOUSE_EVENTS_HISTORY", true, "TIMESTAMP", null, "WAREHOUSE_ID, TIMESTAMP");
replicateData("METERING_DAILY_HISTORY", true, "USAGE_DATE", null, "SERVICE_TYPE, USAGE_DATE");
replicateData("DATABASE_STORAGE_USAGE_HISTORY", true, "USAGE_DATE", null, "DATABASE_ID, USAGE_DATE");
replicateData("STAGE_STORAGE_USAGE_HISTORY", true, "USAGE_DATE", null, "USAGE_DATE");
replicateData("STORAGE_USAGE", true, "USAGE_DATE", null, "USAGE_DATE");
replicateData("CORTEX_AISQL_USAGE_HISTORY",true,"USAGE_TIME",null,"QUERY_ID, FUNCTION_NAME, MODEL_NAME, USAGE_TIME");
replicateData("CORTEX_SEARCH_DAILY_USAGE_HISTORY",true,"USAGE_DATE",null,"SERVICE_ID, CONSUMPTION_TYPE, USAGE_DATE");

// For account usage tables without date column, do full load every time
replicateData("TABLES", false, null, null, null);
replicateData("TABLE_STORAGE_METRICS", false, null, null, null);
replicateData("COLUMNS", false, null, null, null);
replicateData("TAGS", false, null, null, null);
replicateData("TAG_REFERENCES", false, null, null, null);
replicateData("ROLES", false, null, null, null);
replicateData("GRANTS_TO_ROLES", false, null, null, null);
replicateData("GRANTS_TO_USERS", false, null, null, null);
replicateData("USERS", false, null, null, null);

// To enable below account usage tables when required
//replicateData("TABLE_DML_HISTORY",true,"START_TIME","END_TIME","TABLE_ID, START_TIME, END_TIME");
//replicateData("TABLE_PRUNING_HISTORY",true,"START_TIME","END_TIME","TABLE_ID, START_TIME, END_TIME");
//replicateData("TASK_HISTORY",true,"QUERY_START_TIME",null,"INSTANCE_ID", "QUERY_START_TIME");
//replicateData("STAGES", false, null, null, null);
//replicateData("PROCEDURES", false, null, null, null);

if (error.length > 0) {
    return error;
}
insertToReplicationLog("completed", task + "completed", task);
return returnVal;
$$;

--PROCEDURE FOR REPLICATE HISTORY QUERY
CREATE OR REPLACE PROCEDURE REPLICATE_HISTORY_QUERY(
  DB_NAME STRING,
  SCHEMA_NAME STRING,
  LOOK_BACK_DAYS STRING,
  SOURCE_DB STRING DEFAULT 'SNOWFLAKE',
  SOURCE_SCHEMA STRING DEFAULT 'ACCOUNT_USAGE'
)
    returns VARCHAR(25200)
    LANGUAGE javascript
    EXECUTE AS CALLER
AS
$$
var taskDetails = "history_query_task ---> Getting history query data ";
var task = "history_query_task";
var sourceDb = (SOURCE_DB && String(SOURCE_DB).trim().length > 0) ? String(SOURCE_DB).trim() : "SNOWFLAKE";
var sourceSchema = (SOURCE_SCHEMA && String(SOURCE_SCHEMA).trim().length > 0) ? String(SOURCE_SCHEMA).trim() : "ACCOUNT_USAGE";

function logError(err, taskName)
{
    snowflake.createStatement({
        sqlText: `INSERT INTO REPLICATION_LOG VALUES (TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()), 'FAILED', ?, ?, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`,
        binds: [String(err), String(taskName)]
    }).execute();
}

function insertToReplicationLog(status, message, taskName)
{
    snowflake.createStatement({
        sqlText: `INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), ?, ?, ?, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`,
        binds: [String(status), String(message), String(taskName)]
    }).execute();
}

var SCHEMA_NAME = SCHEMA_NAME;
var DB_NAME = DB_NAME;
var lookBackDays = parseInt(LOOK_BACK_DAYS);
var error = "";
var returnVal = "SUCCESS";

function getColumns(tableName)
{
    let columns = "";
    const columnQuery = `
        SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) AS ALL_COLUMNS
        FROM ${DB_NAME}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '${tableName}'
          AND TABLE_SCHEMA = '${SCHEMA_NAME}'
    `;
    const stmt = snowflake.createStatement({ sqlText: columnQuery });
    try {
        const res = stmt.execute();
        res.next();
        columns = res.getColumnValue(1);
    } catch (err) {
        logError(err, taskDetails);
        error += "Failed: " + err;
    }
    return columns;
}

// Create a join condition based on PK columns for incremental load to avoid duplicates in history tables. If PK columns are not provided,return null.
function getPkJoinCondition(pkCsv, leftAlias, rightAlias)
{
    let cols = (pkCsv || "").split(",").map(s => s.trim()).filter(s => s.length > 0);
    if (cols.length === 0) return null;
    return cols.map(c => `${leftAlias}."${c}" = ${rightAlias}."${c}"`).join(" AND ");
}

function buildSelectLists(targetColumnsCsv, mode) {
    if (!targetColumnsCsv) return null;

    return targetColumnsCsv
        .split(",")
        .map(c => c.trim())
        .filter(c => c.length > 0)
        .map(c => {
            if (c.toUpperCase() === "STATUS_DATE") {
                return `CURRENT_DATE() AS "STATUS_DATE"`;
            }
            if (mode === "LOWER_SOURCE") {
                return `"${c.toLowerCase()}" AS "${c}"`;
            }
            return `"${c}"`;
        })
        .join(",");
}

// For tables with date column, pull data within look back window and do incremental insert based on PK to avoid duplicates.
// For tables without date column, do full load every time.
// NOTE: `columns` param is the raw target column CSV from getColumns (unquoted), not pre-quoted anymore.
function insertToTable(destnTableName, sourceTableName, isDate, startDateCol, endDateCol, columns, primaryKeys)
{
    try {
        let insertQuery = "";
        const insertTable = `${DB_NAME}.${SCHEMA_NAME}.${destnTableName}`;

        // Build aligned select list
        const selectList = buildSelectLists(columns,"AS_IS");
        if (!selectList) {
            throw `No columns found in target table: ${insertTable}`;
        }

        if (isDate) {
            const joinCond = getPkJoinCondition(primaryKeys, "t", "w");

            const windowStart = `DATEADD('day', -${lookBackDays}, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`;
            const windowEnd = `TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP())`;

            let winPred = "";
            if (endDateCol && endDateCol.length > 0) {
                winPred = `(
                    ("${startDateCol}" >= ${windowStart})
                    OR ("${endDateCol}" IS NOT NULL AND "${endDateCol}" >= ${windowStart})
                ) AND "${startDateCol}" <= ${windowEnd}`;
            } else {
                winPred = `("${startDateCol}" >= ${windowStart} AND "${startDateCol}" <= ${windowEnd})`;
            }

            if (!joinCond) {
                insertQuery = `
                    INSERT INTO ${insertTable}
                    SELECT ${selectList}
                    FROM ${sourceTableName}
                    WHERE ${winPred}
                `;
            } else {
                insertQuery = `
                    INSERT INTO ${insertTable}
                    SELECT ${selectList}
                    FROM (
                        SELECT ${selectList}
                        FROM ${sourceTableName}
                        WHERE ${winPred}
                    ) w
                    WHERE NOT EXISTS (
                        SELECT 1 FROM ${insertTable} t WHERE ${joinCond}
                    )
                `;
            }
        } else {
            insertQuery = `
                INSERT INTO ${insertTable}
                SELECT ${selectList}
                FROM ${sourceTableName}
            `;
        }

        snowflake.createStatement({ sqlText: insertQuery }).execute();
    } catch (err) {
        logError(err, taskDetails);
        error += "Failed: " + err;
    }
}

function insertToAccountUsageTable(tableName, isDate, startDateCol, endDateCol, columns, primaryKeys)
{
    let sourceTableName = `${sourceDb}.${sourceSchema}.${tableName}`;
    return insertToTable(tableName, sourceTableName, isDate, startDateCol, endDateCol, columns, primaryKeys);
}

function replicateData(tableName, isDate, startDateCol, endDateCol, primaryKeys)
{
    //truncateTable(tableName);
    var columns = getColumns(tableName);
    insertToAccountUsageTable(tableName, isDate, startDateCol, endDateCol, columns, primaryKeys);
    return true;
}

function replicateDataWithSource(tableName, sourceTableName, isDate, startDateCol, endDateCol, primaryKeys)
{
    //truncateTable(tableName);
    var columns = getColumns(tableName);
    insertToTable(tableName, sourceTableName, isDate, startDateCol, endDateCol, columns, primaryKeys);
    return true;
}

insertToReplicationLog("started", task + "started", task);

replicateData("SESSIONS", true, "CREATED_ON", null, "SESSION_ID");
replicateData("ACCESS_HISTORY", true, "QUERY_START_TIME", null, "QUERY_ID");
replicateData("QUERY_HISTORY", true, "START_TIME", "END_TIME", "QUERY_ID");
replicateData("QUERY_INSIGHTS", true, "START_TIME", "END_TIME", "START_TIME, QUERY_ID");

if (error.length > 0) {
    return error;
}

insertToReplicationLog("completed", task + "completed", task);
return returnVal;
$$;

-- PROCEDURE FOR REPLICATE REALTIME QUERY
CREATE OR REPLACE PROCEDURE REPLICATE_REALTIME_QUERY(
    DB_NAME STRING,
    SCHEMA_NAME STRING,
    LOOK_BACK_HOURS STRING,
    SOURCE_DB STRING DEFAULT NULL,
    SOURCE_SCHEMA STRING DEFAULT 'INFORMATION_SCHEMA'
)
    RETURNS VARCHAR(25200)
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
AS
$$
var taskDetails = "realtime_query_task started ---> Getting realtime data ";
var task = "realtime_query_task";

var SCHEMA_NAME = SCHEMA_NAME;
var DB_NAME = DB_NAME;
var lookBackHours = parseInt(LOOK_BACK_HOURS);

var sourceDb = (SOURCE_DB && String(SOURCE_DB).trim().length > 0) ? String(SOURCE_DB).trim() : null;
// If SOURCE_SCHEMA is NULL/empty -> use IS.QUERY_HISTORY() snapshot path
var sourceSchema = (SOURCE_SCHEMA && String(SOURCE_SCHEMA).trim().length > 0) ? String(SOURCE_SCHEMA).trim() : null;

var error = "";
var returnVal = "SUCCESS";

function logError(err, taskName) {
    snowflake.createStatement({
        sqlText: `INSERT INTO REPLICATION_LOG VALUES (TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()), 'FAILED', ?, ?, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`,
        binds: [String(err), String(taskName)]
    }).execute();
}

function insertToReplicationLog(status, message, taskName) {
    snowflake.createStatement({
        sqlText: `INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), ?, ?, ?, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`,
        binds: [String(status), String(message), String(taskName)]
    }).execute();
}

function getColumns(tableName) {
    let columns = "";
    const columnQuery = `
        SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) AS ALL_COLUMNS
        FROM ${DB_NAME}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '${tableName}'
          AND TABLE_SCHEMA = '${SCHEMA_NAME}'
    `;
    const stmt = snowflake.createStatement({ sqlText: columnQuery });
    try {
        const res = stmt.execute();
        res.next();
        columns = res.getColumnValue(1);
    } catch (err) {
        logError(err, taskDetails);
        error += "Failed: " + err;
    }
    return columns;
}

function getPkJoinCondition(pkCsv, leftAlias, rightAlias) {
    let cols = (pkCsv || "").split(",").map(s => s.trim()).filter(s => s.length > 0);
    if (cols.length === 0) return null;
    return cols.map(c => `${leftAlias}."${c}" = ${rightAlias}."${c}"`).join(" AND ");
}

function buildSelectLists(targetColumnsCsv, mode) {
    if (!targetColumnsCsv) return null;

    return targetColumnsCsv
        .split(",")
        .map(c => c.trim())
        .filter(c => c.length > 0)
        .map(c => {
            if (c.toUpperCase() === "STATUS_DATE") return `CURRENT_DATE() AS "STATUS_DATE"`;
            if (mode === "LOWER_SOURCE") return `"${c.toLowerCase()}" AS "${c}"`;
            return `"${c}"`;
        })
        .join(",");
}

function exec(sqlText) { snowflake.createStatement({ sqlText }).execute(); }

insertToReplicationLog("started", "realtime_query_task started", task);

try {
    const targetTable = `${DB_NAME}.${SCHEMA_NAME}.IS_QUERY_HISTORY`;
    const columnsCsv = getColumns("IS_QUERY_HISTORY");
    const selectList = buildSelectLists(columnsCsv, "AS_IS");
    if (!selectList) throw `No columns found in target table: ${targetTable}`;

    if (sourceSchema === "INFORMATION_SCHEMA") {
        const insertQuery = `
            INSERT INTO ${targetTable}
            SELECT ${selectList}
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                DATEADD('hours', -${lookBackHours}, CURRENT_TIMESTAMP()),
                NULL,
                10000
            ))
        `;
        exec(insertQuery);
    } else {
        if (!sourceDb) throw "SOURCE_DB must be provided when SOURCE_SCHEMA is provided (incremental mode).";

        const sourceTable = `${sourceDb}.${sourceSchema}.IS_QUERY_HISTORY`;

        const windowStart = `DATEADD('hour', -${lookBackHours}, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`;
        const windowEnd   = `TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP())`;

        // Window predicate using BOTH START_TIME and END_TIME:
        // - include rows where START_TIME in window
        // - OR END_TIME present and in window (covers long-running queries that started earlier)
        // - also guard that START_TIME is not after now
        const winPred = `(
            ("START_TIME" >= ${windowStart} AND "START_TIME" <= ${windowEnd})
            OR ("END_TIME" IS NOT NULL AND "END_TIME" >= ${windowStart} AND "END_TIME" <= ${windowEnd})
        )`;

        // PK-based incremental insert (avoid duplicates)
        const joinCond = getPkJoinCondition("QUERY_ID", "t", "w");

        const insertQuery = `
            INSERT INTO ${targetTable}
            SELECT ${selectList}
            FROM (
                SELECT ${selectList}
                FROM ${sourceTable}
                WHERE ${winPred}
            ) w
            WHERE NOT EXISTS (
                SELECT 1 FROM ${targetTable} t WHERE ${joinCond}
            )
        `;
        exec(insertQuery);
    }

} catch (err) {
    logError(err, taskDetails);
    error += "Failed: " + err;
}

if (error.length > 0) {
 return error;
}

insertToReplicationLog("completed", "realtime_query_task completed", task);
return returnVal;
$$;

--PROCEDURE FOR REPLICATE REALTIME QUERY BY WAREHOUSE
CREATE OR REPLACE PROCEDURE REPLICATE_REALTIME_QUERY_BY_WAREHOUSE(DB_NAME STRING, SCHEMA_NAME STRING, LOOK_BACK_HOURS String)
  RETURNS VARCHAR(25200)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$
var taskDetails = "realtime_query_task ---> REPLICATE_REALTIME_QUERY_BY_WAREHOUSE Table Creation";
var task = "realtime_query_task";

insertToReplicationLog("started", "realtime_query_task started", task);
var returnVal = "SUCCESS";
var error = "";
var lookBackHours = parseInt(LOOK_BACK_HOURS);

function logError(err, taskName)
{
    snowflake.createStatement({
        sqlText: `INSERT INTO REPLICATION_LOG VALUES (TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()), 'FAILED', ?, ?, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`,
        binds: [String(err), String(taskName)]
    }).execute();
}

function insertToReplicationLog(status, message, taskName)
{
    snowflake.createStatement({
        sqlText: `INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), ?, ?, ?, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`,
        binds: [String(status), String(message), String(taskName)]
    }).execute();
}

function getColumns(tableName)
{
    var columns = "";
    var columnQuery =
        `SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) as ALL_COLUMNS
        FROM ${DB_NAME}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '${tableName}' AND TABLE_SCHEMA ='${SCHEMA_NAME}'`;
    var stmt = snowflake.createStatement({ sqlText: columnQuery });
    try {
        var res = stmt.execute();
        res.next();
        columns = res.getColumnValue(1);
    } catch (err) {
        logError(err, taskDetails);
        error += "Failed: " + err;
    }
    return columns;
}

function buildSelectLists(targetColumnsCsv, mode) {
    if (!targetColumnsCsv) return null;

    return targetColumnsCsv
        .split(",")
        .map(c => c.trim())
        .filter(c => c.length > 0)
        .map(c => {
            if (c.toUpperCase() === "STATUS_DATE") {
                return `CURRENT_DATE() AS "STATUS_DATE"`;
            }
            if (mode === "LOWER_SOURCE") {
                return `"${c.toLowerCase()}" AS "${c}"`;
            }
            return `"${c}"`;
        })
        .join(",");
}


try {
    // 1. run show warehouses
    var showWarehouse = `SHOW WAREHOUSES`;
    var showWarehouseStmt = snowflake.createStatement({
        sqlText: showWarehouse
    });
    var resultSet = showWarehouseStmt.execute();
    var count = 0;
    while (resultSet.next()) {
        // 2. Delete IS_QUERY_HISTORY table by warehouse name
        var whName = resultSet.getColumnValue(1);
        var deleteRealtimeQueryByWh =
            `DELETE FROM ${DB_NAME}.${SCHEMA_NAME}.IS_QUERY_HISTORY WHERE WAREHOUSE_NAME = '${whName}'`;

        var deleteRealtimeQueryByWhStmt = snowflake.createStatement({
            sqlText: deleteRealtimeQueryByWh
        });

        deleteRealtimeQueryByWhStmt.execute();

        // 3. Insert to IS_QUERY_HISTORY table by warehouse name
        var columns = getColumns("IS_QUERY_HISTORY");
        var selectList = buildSelectLists(columns,"AS_IS");
        var insertRealtimeQuery = `
            INSERT INTO ${DB_NAME}.${SCHEMA_NAME}.IS_QUERY_HISTORY
            SELECT ${selectList}
            FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY_BY_WAREHOUSE('${whName}',dateadd(hours,-${lookBackHours}, current_timestamp()),null,10000))
            ORDER BY start_time
        `;

        var insertRealtimeQueryStmt = snowflake.createStatement({
            sqlText: insertRealtimeQuery
        });

        insertRealtimeQueryStmt.execute();
        count++;
    }
} catch (err) {
    logError(err, taskDetails);
    error += "Failed: " + err;
}

if (error.length > 0) {
    return error;
}

insertToReplicationLog("completed", "realtime_query_task completed", task);

return returnVal;
$$;

-- PROCEDURE FOR REPLICATE QUERY PROFILE
CREATE OR REPLACE PROCEDURE CREATE_QUERY_PROFILE(
    DB_NAME STRING,
    SCHEMA_NAME STRING,
    CREDIT STRING,
    DAYS STRING,
    SOURCE_DB STRING DEFAULT NULL,
    SOURCE_SCHEMA STRING DEFAULT 'INFORMATION_SCHEMA'
)
RETURNS VARCHAR(25200)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var taskDetails = "create_query_profile ---> Getting Query Profile data and inserting into Query_profile table";
var task = "profile_task";

var SCHEMA_NAME = SCHEMA_NAME;
var DB_NAME = DB_NAME;
var cost = parseFloat(CREDIT);
var lookBackDays = parseInt(DAYS);

var sourceDb = (SOURCE_DB && String(SOURCE_DB).trim().length > 0) ? String(SOURCE_DB).trim() : null;
var sourceSchema = (SOURCE_SCHEMA && String(SOURCE_SCHEMA).trim().length > 0) ? String(SOURCE_SCHEMA).trim() : null;

const queries = [];

function logError(err, taskName)
{
    snowflake.createStatement({
        sqlText: `INSERT INTO REPLICATION_LOG VALUES (TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()), 'FAILED', ?, ?, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`,
        binds: [String(err), String(taskName)]
    }).execute();
}
function insertToReplicationLog(status, message, taskName)
{
    snowflake.createStatement({
        sqlText: `INSERT INTO REPLICATION_LOG VALUES (to_timestamp_tz(current_timestamp), ?, ?, ?, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`,
        binds: [String(status), String(message), String(taskName)]
    }).execute();
}

function getColumns(tableName)
{
    var columns = "";
    var columnQuery =
        `SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) as ALL_COLUMNS
        FROM ${DB_NAME}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '${tableName}' AND TABLE_SCHEMA ='${SCHEMA_NAME}'`;
    var stmt = snowflake.createStatement({ sqlText: columnQuery });
    try {
        var res = stmt.execute();
        res.next();
        columns = res.getColumnValue(1);
    } catch (err) {
        logError(err, taskDetails);
        error += "Failed: " + err;
    }
    return columns;
}

function buildSelectLists(targetColumnsCsv, mode) {
    if (!targetColumnsCsv) return null;

    return targetColumnsCsv
        .split(",")
        .map(c => c.trim())
        .filter(c => c.length > 0)
        .map(c => {
            if (c.toUpperCase() === "STATUS_DATE") {
                return `CURRENT_DATE() AS "STATUS_DATE"`;
            }
            if (mode === "LOWER_SOURCE") {
                return `"${c.toLowerCase()}" AS "${c}"`;
            }
            return `"${c}"`;
        })
        .join(",");
}

queries[0] = `
    CREATE TRANSIENT TABLE IF NOT EXISTS ${DB_NAME}.${SCHEMA_NAME}.QUERY_PROFILE (
      QUERY_ID VARCHAR(16777216),
      STEP_ID NUMBER(38, 0),
      OPERATOR_ID NUMBER(38, 0),
      PARENT_OPERATORS ARRAY,
      OPERATOR_TYPE VARCHAR(16777216),
      OPERATOR_STATISTICS VARIANT,
      EXECUTION_TIME_BREAKDOWN VARIANT,
      OPERATOR_ATTRIBUTES VARIANT,
      STATUS_DATE DATE
    )
    `;

queries[1] = `ALTER TABLE ${DB_NAME}.${SCHEMA_NAME}.QUERY_PROFILE ADD COLUMN IF NOT EXISTS status_date DATE`;

if (sourceDb && sourceSchema) {
    for (let i = 0; i < queries.length; i++) {
            try {
                var stmt = snowflake.createStatement({ sqlText: queries[i] });
                var res = stmt.execute();
            } catch (err) {
                logError(err, taskDetails);
                error += "Failed: " + err;
            }
        }
    const srcTable = `${sourceDb}.${sourceSchema}.QUERY_PROFILE`;
    const tgtTable = `${DB_NAME}.${SCHEMA_NAME}.QUERY_PROFILE`;

    // Build select list aligned to TARGET columns, inject CURRENT_DATE() for STATUS_DATE
    var columns = getColumns("QUERY_PROFILE");
    var selectExprs = buildSelectLists(columns, "AS_IS");

    // CDC NOT EXISTS on QUERY_ID, window on STATUS_DATE
    var insertSql = `
      INSERT INTO ${tgtTable}
      SELECT ${selectExprs}
      FROM ${srcTable} w
      WHERE w.STATUS_DATE >= DATEADD(day, -${lookBackDays}, CURRENT_DATE)
        AND NOT EXISTS (
          SELECT 1 FROM ${tgtTable} t WHERE t."QUERY_ID" = w."QUERY_ID"
        )
    `;
    snowflake.createStatement({ sqlText: insertSql }).execute();
    insertToReplicationLog("completed", "create_query_profile task completed", taskDetails);
    return "SUCCESS";
} else {
    queries[2] = `
    CREATE OR REPLACE TEMPORARY TABLE ${DB_NAME}.${SCHEMA_NAME}.QUERY_HISTORY_TEMP AS
    SELECT
      query_id,
      unit * execution_time * query_load_percent / 100 / (3600 * 1000) AS cost
    FROM (
      SELECT
        query_id,
        query_load_percent,
        CASE
          WHEN WAREHOUSE_SIZE = 'X-Small'  THEN 1
          WHEN WAREHOUSE_SIZE = 'Small'    THEN 2
          WHEN WAREHOUSE_SIZE = 'Medium'   THEN 4
          WHEN WAREHOUSE_SIZE = 'Large'    THEN 8
          WHEN WAREHOUSE_SIZE = 'X-Large'  THEN 16
          WHEN WAREHOUSE_SIZE = '2X-Large' THEN 32
          WHEN WAREHOUSE_SIZE = '3X-Large' THEN 64
          WHEN WAREHOUSE_SIZE = '4X-Large' THEN 128
          WHEN WAREHOUSE_SIZE = '5X-Large' THEN 256
          WHEN WAREHOUSE_SIZE = '6X-Large' THEN 512
          ELSE 1
        END AS unit,
        execution_time
      FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
      WHERE START_TIME > DATEADD(day, -${lookBackDays}, CURRENT_DATE)
      ORDER BY start_time
    )
    WHERE cost IS NOT NULL
      AND cost > ${cost}
    `;

    queries[3] = `
    SELECT COUNT(1)
    FROM ${DB_NAME}.${SCHEMA_NAME}.QUERY_HISTORY_TEMP
    `;

    var returnVal = "SUCCESS";
    var error = "";
    var total_query_count = 0;
    var failed_query_count = 0;

    for (let i = 0; i < queries.length; i++) {
        try {
            var stmt = snowflake.createStatement({ sqlText: queries[i] });
            var res = stmt.execute();
            if (i == 3) {
                res.next();
                total_query_count = res.getColumnValue(1);
                var message = "Total records = " + total_query_count;
                insertToReplicationLog("started", message, task);
            }
        } catch (err) {
            logError(err, taskDetails);
            error += "Failed: " + err;
        }
    }


    var columns = getColumns("QUERY_PROFILE");

    if (error.length > 0) {
        return error;
    }


    var actualQueryId = `
        SELECT tmp.query_id
        FROM ${DB_NAME}.${SCHEMA_NAME}.query_history_temp tmp
        WHERE NOT EXISTS
        (SELECT query_id FROM ${DB_NAME}.${SCHEMA_NAME}.QUERY_PROFILE qp WHERE qp.query_id = tmp.query_id);
    `;

    var selectExprs = buildSelectLists(columns, "AS_IS");

    var profileInsert = `INSERT INTO ${DB_NAME}.${SCHEMA_NAME}.QUERY_PROFILE select ${selectExprs} from table(get_query_operator_stats(?))`;
    var stmt = snowflake.createStatement({ sqlText: actualQueryId });

    var query_count = 0;
    try {
        var result_set1 = stmt.execute();
        while (result_set1.next()) {
            var queryId = result_set1.getColumnValue(1);
            try{
                var profileInsertStmt = snowflake.createStatement({ sqlText: profileInsert, binds: [queryId] });
                profileInsertStmt.execute();
                query_count++;
            } catch(err) {
                logError(err, taskDetails);
                error += "Failed: " + err;
                failed_query_count++;
            }
            if (query_count % 100 == 0) {
                var message = "Total records = " + total_query_count + ", completed = " + query_count + ", failed = " + failed_query_count;
                insertToReplicationLog("running", message, task);
            }
        }
    } catch (err) {
        logError(err, taskDetails);
        error += "Failed: " + err;
    }
    var message = "Total records = " + total_query_count + ", completed = " + query_count + ", failed = " + failed_query_count;
    insertToReplicationLog("completed", message, task);
}
return returnVal;
$$;


-- PROCEDURE FOR REPLICATE WAREHOUSE INFO
CREATE OR REPLACE PROCEDURE WAREHOUSE_PROC(
    DB_NAME STRING,
    SCHEMA_NAME STRING,
    SOURCE_DB STRING DEFAULT NULL,
    SOURCE_SCHEMA STRING DEFAULT NULL,
    LOOK_BACK_DAYS STRING DEFAULT '2'
)
  RETURNS VARCHAR(252)
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$
var taskDetails = "warehouse_proc ---> Warehouses and Warehouse_Parameter Table Creation";
var task = "warehouse_task";

var DB_NAME = DB_NAME;
var SCHEMA_NAME = SCHEMA_NAME;

var sourceDb = (SOURCE_DB && String(SOURCE_DB).trim().length > 0) ? String(SOURCE_DB).trim() : null;
var sourceSchema = (SOURCE_SCHEMA && String(SOURCE_SCHEMA).trim().length > 0) ? String(SOURCE_SCHEMA).trim() : null;

var lookBackDays = parseInt(LOOK_BACK_DAYS);
if (isNaN(lookBackDays) || lookBackDays <= 0) lookBackDays = 2;

var error = "";

function logError(err, taskName) {
    snowflake.createStatement({
        sqlText: `INSERT INTO REPLICATION_LOG VALUES (TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()), 'FAILED', ?, ?, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`,
        binds: [String(err), String(taskName)]
    }).execute();
}

function insertToReplicationLog(status, message, taskName) {
    snowflake.createStatement({
        sqlText: `INSERT INTO REPLICATION_LOG VALUES (TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()), ?, ?, ?, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`,
        binds: [String(status), String(message), String(taskName)]
    }).execute();
}

function exec(sqlText) { snowflake.createStatement({ sqlText }).execute(); }

function getColumns(tableName) {
    var rs = snowflake.createStatement({
        sqlText: `
          SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position)
          FROM ${DB_NAME}.INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_NAME='${tableName}' AND TABLE_SCHEMA='${SCHEMA_NAME}'
        `
    }).execute();
    rs.next();
    return rs.getColumnValue(1);
}

function buildSelectLists(targetColumnsCsv, mode) {
    if (!targetColumnsCsv) return null;

    return targetColumnsCsv
        .split(",")
        .map(c => c.trim())
        .filter(c => c.length > 0)
        .map(c => {
            if (c.toUpperCase() === "STATUS_DATE") return `CURRENT_DATE() AS "STATUS_DATE"`;
            if (mode === "LOWER_SOURCE") return `"${c.toLowerCase()}" AS "${c}"`;
            return `"${c}"`;
        })
        .join(",");
}

insertToReplicationLog("started", "warehouse_task started", task);
var returnVal = "SUCCESS";

try {
    if (sourceDb && sourceSchema) {
        const srcWh = `${sourceDb}.${sourceSchema}.WAREHOUSES`;
        const srcWp = `${sourceDb}.${sourceSchema}.WAREHOUSE_PARAMETERS`;
        const tgtWh = `${DB_NAME}.${SCHEMA_NAME}.WAREHOUSES`;
        const tgtWp = `${DB_NAME}.${SCHEMA_NAME}.WAREHOUSE_PARAMETERS`;

        // Ensure target tables exist + status_date column (cheap DDL; runs once due to IF NOT EXISTS)
        exec(`CREATE TRANSIENT TABLE IF NOT EXISTS ${tgtWh} LIKE ${srcWh}`);
        exec(`ALTER TABLE ${tgtWh} ADD COLUMN IF NOT EXISTS STATUS_DATE DATE`);

        exec(`CREATE TRANSIENT TABLE IF NOT EXISTS ${tgtWp} LIKE ${srcWp}`);
        exec(`ALTER TABLE ${tgtWp} ADD COLUMN IF NOT EXISTS STATUS_DATE DATE`);

        // Build aligned select lists using TARGET columns (inject CURRENT_DATE for STATUS_DATE)
        var tgtWhCols = getColumns("WAREHOUSES");
        var whSelect = buildSelectLists(tgtWhCols, "AS_IS");

        var tgtWpCols = getColumns("WAREHOUSE_PARAMETERS");
        var wpSelect = buildSelectLists(tgtWpCols, "AS_IS");

        // window predicate (STATUS_DATE) on source
        var winPred = `w.STATUS_DATE >= DATEADD(day, -${lookBackDays}, CURRENT_DATE)`;

        exec(`
          INSERT INTO ${tgtWh}
          SELECT ${whSelect}
          FROM ${srcWh} w
          WHERE ${winPred}
            AND NOT EXISTS (
              SELECT 1 FROM ${tgtWh} t
              WHERE t."name" = w."name"
            )
        `);

        exec(`
          INSERT INTO ${tgtWp}
          SELECT ${wpSelect}
          FROM ${srcWp} w
          WHERE ${winPred}
            AND NOT EXISTS (
              SELECT 1 FROM ${tgtWp} t
              WHERE t."WAREHOUSE" = w."WAREHOUSE"
                AND t."KEY" = w."KEY"
            )
        `);

        insertToReplicationLog("completed", "warehouse_task completed (CDC from source)", task);
        return "SUCCESS";
    }

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
    var create_table_sql = `CREATE TRANSIENT TABLE IF NOT EXISTS "${DB_NAME}"."${SCHEMA_NAME}".WAREHOUSES (
        ${column_defs.join(",\n    ")}
    );`;
    var create_stmt = snowflake.createStatement({sqlText: create_table_sql});
    create_stmt.execute();
    var alterTable = `ALTER TABLE ${DB_NAME}.${SCHEMA_NAME}.WAREHOUSES ADD COLUMN IF NOT EXISTS status_date DATE `;
    snowflake.createStatement({sqlText: alterTable}).execute();

    // 5. TRUNCATE TABLE
    var truncate_sql = `TRUNCATE TABLE IF EXISTS "${DB_NAME}"."${SCHEMA_NAME}".WAREHOUSES;`;
    var truncate_stmt = snowflake.createStatement({sqlText: truncate_sql});
    truncate_stmt.execute();

    // 5.1 common cols between SHOW WAREHOUSES and WAREHOUSES table
    var existing_columns = getColumns("WAREHOUSES");
    existing_columns = existing_columns.split(',').map(item => `"${item.trim()}"`);
    column_names = column_names.filter(col => existing_columns.includes(col));

    // 6. INSERT INTO
    var insert_sql_wh = `INSERT INTO "${DB_NAME}"."${SCHEMA_NAME}".WAREHOUSES (${column_names.join(", ")}, status_date)
                      SELECT ${column_names.join(", ")}, current_date() as status_date FROM TABLE(RESULT_SCAN('${query_id}'));`;
    var insert_stmt_wh = snowflake.createStatement({sqlText: insert_sql_wh});
    insert_stmt_wh.execute();

} catch (err) {
    logError(err, taskDetails);
    error += "Failed: " + err;
}

try {
    //1. create warehouse parameters table
    var createWP = 'CREATE TRANSIENT TABLE IF NOT EXISTS ' + DB_NAME + '.' + SCHEMA_NAME + '.WAREHOUSE_PARAMETERS (WAREHOUSE VARCHAR(1000), KEY VARCHAR(1000), VALUE VARCHAR(1000), DEFAULT VARCHAR(1000),LEVEL VARCHAR(1000), DESCRIPTION VARCHAR(10000),TYPE VARCHAR(100), status_date DATE);';
    snowflake.createStatement({ sqlText: createWP }).execute();

    var alterTable2 = `ALTER TABLE ${DB_NAME}.${SCHEMA_NAME}.WAREHOUSE_PARAMETERS ADD COLUMN IF NOT EXISTS status_date DATE `;
    snowflake.createStatement({sqlText: alterTable2}).execute();

    //2. truncate warehouse parameter tables
    var truncateWarehouseParameter = `TRUNCATE TABLE IF EXISTS "${DB_NAME}"."${SCHEMA_NAME}".WAREHOUSE_PARAMETERS;`;
    snowflake.createStatement({ sqlText: truncateWarehouseParameter }).execute();

} catch (err) {
    logError(err, taskDetails);
    error += "Failed: " + err;
}

try {
    //get columns
    var columns = getColumns("WAREHOUSE_PARAMETERS");
    var selectList = buildSelectLists(columns.split(',').slice(1).join(','), "LOWER_SOURCE");

    //3.Get warehouse details
    var wn = `SELECT * FROM "${DB_NAME}"."${SCHEMA_NAME}".WAREHOUSES;`;
    var wnStmt = snowflake.createStatement({ sqlText: wn });
    var resultSet1 = wnStmt.execute();

    while (resultSet1.next()) {
        var whName = resultSet1.getColumnValue(1);

        //4. show warehouse parameters
        var showWP = `SHOW PARAMETERS IN WAREHOUSE ${whName};`;
        var showWPStmt = snowflake.createStatement({ sqlText: showWP });
        showWPStmt.execute();

        //5. insert into WAREHOUSE_PARAMETERS table
        var wpInsert = `
          INSERT INTO "${DB_NAME}"."${SCHEMA_NAME}".WAREHOUSE_PARAMETERS
          SELECT '${whName}', ${selectList}
          FROM TABLE (result_scan(last_query_id()))
        `;
        snowflake.createStatement({ sqlText: wpInsert }).execute();
    }

} catch (err) {
    logError(err, taskDetails);
    error += "Failed: " + err;
}

if (error.length > 0) return error;

insertToReplicationLog("completed", "warehouse_task completed", task);
return returnVal;
$$;


-- PROCEDURE FOR SHARED DB METADATA
CREATE OR REPLACE PROCEDURE CREATE_SHARED_DB_METADATA(
  DATABASE_NAME STRING,
  SCHEMA_NAME   STRING,
  SOURCE_DB     STRING DEFAULT NULL,
  SOURCE_SCHEMA STRING DEFAULT NULL
)
  RETURNS VARIANT
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$
var taskDetails = "create_shared_db ---> Shared tables, Shared columns and Shared views Table Creation";
var task = "create_shared_db";
var error = "";

function logError(err, taskName) {
  snowflake.createStatement({
    sqlText: `INSERT INTO REPLICATION_LOG VALUES (TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()), 'FAILED', ?, ?, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`,
    binds: [String(err), String(taskName)]
  }).execute();
}

function insertToReplicationLog(status, message, taskName) {
  snowflake.createStatement({
    sqlText: `INSERT INTO REPLICATION_LOG VALUES (TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()), ?, ?, ?, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`,
    binds: [String(status), String(message), String(taskName)]
  }).execute();
}

function exec(sqlText) { snowflake.createStatement({ sqlText }).execute(); }

function getColumns(sourceDB, sourceSchema, sourceTable) {
  var columns = "";
  var columnQuery = `
    SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position) as ALL_COLUMNS
    FROM ${sourceDB}.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME='${sourceTable}' AND TABLE_SCHEMA='${sourceSchema}'
  `;
  var stmt = snowflake.createStatement({ sqlText: columnQuery });
  try {
    var res = stmt.execute();
    res.next();
    columns = res.getColumnValue(1);
  } catch (err) {
    logError(err, taskDetails);
    error += "Failed: " + err;
  }
  return columns;
}

function buildSelectLists(targetColumnsCsv, mode) {
  if (!targetColumnsCsv) return null;

  let cols = targetColumnsCsv.split(",").map(c => c.trim()).filter(c => c.length > 0);
  let hasStatusDate = cols.some(c => c.toUpperCase() === "STATUS_DATE");

  let selectExprs = cols.map(c => {
    if (c.toUpperCase() === "STATUS_DATE") return `CURRENT_DATE() AS "STATUS_DATE"`;
    if (mode === "LOWER_SOURCE") return `"${c.toLowerCase()}" AS "${c}"`;
    return `"${c}"`;
  });

  if (!hasStatusDate) selectExprs.push(`CURRENT_DATE() AS "STATUS_DATE"`);

  return selectExprs.join(",");
}

function executeSQL(sqlText, column_name) {
  let result = [];
  const res = snowflake.createStatement({ sqlText }).execute();
  while (res.next()) {
    const v = res.getColumnValue(column_name);
    if (v && v !== "SNOWFLAKE") result.push(v);
  }
  return result;
}

insertToReplicationLog("started", "replicate shared_db started", task);

try {
  // 1) Create SHARED_* tables if they do not exist + ensure STATUS_DATE exists
  [
    `CREATE TRANSIENT TABLE IF NOT EXISTS ${DATABASE_NAME}.${SCHEMA_NAME}.SHARED_TABLES  AS SELECT * FROM INFORMATION_SCHEMA.TABLES  WHERE 1=0;`,
    `CREATE TRANSIENT TABLE IF NOT EXISTS ${DATABASE_NAME}.${SCHEMA_NAME}.SHARED_VIEWS   AS SELECT * FROM INFORMATION_SCHEMA.VIEWS   WHERE 1=0;`,
    `CREATE TRANSIENT TABLE IF NOT EXISTS ${DATABASE_NAME}.${SCHEMA_NAME}.SHARED_COLUMNS AS SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE 1=0;`,
    `ALTER TABLE ${DATABASE_NAME}.${SCHEMA_NAME}.SHARED_TABLES  ADD COLUMN IF NOT EXISTS STATUS_DATE DATE;`,
    `ALTER TABLE ${DATABASE_NAME}.${SCHEMA_NAME}.SHARED_VIEWS   ADD COLUMN IF NOT EXISTS STATUS_DATE DATE;`,
    `ALTER TABLE ${DATABASE_NAME}.${SCHEMA_NAME}.SHARED_COLUMNS ADD COLUMN IF NOT EXISTS STATUS_DATE DATE;`
  ].forEach(sql => exec(sql));

  // 2) Dedupe predicates
  let notExistsCondition = {
    SHARED_TABLES:  "m.TABLE_NAME = t.TABLE_NAME AND m.TABLE_SCHEMA = t.TABLE_SCHEMA AND m.TABLE_CATALOG = t.TABLE_CATALOG",
    SHARED_VIEWS:   "m.TABLE_NAME = t.TABLE_NAME AND m.TABLE_SCHEMA = t.TABLE_SCHEMA AND m.TABLE_CATALOG = t.TABLE_CATALOG",
    SHARED_COLUMNS: "m.TABLE_NAME = t.TABLE_NAME AND m.TABLE_SCHEMA = t.TABLE_SCHEMA AND m.TABLE_CATALOG = t.TABLE_CATALOG AND m.COLUMN_NAME = t.COLUMN_NAME"
  };

  // 3) Cache TARGET column lists (note: uses DATABASE_NAME.INFORMATION_SCHEMA.COLUMNS, not INFORMATION_SCHEMA)
  let colsCsv = {
    SHARED_TABLES:  getColumns(DATABASE_NAME, SCHEMA_NAME, "SHARED_TABLES"),
    SHARED_VIEWS:   getColumns(DATABASE_NAME, SCHEMA_NAME, "SHARED_VIEWS"),
    SHARED_COLUMNS: getColumns(DATABASE_NAME, SCHEMA_NAME, "SHARED_COLUMNS")
  };

  function insertMeta(sharedTable, sourceDB, sourceSchema, sourceTable, mode) {
    let selectList = buildSelectLists(colsCsv[sharedTable], mode);
    let insertQuery = `
      INSERT INTO ${DATABASE_NAME}.${SCHEMA_NAME}.${sharedTable}
      SELECT ${selectList}
      FROM ${sourceDB}.${sourceSchema}.${sourceTable} t
      WHERE t.TABLE_SCHEMA != 'INFORMATION_SCHEMA'
        AND NOT EXISTS (
          SELECT 1 FROM ${DATABASE_NAME}.${SCHEMA_NAME}.${sharedTable} m
          WHERE ${notExistsCondition[sharedTable]}
        )
    `;
    exec(insertQuery);
  }

  // 4) If SOURCE_DB is provided, read from SOURCE_DB.SOURCE_SCHEMA.SHARED_*
  var srcDb = (SOURCE_DB && String(SOURCE_DB).trim().length > 0) ? String(SOURCE_DB).trim() : null;
  var srcSchema = (SOURCE_SCHEMA && String(SOURCE_SCHEMA).trim().length > 0) ? String(SOURCE_SCHEMA).trim() : "INFORMATION_SCHEMA";
  let mode = "AS_IS";
  if (srcDb && srcSchema) {
    // Source is your backup schema (tables already have STATUS_DATE potentially).
    // We still inject CURRENT_DATE() for STATUS_DATE via buildSelectLists.
    insertMeta("SHARED_TABLES",  srcDb, srcSchema, "SHARED_TABLES",  mode);
    insertMeta("SHARED_VIEWS",   srcDb, srcSchema, "SHARED_VIEWS",   mode);
    insertMeta("SHARED_COLUMNS", srcDb, srcSchema, "SHARED_COLUMNS", mode);
    var message = `Total shared db = ${total_shared_db}, failed db = ${total_failed_db}`;
    insertToReplicationLog("completed", message, task);
  } else {
    // Default behavior: read from shares' INFORMATION_SCHEMA
    const dbShares = executeSQL("SHOW SHARES;", "database_name");
    var total_shared_db = 0;
    var total_failed_db = 0;
    for (const shareDB of dbShares) {
        try {
          insertMeta("SHARED_TABLES",  shareDB, "INFORMATION_SCHEMA", "TABLES",  mode);
          insertMeta("SHARED_VIEWS",   shareDB, "INFORMATION_SCHEMA", "VIEWS",   mode);
          insertMeta("SHARED_COLUMNS", shareDB, "INFORMATION_SCHEMA", "COLUMNS", mode);
          total_shared_db++ ;
       } catch(err) {
           logError(err, taskDetails);
           total_failed_db++;
       }
    }
    insertToReplicationLog("completed", "create_shared_db_metadata_task completed: total shared db:"+total_shared_db+" total failed db:"+total_failed_db, task);
    return "success";
  }
} catch (err) {
  try {
    logError(err, taskDetails);
  } catch (e) {}
  return { status: "failure", message: String(err) };
}
$$;


CREATE OR REPLACE PROCEDURE REPL_CLEANUP_RETENTION(
    DB_NAME STRING,
    SCHEMA_NAME STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
function exec(sqlText) {
    snowflake.createStatement({ sqlText }).execute();
}

/**
 * Deletes rows older than `truncateDurationHours` with respect to current_timestamp
 * Priority:
 *  if endCol exists -> use endCol
 *  else if startCol exists -> use startCol, or "STATUS_DATE" (snapshot tables)
 *  else -> skip
 */
function truncateTable(tableName, startCol, endCol, truncateDurationHours) {
    try {
        let cutoffExpr =
            `DATEADD('hour', -${Number(truncateDurationHours)}, TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()))`;
        let timePredicate = null;
        if (endCol && endCol.length > 0) {
            timePredicate = `TRY_TO_TIMESTAMP_TZ("${endCol}") < ${cutoffExpr}`;
        } else if (startCol && startCol.length > 0) {
            timePredicate = `TRY_TO_TIMESTAMP_TZ("${startCol}") < ${cutoffExpr}`;
        } else {
            exec(`
                INSERT INTO REPLICATION_LOG
                VALUES (TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()),'WARN','Skipping ${tableName}: no time column provided','REPL_TRUNCATE_BY_TIME')
            `);
            return;
        }

        let sqlText = `
            DELETE FROM ${DB_NAME}.${SCHEMA_NAME}.${tableName}
            WHERE ${timePredicate}
        `;

        exec(sqlText);
    } catch (err) {
        snowflake.createStatement({
            sqlText: `INSERT INTO REPLICATION_LOG
                VALUES (TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()),'FAILED',?,'REPL_TRUNCATE_BY_TIME')`,
            binds: [String(err)]
        }).execute();
    }
}

truncateTable("AUTO_REFRESH_REGISTRATION_HISTORY", "START_TIME", "END_TIME", 48);
truncateTable("WAREHOUSE_METERING_HISTORY", "START_TIME", "END_TIME", 48);
truncateTable("WAREHOUSE_LOAD_HISTORY", "START_TIME", "END_TIME", 48);
truncateTable("METERING_HISTORY", "START_TIME", "END_TIME", 48);
truncateTable("DATABASE_REPLICATION_USAGE_HISTORY", "START_TIME", "END_TIME", 48);
truncateTable("REPLICATION_GROUP_USAGE_HISTORY", "START_TIME", "END_TIME", 48);
truncateTable("SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY", "START_TIME", "END_TIME", 48);
truncateTable("SEARCH_OPTIMIZATION_HISTORY", "START_TIME", "END_TIME", 48);
truncateTable("DATA_TRANSFER_HISTORY", "START_TIME", "END_TIME", 48);
truncateTable("AUTOMATIC_CLUSTERING_HISTORY", "START_TIME", "END_TIME", 48);
truncateTable("TABLE_DML_HISTORY", "START_TIME", "END_TIME", 48);
truncateTable("TABLE_PRUNING_HISTORY", "START_TIME", "END_TIME", 48);

truncateTable("WAREHOUSE_EVENTS_HISTORY", "TIMESTAMP", null, 48);
truncateTable("METERING_DAILY_HISTORY", "USAGE_DATE", null, 48);
truncateTable("DATABASE_STORAGE_USAGE_HISTORY", "USAGE_DATE", null, 48);
truncateTable("STAGE_STORAGE_USAGE_HISTORY", "USAGE_DATE", null, 48);
truncateTable("STORAGE_USAGE", "USAGE_DATE", null, 48);
truncateTable("TASK_HISTORY", "QUERY_START_TIME", null, 48);

truncateTable("TABLES", "STATUS_DATE", null, 48);
truncateTable("TABLE_STORAGE_METRICS", "STATUS_DATE", null, 48);
truncateTable("COLUMNS", "STATUS_DATE", null, 48);
truncateTable("TAGS", "STATUS_DATE", null, 48);
truncateTable("TAG_REFERENCES", "STATUS_DATE", null, 48);
truncateTable("STAGES", "STATUS_DATE", null, 48);
truncateTable("PROCEDURES", "STATUS_DATE", null, 48);

truncateTable("SESSIONS", "CREATED_ON", null, 48);
truncateTable("ACCESS_HISTORY", "QUERY_START_TIME", null, 48);
truncateTable("QUERY_HISTORY", "START_TIME", "END_TIME", 48);
truncateTable("QUERY_INSIGHTS", "START_TIME", "END_TIME", 48);

truncateTable("GRANTS_TO_USERS", "STATUS_DATE", null, 48);
truncateTable("GRANTS_TO_ROLES", "STATUS_DATE", null, 48);
truncateTable("ROLES", "STATUS_DATE", null, 48);

truncateTable("WAREHOUSES", "STATUS_DATE", null, 48);
truncateTable("WAREHOUSE_PARAMETERS", "STATUS_DATE", null, 48);

truncateTable("SHARED_TABLES", "STATUS_DATE", null, 48);
truncateTable("SHARED_VIEWS", "STATUS_DATE", null, 48);
truncateTable("SHARED_COLUMNS", "STATUS_DATE", null, 48);


truncateTable("CORTEX_AISQL_USAGE_HISTORY","USAGE_DATE",null,48);
truncateTable("CORTEX_ANALYST_USAGE_HISTORY","USAGE_DATE",null,48);
truncateTable("CORTEX_FINE_TUNING_USAGE_HISTORY","USAGE_DATE",null,48);
truncateTable("CORTEX_PROVISIONED_THROUGHPUT_USAGE_HISTORY","USAGE_DATE",null,48);
truncateTable("CORTEX_REST_API_USAGE_HISTORY","USAGE_DATE",null,48);
truncateTable("CORTEX_SEARCH_DAILY_USAGE_HISTORY","USAGE_DATE",null,48);
truncateTable("CORTEX_SEARCH_SERVING_USAGE_HISTORY","USAGE_DATE",null,48);
truncateTable("SNOWPARK_CONTAINER_SERVICES_HISTORY","USAGE_DATE",null,48);
truncateTable("USERS","USAGE_DATE",null,48);

return 'OK';
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
    CREATE SHARE IF NOT EXISTS S_SECURE_SHARE;
    GRANT USAGE ON DATABASE UNRAVEL_SHARE to share S_SECURE_SHARE;
    GRANT USAGE ON SCHEMA UNRAVEL_SHARE.SCHEMA_4827_T to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.WAREHOUSE_METERING_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.WAREHOUSE_EVENTS_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.WAREHOUSE_LOAD_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.COLUMNS to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.TAGS to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.TAG_REFERENCES to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.TABLES to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.TABLE_STORAGE_METRICS to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.METERING_DAILY_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.METERING_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.DATABASE_REPLICATION_USAGE_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.REPLICATION_GROUP_USAGE_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.QUERY_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.SESSIONS to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.ACCESS_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.IS_QUERY_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.WAREHOUSE_PARAMETERS to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.WAREHOUSES to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.QUERY_PROFILE to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.DATABASE_STORAGE_USAGE_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.STAGE_STORAGE_USAGE_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.SEARCH_OPTIMIZATION_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.DATA_TRANSFER_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.AUTOMATIC_CLUSTERING_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.AUTO_REFRESH_REGISTRATION_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.REPLICATION_LOG to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.SHARED_TABLES to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.SHARED_VIEWS to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.SHARED_COLUMNS to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.QUERY_INSIGHTS to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.GRANTS_TO_USERS to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.GRANTS_TO_ROLES to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.CORTEX_AISQL_USAGE_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.CORTEX_ANALYST_USAGE_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.CORTEX_FINE_TUNING_USAGE_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.CORTEX_PROVISIONED_THROUGHPUT_USAGE_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.CORTEX_REST_API_USAGE_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.CORTEX_SEARCH_DAILY_USAGE_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.CORTEX_SEARCH_SERVING_USAGE_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.SNOWPARK_CONTAINER_SERVICES_HISTORY to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.USERS to share S_SECURE_SHARE;
    GRANT SELECT ON TABLE UNRAVEL_SHARE.SCHEMA_4827_T.ROLES to share S_SECURE_SHARE;

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

CALL CREATE_ACCOUNT_USAGE_TABLES('SNOWFLAKE', 'ACCOUNT_USAGE', 'UNRAVEL_SHARE', 'SCHEMA_4827_T');
CALL CREATE_DERIVED_TABLES(TARGET_DB => 'UNRAVEL_SHARE', TARGET_SCHEMA => 'SCHEMA_4827_T');

CALL REPLICATE_ACCOUNT_USAGE('UNRAVEL_SHARE', 'SCHEMA_4827_T', 180);
CALL REPLICATE_HISTORY_QUERY('UNRAVEL_SHARE', 'SCHEMA_4827_T', 180);
CALL WAREHOUSE_PROC('UNRAVEL_SHARE', 'SCHEMA_4827_T');
CALL CREATE_QUERY_PROFILE('UNRAVEL_SHARE', 'SCHEMA_4827_T', '1', '14');
CALL CREATE_SHARED_DB_METADATA('UNRAVEL_SHARE', 'SCHEMA_4827_T');
-- CALL REPLICATE_REALTIME_QUERY('UNRAVEL_SHARE', 'SCHEMA_4827_T', '48');

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
CALL REPLICATE_ACCOUNT_USAGE('UNRAVEL_SHARE', 'SCHEMA_4827_T', 3);
CALL REPLICATE_HISTORY_QUERY('UNRAVEL_SHARE', 'SCHEMA_4827_T', 3);
CALL WAREHOUSE_PROC('UNRAVEL_SHARE', 'SCHEMA_4827_T');
CALL CREATE_QUERY_PROFILE('UNRAVEL_SHARE', 'SCHEMA_4827_T', '0.1', '3');
CALL CREATE_SHARED_DB_METADATA('UNRAVEL_SHARE', 'SCHEMA_4827_T');
-- CALL REPLICATE_REALTIME_QUERY('UNRAVEL_SHARE', 'SCHEMA_4827_T', '48');
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
CALL REPLICATE_ACCOUNT_USAGE('UNRAVEL_SHARE', 'SCHEMA_4827_T', 2);

CREATE OR REPLACE TASK replicate_history_query
    WAREHOUSE = UNRAVELDATA
    SCHEDULE = '60 MINUTE'
AS
CALL REPLICATE_HISTORY_QUERY('UNRAVEL_SHARE', 'SCHEMA_4827_T', 2);

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
    CALL warehouse_proc('UNRAVEL_SHARE', 'SCHEMA_4827_T');
END;

CREATE OR REPLACE TASK shared_db_metadata_task
    WAREHOUSE = UNRAVELDATA
    SCHEDULE = '720 MINUTE'
AS
BEGIN
    CALL create_shared_db_metadata('UNRAVEL_SHARE', 'SCHEMA_4827_T');
END;

CREATE OR REPLACE TASK REPL_TASK_CLEANUP_RETENTION
  WAREHOUSE = UNRAVELDATA
  SCHEDULE = 'USING CRON 0 */6 * * * UTC'
AS
  CALL REPL_CLEANUP_RETENTION('UNRAVEL_SHARE', 'SCHEMA_4827_T');


/**
  (Resume all TASKS)
**/
ALTER TASK replicate_metadata RESUME;
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

