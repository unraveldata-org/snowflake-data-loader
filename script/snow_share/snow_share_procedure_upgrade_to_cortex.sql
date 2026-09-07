/**
    Step-1a: Started (procedures creation started.)
**/

CREATE DATABASE IF NOT EXISTS UNRAVEL_SHARE;
USE UNRAVEL_SHARE;

CREATE SCHEMA IF NOT EXISTS SCHEMA_4827_T;
USE UNRAVEL_SHARE.SCHEMA_4827_T;

-- Stop future legacy task executions before replacing their procedure bindings.
ALTER TASK IF EXISTS replicate_metadata SUSPEND;
ALTER TASK IF EXISTS replicate_metadata_snapshots SUSPEND;
ALTER TASK IF EXISTS replicate_history_query SUSPEND;
ALTER TASK IF EXISTS createProfileTable SUSPEND;
ALTER TASK IF EXISTS replicate_warehouse_and_realtime_query SUSPEND;
ALTER TASK IF EXISTS shared_db_metadata_task SUSPEND;
ALTER TASK IF EXISTS REPL_TASK_CLEANUP_RETENTION SUSPEND;

-- New defaulted signatures conflict with these legacy overloads in Snowflake.
DROP PROCEDURE IF EXISTS CREATE_TABLES(STRING, STRING);
DROP PROCEDURE IF EXISTS REPLICATE_ACCOUNT_USAGE(STRING, STRING, STRING);
DROP PROCEDURE IF EXISTS REPLICATE_ACCOUNT_USAGE(STRING, STRING, STRING, STRING, STRING);
DROP PROCEDURE IF EXISTS REPLICATE_HISTORY_QUERY(STRING, STRING, STRING);
DROP PROCEDURE IF EXISTS REPLICATE_REALTIME_QUERY(STRING, STRING, STRING);
DROP PROCEDURE IF EXISTS REPLICATE_REALTIME_QUERY_BY_WAREHOUSE(STRING, STRING, STRING);
DROP PROCEDURE IF EXISTS CREATE_QUERY_PROFILE(STRING, STRING, STRING, STRING);
DROP PROCEDURE IF EXISTS WAREHOUSE_PROC(STRING, STRING);
DROP PROCEDURE IF EXISTS CREATE_SHARED_DB_METADATA(STRING, STRING);

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
        // hashDedup=true: these tables have no documented PK in ACCOUNT_USAGE, so
        // duplicate suppression is done via a stored content hash (see REPLICATE_ACCOUNT_USAGE).
        ,{tableName: "CORTEX_AGENT_USAGE_HISTORY", hashDedup: true}
        ,{tableName: "CORTEX_AI_FUNCTIONS_USAGE_HISTORY", hashDedup: true}
        ,{tableName: "CORTEX_SEARCH_BATCH_QUERY_USAGE_HISTORY", hashDedup: true}
        ,{tableName: "CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY", hashDedup: true}
        ,{tableName: "CORTEX_CODE_CLI_USAGE_HISTORY", hashDedup: true}
        ,{tableName: "CORTEX_AI_GUARDRAILS_USAGE_HISTORY", hashDedup: true}
        ,{tableName: "CORTEX_DOCUMENT_PROCESSING_USAGE_HISTORY", hashDedup: true}
        ,{tableName: "SNOWFLAKE_INTELLIGENCE_USAGE_HISTORY", hashDedup: true}
        ,{tableName: "QUERY_ATTRIBUTION_HISTORY", hashDedup: true}
        ,{tableName: "CORTEX_REST_API_RATE_LIMIT_POLICIES"}
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

            if (table.hashDedup) {
                var alterHashTable = `ALTER TABLE ${TARGET_DB}.${TARGET_SCHEMA}.${table.tableName} ADD COLUMN IF NOT EXISTS ROW_HASH NUMBER`;
                snowflake.createStatement({ sqlText: alterHashTable }).execute();
            }
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
        // No documented PK for this table; dedup via stored content hash (see REPLICATE_ACCOUNT_USAGE).
        alterTable = `ALTER TABLE ${TARGET_DB}.${TARGET_SCHEMA}.AUTO_REFRESH_REGISTRATION_HISTORY ADD COLUMN IF NOT EXISTS ROW_HASH NUMBER `;
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
            ,{tableName: "AUTO_REFRESH_REGISTRATION_HISTORY", hashDedup: true}
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

                if (table.hashDedup) {
                    var alterHashTable = `ALTER TABLE ${TARGET_DB}.${TARGET_SCHEMA}.${table.tableName} ADD COLUMN IF NOT EXISTS ROW_HASH NUMBER`;
                    snowflake.createStatement({ sqlText: alterHashTable }).execute();
                }
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
  SOURCE_SCHEMA STRING DEFAULT 'ACCOUNT_USAGE',
  TABLE_GROUP STRING DEFAULT 'ALL'
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
// TABLE_GROUP splits this procedure's ~43 tables into two independently schedulable batches,
// each its own task/CALL (and therefore its own STATEMENT_TIMEOUT_IN_SECONDS/USER_TASK_TIMEOUT_MS
// budget), so a slow/large snapshot table (e.g. COLUMNS on a big account) cannot threaten the
// timeout budget of the many bounded, LOOK_BACK_DAYS-capped history tables, or vice versa:
//   'HISTORY'   -> time-windowed tables, PK- or hash-deduped (bounded cost per run)
//   'SNAPSHOTS' -> full-refresh tables via build-in-staging + SWAP (cost scales with account size)
//   'ALL'       -> both, in one call (default; preserves the original single-call behavior
//                  for any existing caller that does not pass this new parameter)
var tableGroup = (TABLE_GROUP && String(TABLE_GROUP).trim().length > 0) ? String(TABLE_GROUP).trim().toUpperCase() : "ALL";
var runHistory = (tableGroup === "ALL" || tableGroup === "HISTORY");
var runSnapshots = (tableGroup === "ALL" || tableGroup === "SNAPSHOTS");

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

var _columnsCache = {}; // memoized per-procedure-call: avoids re-querying INFORMATION_SCHEMA.COLUMNS for the same table
function getColumns(tableName)
{
    if (_columnsCache.hasOwnProperty(tableName)) return _columnsCache[tableName];
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
    _columnsCache[tableName] = columns;
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
        // ROW_HASH is a synthetic dedup column populated explicitly by the hash-dedup
        // branch below; it never exists on the source side, so it's excluded here.
        .filter(c => c.length > 0 && c.toUpperCase() !== "ROW_HASH")
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
// For tables with a date window but no documented PK, dedup via a stored content hash (useHashDedup=true).
// For tables without date column, do a full atomic refresh (build-in-staging + zero-copy SWAP) every time.
// NOTE: `columns` param is the raw target column CSV from getColumns (unquoted), not pre-quoted anymore.
function insertToTable(destnTableName, sourceTableName, isDate, startDateCol, endDateCol, columns, primaryKeys, useHashDedup)
{
    const t0 = Date.now();
    try {
        let insertQuery = "";
        const insertTable = `${DB_NAME}.${SCHEMA_NAME}.${destnTableName}`;

        // Build aligned select list (excludes ROW_HASH, see buildSelectLists)
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

            if (joinCond) {
                // Category 1: documented PK — exactly-once insert.
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
            } else if (useHashDedup) {
                // Category 2: no documented PK — dedupe on a stored content hash instead.
                // The target-side STATUS_DATE filter keeps this cheap: only the last
                // LOOK_BACK_DAYS of already-loaded rows are compared, not the whole table,
                // which is all that can possibly overlap with the incoming window anyway.
                const rawCols = columns.split(",").map(c => c.trim())
                    .filter(c => c.length > 0 && c.toUpperCase() !== "ROW_HASH" && c.toUpperCase() !== "STATUS_DATE");
                const hashArgsW = rawCols.map(c => `w."${c}"`).join(",");
                insertQuery = `
                    INSERT INTO ${insertTable}
                    SELECT ${selectList}, HASH(${hashArgsW}) AS "ROW_HASH"
                    FROM (
                        SELECT ${selectList}
                        FROM ${sourceTableName}
                        WHERE ${winPred}
                    ) w
                    WHERE NOT EXISTS (
                        SELECT 1 FROM ${insertTable} t
                        WHERE t."ROW_HASH" = HASH(${hashArgsW})
                          AND t."STATUS_DATE" >= DATEADD('day', -${lookBackDays}, CURRENT_DATE())
                    )
                `;
            } else {
                // No PK, hash dedup not requested for this table: plain windowed insert.
                insertQuery = `
                    INSERT INTO ${insertTable}
                    SELECT ${selectList}
                    FROM ${sourceTableName}
                    WHERE ${winPred}
                `;
            }

            const rs = snowflake.createStatement({ sqlText: insertQuery }).execute();
            rs.next();
            const rowsInserted = rs.getColumnValue(1);
            insertToReplicationLog("completed", `table=${destnTableName} rows=${rowsInserted} ms=${Date.now() - t0}`, task);
        } else {
            // Category 3: full snapshot. Build the fresh copy in a staging table first, then
            // swap it in atomically (zero-copy metadata operation). If the staging build fails,
            // the real target is never touched (self-recoverable: next run just retries), and
            // there is never a truncated/empty window visible to concurrent readers.
            const stgTable = `${insertTable}_STG_${Math.floor(Math.random() * 1e9)}`;
            try {
                snowflake.createStatement({ sqlText:
                    `CREATE OR REPLACE TRANSIENT TABLE ${stgTable} AS SELECT ${selectList} FROM ${sourceTableName}`
                }).execute();
                const rs = snowflake.createStatement({ sqlText: `SELECT COUNT(*) FROM ${stgTable}` }).execute();
                rs.next();
                const rowCount = rs.getColumnValue(1);
                snowflake.createStatement({ sqlText: `ALTER TABLE ${insertTable} SWAP WITH ${stgTable}` }).execute();
                snowflake.createStatement({ sqlText: `DROP TABLE IF EXISTS ${stgTable}` }).execute();
                insertToReplicationLog("completed", `table=${destnTableName} rows=${rowCount} ms=${Date.now() - t0}`, task);
            } catch (stagingErr) {
                snowflake.createStatement({ sqlText: `DROP TABLE IF EXISTS ${stgTable}` }).execute();
                throw stagingErr;
            }
        }
    } catch (err) {
        logError(err, taskDetails);
        error += "Failed: " + err;
        insertToReplicationLog("failed", `table=${destnTableName} err=${String(err).slice(0,1000)} ms=${Date.now() - t0}`, task);
    }
}

function insertToAccountUsageTable(tableName, isDate, startDateCol, endDateCol, columns, primaryKeys, useHashDedup)
{
    let sourceTableName = `${sourceDb}.${sourceSchema}.${tableName}`;
    return insertToTable(tableName, sourceTableName, isDate, startDateCol, endDateCol, columns, primaryKeys, useHashDedup);
}

function replicateData(tableName, isDate, startDateCol, endDateCol, primaryKeys, useHashDedup)
{
    var columns = getColumns(tableName);
    insertToAccountUsageTable(tableName, isDate, startDateCol, endDateCol, columns, primaryKeys, useHashDedup);
    return true;
}

function replicateDataWithSource(tableName, sourceTableName, isDate, startDateCol, endDateCol, primaryKeys, useHashDedup)
{
    var columns = getColumns(tableName);
    insertToTable(tableName, sourceTableName, isDate, startDateCol, endDateCol, columns, primaryKeys, useHashDedup);
    return true;
}

insertToReplicationLog("started", task + " started (" + tableGroup + ")", task);

if (runHistory) {
if (sourceDb.toUpperCase() === "SNOWFLAKE" && sourceSchema.toUpperCase() === "ACCOUNT_USAGE") {
    replicateDataWithSource(
        "AUTO_REFRESH_REGISTRATION_HISTORY",
        "TABLE(SNOWFLAKE.INFORMATION_SCHEMA.AUTO_REFRESH_REGISTRATION_HISTORY())",
        true, "START_TIME", "END_TIME", null, true
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
        true, "START_TIME", "END_TIME", null, true
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
replicateData("CORTEX_AGENT_USAGE_HISTORY",true,"START_TIME",null,null,true);
replicateData("CORTEX_AI_FUNCTIONS_USAGE_HISTORY",true,"START_TIME",null,null,true);
replicateData("CORTEX_SEARCH_BATCH_QUERY_USAGE_HISTORY",true,"START_TIME",null,null,true);
replicateData("CORTEX_DOCUMENT_PROCESSING_USAGE_HISTORY",true,"START_TIME",null,null,true);
replicateData("SNOWFLAKE_INTELLIGENCE_USAGE_HISTORY",true,"START_TIME",null,null,true);
replicateData("QUERY_ATTRIBUTION_HISTORY",true,"START_TIME",null,null,true);
replicateData("SNOWPARK_CONTAINER_SERVICES_HISTORY",true,"START_TIME","END_TIME","START_TIME, COMPUTE_POOL_NAME, APPLICATION_ID");

// For account usage tables with only one date column, use that for incremental load
replicateData("WAREHOUSE_EVENTS_HISTORY", true, "TIMESTAMP", null, "WAREHOUSE_ID, TIMESTAMP");
replicateData("METERING_DAILY_HISTORY", true, "USAGE_DATE", null, "SERVICE_TYPE, USAGE_DATE");
replicateData("DATABASE_STORAGE_USAGE_HISTORY", true, "USAGE_DATE", null, "DATABASE_ID, USAGE_DATE");
replicateData("STAGE_STORAGE_USAGE_HISTORY", true, "USAGE_DATE", null, "USAGE_DATE");
replicateData("STORAGE_USAGE", true, "USAGE_DATE", null, "USAGE_DATE");
replicateData("CORTEX_AISQL_USAGE_HISTORY",true,"USAGE_TIME",null,"QUERY_ID, FUNCTION_NAME, MODEL_NAME, USAGE_TIME");
replicateData("CORTEX_SEARCH_DAILY_USAGE_HISTORY",true,"USAGE_DATE",null,"SERVICE_ID, CONSUMPTION_TYPE, USAGE_DATE");
replicateData("CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY",true,"USAGE_TIME",null,null,true);
replicateData("CORTEX_CODE_CLI_USAGE_HISTORY",true,"USAGE_TIME",null,null,true);
replicateData("CORTEX_AI_GUARDRAILS_USAGE_HISTORY",true,"USAGE_TIME",null,null,true);

// To enable below account usage tables when required
//replicateData("TABLE_DML_HISTORY",true,"START_TIME","END_TIME","TABLE_ID, START_TIME, END_TIME");
//replicateData("TABLE_PRUNING_HISTORY",true,"START_TIME","END_TIME","TABLE_ID, START_TIME, END_TIME");
//replicateData("TASK_HISTORY",true,"QUERY_START_TIME",null,"INSTANCE_ID", "QUERY_START_TIME");
//replicateData("STAGES", false, null, null, null);
//replicateData("PROCEDURES", false, null, null, null);
} // end runHistory

if (runSnapshots) {
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
replicateData("CORTEX_REST_API_RATE_LIMIT_POLICIES", false, null, null, null);
} // end runSnapshots

if (error.length > 0) {
    return error;
}
insertToReplicationLog("completed", task + " completed (" + tableGroup + ")", task);
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

var _columnsCache = {}; // memoized per-procedure-call: avoids re-querying INFORMATION_SCHEMA.COLUMNS for the same table
function getColumns(tableName)
{
    if (_columnsCache.hasOwnProperty(tableName)) return _columnsCache[tableName];
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
    _columnsCache[tableName] = columns;
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

var _columnsCache = {}; // memoized per-procedure-call: avoids re-querying INFORMATION_SCHEMA.COLUMNS for the same table
function getColumns(tableName) {
    if (_columnsCache.hasOwnProperty(tableName)) return _columnsCache[tableName];
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
    _columnsCache[tableName] = columns;
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

var _columnsCache = {}; // memoized per-procedure-call: avoids re-querying INFORMATION_SCHEMA.COLUMNS for the same table
function getColumns(tableName)
{
    if (_columnsCache.hasOwnProperty(tableName)) return _columnsCache[tableName];
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
    _columnsCache[tableName] = columns;
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
    // Column list is the same for every warehouse in this loop; fetch it once
    // outside the loop instead of once per warehouse (getColumns() is also
    // memoized now, but hoisting avoids the redundant call entirely).
    var columns = getColumns("IS_QUERY_HISTORY");
    var selectList = buildSelectLists(columns,"AS_IS");
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

var _columnsCache = {}; // memoized per-procedure-call: avoids re-querying INFORMATION_SCHEMA.COLUMNS for the same table
function getColumns(tableName)
{
    if (_columnsCache.hasOwnProperty(tableName)) return _columnsCache[tableName];
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
    _columnsCache[tableName] = columns;
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

var _columnsCache = {}; // memoized per-procedure-call: avoids re-querying INFORMATION_SCHEMA.COLUMNS for the same table
function getColumns(tableName) {
    if (_columnsCache.hasOwnProperty(tableName)) return _columnsCache[tableName];
    var rs = snowflake.createStatement({
        sqlText: `
          SELECT LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position)
          FROM ${DB_NAME}.INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_NAME='${tableName}' AND TABLE_SCHEMA='${SCHEMA_NAME}'
        `
    }).execute();
    rs.next();
    _columnsCache[tableName] = rs.getColumnValue(1);
    return _columnsCache[tableName];
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

var _columnsCache = {}; // memoized per-procedure-call: avoids re-querying INFORMATION_SCHEMA.COLUMNS for the same table
function getColumns(sourceDB, sourceSchema, sourceTable) {
  var _cacheKey = `${sourceDB}.${sourceSchema}.${sourceTable}`;
  if (_columnsCache.hasOwnProperty(_cacheKey)) return _columnsCache[_cacheKey];
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
  _columnsCache[_cacheKey] = columns;
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
        // REPLICATION_LOG.eventDate is already TIMESTAMP_TZ (see CREATE_DERIVED_TABLES). Snowflake's
        // TRY_TO_TIMESTAMP_TZ() does not support a TIMESTAMP_TZ -> TIMESTAMP_TZ cast (compile error),
        // so that column is compared directly instead of going through the TRY_TO_TIMESTAMP_TZ()
        // wrapper used for the other (event-time-column) tables below.
        const timeExpr = (col) => (tableName === "REPLICATION_LOG") ? `"${col}"` : `TRY_TO_TIMESTAMP_TZ("${col}")`;
        if (endCol && endCol.length > 0) {
            timePredicate = `${timeExpr(endCol)} < ${cutoffExpr}`;
        } else if (startCol && startCol.length > 0) {
            timePredicate = `${timeExpr(startCol)} < ${cutoffExpr}`;
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
                VALUES (TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP()),'FAILED',?,'REPL_TRUNCATE_BY_TIME',CURRENT_DATE())`,
            binds: [String(err)]
        }).execute();
    }
}

// One-time deployment config: the standard retention window for operational history/snapshot
// tables. Declared once here so changing it is a single edit instead of updating every one
// of the ~50 truncateTable(...) calls below.
var DEFAULT_RETENTION_HOURS = 48;
// REPLICATION_LOG is the debugging trail for this whole pipeline, not customer usage data
// subject to the same storage/cost pressure, so it gets a much longer window (180 days).
var REPLICATION_LOG_RETENTION_HOURS = 4320;

truncateTable("AUTO_REFRESH_REGISTRATION_HISTORY", "START_TIME", "END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("WAREHOUSE_METERING_HISTORY", "START_TIME", "END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("WAREHOUSE_LOAD_HISTORY", "START_TIME", "END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("METERING_HISTORY", "START_TIME", "END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("DATABASE_REPLICATION_USAGE_HISTORY", "START_TIME", "END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("REPLICATION_GROUP_USAGE_HISTORY", "START_TIME", "END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("SNOWPIPE_STREAMING_FILE_MIGRATION_HISTORY", "START_TIME", "END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("SEARCH_OPTIMIZATION_HISTORY", "START_TIME", "END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("DATA_TRANSFER_HISTORY", "START_TIME", "END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("AUTOMATIC_CLUSTERING_HISTORY", "START_TIME", "END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("WAREHOUSE_EVENTS_HISTORY", "TIMESTAMP", null, DEFAULT_RETENTION_HOURS);
truncateTable("METERING_DAILY_HISTORY", "USAGE_DATE", null, DEFAULT_RETENTION_HOURS);
truncateTable("DATABASE_STORAGE_USAGE_HISTORY", "USAGE_DATE", null, DEFAULT_RETENTION_HOURS);
truncateTable("STAGE_STORAGE_USAGE_HISTORY", "USAGE_DATE", null, DEFAULT_RETENTION_HOURS);
truncateTable("STORAGE_USAGE", "USAGE_DATE", null, DEFAULT_RETENTION_HOURS);

truncateTable("TABLES", "STATUS_DATE", null, DEFAULT_RETENTION_HOURS);
truncateTable("TABLE_STORAGE_METRICS", "STATUS_DATE", null, DEFAULT_RETENTION_HOURS);
truncateTable("COLUMNS", "STATUS_DATE", null, DEFAULT_RETENTION_HOURS);
truncateTable("TAGS", "STATUS_DATE", null, DEFAULT_RETENTION_HOURS);
truncateTable("TAG_REFERENCES", "STATUS_DATE", null, DEFAULT_RETENTION_HOURS);

truncateTable("SESSIONS", "CREATED_ON", null, DEFAULT_RETENTION_HOURS);
truncateTable("ACCESS_HISTORY", "QUERY_START_TIME", null, DEFAULT_RETENTION_HOURS);
truncateTable("QUERY_HISTORY", "START_TIME", "END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("QUERY_INSIGHTS", "START_TIME", "END_TIME", DEFAULT_RETENTION_HOURS);

truncateTable("GRANTS_TO_USERS", "STATUS_DATE", null, DEFAULT_RETENTION_HOURS);
truncateTable("GRANTS_TO_ROLES", "STATUS_DATE", null, DEFAULT_RETENTION_HOURS);
truncateTable("ROLES", "STATUS_DATE", null, DEFAULT_RETENTION_HOURS);

truncateTable("WAREHOUSES", "STATUS_DATE", null, DEFAULT_RETENTION_HOURS);
truncateTable("WAREHOUSE_PARAMETERS", "STATUS_DATE", null, DEFAULT_RETENTION_HOURS);

truncateTable("SHARED_TABLES", "STATUS_DATE", null, DEFAULT_RETENTION_HOURS);
truncateTable("SHARED_VIEWS", "STATUS_DATE", null, DEFAULT_RETENTION_HOURS);
truncateTable("SHARED_COLUMNS", "STATUS_DATE", null, DEFAULT_RETENTION_HOURS);


truncateTable("CORTEX_AISQL_USAGE_HISTORY","USAGE_TIME",null, DEFAULT_RETENTION_HOURS);
truncateTable("CORTEX_ANALYST_USAGE_HISTORY","START_TIME","END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("CORTEX_FINE_TUNING_USAGE_HISTORY","START_TIME","END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("CORTEX_PROVISIONED_THROUGHPUT_USAGE_HISTORY","INTERVAL_START_TIME","INTERVAL_END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("CORTEX_REST_API_USAGE_HISTORY","START_TIME","END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("CORTEX_SEARCH_DAILY_USAGE_HISTORY","USAGE_DATE",null, DEFAULT_RETENTION_HOURS);
truncateTable("CORTEX_SEARCH_SERVING_USAGE_HISTORY","START_TIME","END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("CORTEX_SEARCH_REFRESH_HISTORY","REFRESH_START_TIME","REFRESH_END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("CORTEX_AGENT_USAGE_HISTORY","START_TIME",null, DEFAULT_RETENTION_HOURS);
truncateTable("CORTEX_AI_FUNCTIONS_USAGE_HISTORY","START_TIME",null, DEFAULT_RETENTION_HOURS);
truncateTable("CORTEX_SEARCH_BATCH_QUERY_USAGE_HISTORY","START_TIME",null, DEFAULT_RETENTION_HOURS);
truncateTable("CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY","USAGE_TIME",null, DEFAULT_RETENTION_HOURS);
truncateTable("CORTEX_CODE_CLI_USAGE_HISTORY","USAGE_TIME",null, DEFAULT_RETENTION_HOURS);
truncateTable("CORTEX_AI_GUARDRAILS_USAGE_HISTORY","USAGE_TIME",null, DEFAULT_RETENTION_HOURS);
truncateTable("CORTEX_DOCUMENT_PROCESSING_USAGE_HISTORY","START_TIME",null, DEFAULT_RETENTION_HOURS);
truncateTable("SNOWFLAKE_INTELLIGENCE_USAGE_HISTORY","START_TIME",null, DEFAULT_RETENTION_HOURS);
truncateTable("QUERY_ATTRIBUTION_HISTORY","START_TIME",null, DEFAULT_RETENTION_HOURS);
truncateTable("CORTEX_REST_API_RATE_LIMIT_POLICIES","STATUS_DATE",null, DEFAULT_RETENTION_HOURS);
truncateTable("SNOWPARK_CONTAINER_SERVICES_HISTORY","START_TIME","END_TIME", DEFAULT_RETENTION_HOURS);
truncateTable("USERS","STATUS_DATE",null, DEFAULT_RETENTION_HOURS);

// REPLICATION_LOG is kept much longer than the operational history/snapshot tables above
// (180 days instead of 48 hours) since it's the primary debugging trail for this whole
// pipeline, not customer-facing usage data subject to the same storage/cost pressure.
truncateTable("REPLICATION_LOG", "EVENTDATE", null, REPLICATION_LOG_RETENTION_HOURS);

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
    -- One-time deployment config, declared once so changing it is a single edit here
    -- instead of updating every one of the ~55 GRANT statements this replaces. Kept as
    -- local constants (not procedure parameters) so the call signature stays exactly
    -- SHARE_TO_ACCOUNT(VARCHAR) -- adding parameters here would create a second,
    -- ambiguous overload for any existing deployment that already has this procedure
    -- (confirmed live: Snowflake rejects such a CREATE with "ambiguous PROCEDURE
    -- overloading" when a differently-defaulted signature of the same name exists).
    DB_NAME VARCHAR DEFAULT 'UNRAVEL_SHARE';
    SCHEMA_NAME VARCHAR DEFAULT 'SCHEMA_4827_T';
    use_statement VARCHAR;
    res RESULTSET;
BEGIN
    use_statement := 'CREATE SHARE IF NOT EXISTS S_SECURE_SHARE';
    EXECUTE IMMEDIATE :use_statement;

    use_statement := 'GRANT USAGE ON DATABASE ' || DB_NAME || ' TO SHARE S_SECURE_SHARE';
    EXECUTE IMMEDIATE :use_statement;

    use_statement := 'GRANT USAGE ON SCHEMA ' || DB_NAME || '.' || SCHEMA_NAME || ' TO SHARE S_SECURE_SHARE';
    EXECUTE IMMEDIATE :use_statement;

    -- Replaces the ~55 individually-enumerated GRANT SELECT statements this procedure used
    -- to have with one dynamic grant covering every table that exists at call time. Snowflake
    -- does not support "FUTURE TABLES ... TO SHARE" (confirmed live: restricted), so a newly
    -- added table still requires re-calling this procedure once -- but with no script edit,
    -- since nothing here is hardcoded to a specific table list anymore.
    use_statement := 'GRANT SELECT ON ALL TABLES IN SCHEMA ' || DB_NAME || '.' || SCHEMA_NAME || ' TO SHARE S_SECURE_SHARE';
    EXECUTE IMMEDIATE :use_statement;

    use_statement := 'ALTER SHARE S_SECURE_SHARE add accounts = ' || ACCOUNTID::VARIANT::VARCHAR;
    res := (EXECUTE IMMEDIATE :use_statement);
    RETURN 'SUCCESS';
END;

/**
    Legacy migration steps. This script is for customers upgrading directly from
    snow_share_procedure.sql; it intentionally does not run the fresh-install
    180-day backfill because the legacy tables already contain that history.
**/

-- Creates missing tables and adds STATUS_DATE to the legacy table schemas.
CALL CREATE_ACCOUNT_USAGE_TABLES('SNOWFLAKE', 'ACCOUNT_USAGE', 'UNRAVEL_SHARE', 'SCHEMA_4827_T');
CALL CREATE_DERIVED_TABLES(TARGET_DB => 'UNRAVEL_SHARE', TARGET_SCHEMA => 'SCHEMA_4827_T');

CREATE OR REPLACE PROCEDURE BACKFILL_LEGACY_STATUS_DATE()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var tables = [
    "TABLES", "TABLE_STORAGE_METRICS", "COLUMNS", "TAGS", "TAG_REFERENCES",
    "REPLICATION_LOG", "IS_QUERY_HISTORY", "WAREHOUSES", "WAREHOUSE_PARAMETERS",
    "SHARED_TABLES", "SHARED_VIEWS", "SHARED_COLUMNS", "QUERY_PROFILE"
];
for (var i = 0; i < tables.length; i++) {
    var tableName = tables[i];
    var exists = snowflake.createStatement({
        sqlText: `SELECT COUNT(*) FROM UNRAVEL_SHARE.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'SCHEMA_4827_T' AND TABLE_NAME = '${tableName}'`
    }).execute();
    exists.next();
    if (exists.getColumnValue(1) > 0) {
        snowflake.createStatement({
            sqlText: `UPDATE UNRAVEL_SHARE.SCHEMA_4827_T.${tableName} SET STATUS_DATE = CURRENT_DATE() WHERE STATUS_DATE IS NULL`
        }).execute();
    }
}
return "SUCCESS";
$$;

CALL BACKFILL_LEGACY_STATUS_DATE();
DROP PROCEDURE BACKFILL_LEGACY_STATUS_DATE();

-- Do not invoke REPLICATE_ACCOUNT_USAGE inline: its snapshot tables and
-- INFORMATION_SCHEMA functions are not bounded by LOOK_BACK_DAYS. The rebound
-- replicate_metadata task performs the next normal replication on its schedule.

-- Repeatable grants add the new Cortex tables to the existing share.
CALL SHARE_TO_ACCOUNT('<Unravel Account Identifier>');

-- These explicit calls bind tasks to the new overloads rather than the legacy signatures.
-- replicate_metadata / replicate_metadata_snapshots run on the same schedule as independent
-- tasks (not a DAG): the two table sets are fully disjoint, so there is no reason to couple
-- their success/failure, and each gets its own STATEMENT_TIMEOUT_IN_SECONDS/USER_TASK_TIMEOUT_MS
-- budget instead of sharing one across all ~43 tables. See TABLE_GROUP on REPLICATE_ACCOUNT_USAGE.
CREATE OR REPLACE TASK replicate_metadata
    WAREHOUSE = UNRAVELDATA
    SCHEDULE = 'USING CRON 0 3,9,15,21 * * * UTC'
AS
CALL REPLICATE_ACCOUNT_USAGE('UNRAVEL_SHARE', 'SCHEMA_4827_T', 2, 'SNOWFLAKE', 'ACCOUNT_USAGE', 'HISTORY');

CREATE OR REPLACE TASK replicate_metadata_snapshots
    WAREHOUSE = UNRAVELDATA
    SCHEDULE = 'USING CRON 0 3,9,15,21 * * * UTC'
AS
CALL REPLICATE_ACCOUNT_USAGE('UNRAVEL_SHARE', 'SCHEMA_4827_T', 2, 'SNOWFLAKE', 'ACCOUNT_USAGE', 'SNAPSHOTS');

CREATE OR REPLACE TASK replicate_history_query
    WAREHOUSE = UNRAVELDATA
    SCHEDULE = '60 MINUTE'
AS
CALL REPLICATE_HISTORY_QUERY('UNRAVEL_SHARE', 'SCHEMA_4827_T', 2, 'SNOWFLAKE', 'ACCOUNT_USAGE');

CREATE OR REPLACE TASK createProfileTable
    WAREHOUSE = UNRAVELDATA
    SCHEDULE = '60 MINUTE'
AS
CALL CREATE_QUERY_PROFILE('UNRAVEL_SHARE', 'SCHEMA_4827_T', '1', '2', NULL, 'INFORMATION_SCHEMA');

CREATE OR REPLACE TASK replicate_warehouse_and_realtime_query
    WAREHOUSE = UNRAVELDATA
    SCHEDULE = '720 MINUTE'
AS
CALL WAREHOUSE_PROC('UNRAVEL_SHARE', 'SCHEMA_4827_T', NULL, NULL, '2');

CREATE OR REPLACE TASK shared_db_metadata_task
    WAREHOUSE = UNRAVELDATA
    SCHEDULE = '720 MINUTE'
AS
CALL CREATE_SHARED_DB_METADATA('UNRAVEL_SHARE', 'SCHEMA_4827_T', NULL, NULL, '300');

CREATE OR REPLACE TASK REPL_TASK_CLEANUP_RETENTION
    WAREHOUSE = UNRAVELDATA
    SCHEDULE = 'USING CRON 0 */6 * * * UTC'
AS
CALL REPL_CLEANUP_RETENTION('UNRAVEL_SHARE', 'SCHEMA_4827_T');

ALTER TASK replicate_metadata RESUME;
ALTER TASK replicate_metadata_snapshots RESUME;
ALTER TASK replicate_history_query RESUME;
ALTER TASK createProfileTable RESUME;
ALTER TASK replicate_warehouse_and_realtime_query RESUME;
ALTER TASK shared_db_metadata_task RESUME;
ALTER TASK REPL_TASK_CLEANUP_RETENTION RESUME;

SELECT TABLE_NAME
FROM UNRAVEL_SHARE.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'SCHEMA_4827_T'
  AND TABLE_NAME IN (
      'CORTEX_AGENT_USAGE_HISTORY', 'CORTEX_AI_FUNCTIONS_USAGE_HISTORY',
      'CORTEX_SEARCH_BATCH_QUERY_USAGE_HISTORY', 'CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY',
      'CORTEX_CODE_CLI_USAGE_HISTORY', 'CORTEX_AI_GUARDRAILS_USAGE_HISTORY',
      'CORTEX_DOCUMENT_PROCESSING_USAGE_HISTORY', 'SNOWFLAKE_INTELLIGENCE_USAGE_HISTORY',
      'QUERY_ATTRIBUTION_HISTORY', 'CORTEX_REST_API_RATE_LIMIT_POLICIES'
  )
ORDER BY TABLE_NAME;

SHOW TASKS IN SCHEMA UNRAVEL_SHARE.SCHEMA_4827_T;
SHOW GRANTS TO SHARE S_SECURE_SHARE;
