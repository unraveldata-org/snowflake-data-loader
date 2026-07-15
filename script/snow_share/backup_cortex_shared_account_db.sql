/**
    CDC-style backup for Cortex / Snowpark Container Services shared tables.

    This script mirrors the existing TP_CDC_METADATA / CREATE_BACKUP_DB_TABLES
    pattern and extends it to the 9 new tables added by the Cortex replication
    script:
        1. CORTEX_AISQL_USAGE_HISTORY
        2. CORTEX_ANALYST_USAGE_HISTORY
        3. CORTEX_FINE_TUNING_USAGE_HISTORY
        4. CORTEX_PROVISIONED_THROUGHPUT_USAGE_HISTORY
        5. CORTEX_REST_API_USAGE_HISTORY
        6. CORTEX_SEARCH_DAILY_USAGE_HISTORY
        7. CORTEX_SEARCH_SERVING_USAGE_HISTORY
        8. CORTEX_SEARCH_REFRESH_HISTORY
        9. SNOWPARK_CONTAINER_SERVICES_HISTORY
        10. CORTEX_AGENT_USAGE_HISTORY
        11. CORTEX_AI_FUNCTIONS_USAGE_HISTORY
        12. CORTEX_SEARCH_BATCH_QUERY_USAGE_HISTORY
        13. CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY
        14. CORTEX_CODE_CLI_USAGE_HISTORY
        15. CORTEX_AI_GUARDRAILS_USAGE_HISTORY
        16. CORTEX_DOCUMENT_PROCESSING_USAGE_HISTORY
        17. SNOWFLAKE_INTELLIGENCE_USAGE_HISTORY
        18. QUERY_ATTRIBUTION_HISTORY

    Pattern (same as parent CDC script):
       - CREATE_BACKUP_DB_CORTEX_TABLES: clones the source table schema into the
         backup schema with DATA_RETENTION_TIME_IN_DAYS = 0, then ADDs a
         STATUS_DATE DATE column used to track when each row was first captured.
       - TP_CDC_CORTEX_METADATA: for each table, computes the intersection of
         source and destination columns (excluding STATUS_DATE), and appends
         rows from the source that do NOT already exist in the target based on
         the configured key columns. New rows land with STATUS_DATE = CURRENT_DATE.

    Note on CORTEX_SEARCH_REFRESH_HISTORY:
       Because this is INSERT-only CDC, a row backed up while a refresh is still
       in state = 'EXECUTING' will keep that state forever in the backup. In
       practice the daily cadence runs long after refreshes have completed, so
       this is a rare edge case — but worth being aware of if you see stale
       EXECUTING rows in the backup.
**/


CREATE OR REPLACE PROCEDURE TP_CDC_CORTEX_METADATA(
    SOURCE_DB VARCHAR,
    SOURCE_SCHEMA VARCHAR,
    TARGET_DB VARCHAR,
    TARGET_SCHEMA VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var result = "";

    // Key columns per table — used for the WHERE NOT EXISTS check.
    // If a table's key list is empty, the CDC falls back to using ALL common
    // columns as the key (matches parent TP_CDC_METADATA behavior).
    //
    // NOTE on NULL handling: several key columns below can legitimately be NULL
    // (APPLICATION_ID for non-app compute pools; MODEL_NAME for unbilled
    // fine-tuning / non-embedding search rows; query IDs for in-progress
    // refreshes; ROLE_ID for some records). The WHERE clause uses EQUAL_NULL()
    // rather than = so that NULL-valued keys deduplicate correctly instead of
    // appending a fresh duplicate on every task run.
    var tablesToInsert = [
        { tableName: "CORTEX_AISQL_USAGE_HISTORY",                  columns: ["QUERY_ID", "MODEL_NAME", "FUNCTION_NAME", "WAREHOUSE_ID", "USAGE_TIME"] },
        { tableName: "CORTEX_ANALYST_USAGE_HISTORY",                columns: ["START_TIME", "USERNAME"] },
        { tableName: "CORTEX_FINE_TUNING_USAGE_HISTORY",            columns: ["START_TIME", "MODEL_NAME"] },
        { tableName: "CORTEX_PROVISIONED_THROUGHPUT_USAGE_HISTORY", columns: ["PROVISIONED_THROUGHPUT_ID", "INTERVAL_START_TIME", "MODEL_NAME", "CLOUD_SERVICE_PROVIDER"] },
        { tableName: "CORTEX_REST_API_USAGE_HISTORY",               columns: ["REQUEST_ID", "MODEL_NAME", "START_TIME"] },
        { tableName: "CORTEX_SEARCH_DAILY_USAGE_HISTORY",           columns: ["USAGE_DATE", "SERVICE_ID", "CONSUMPTION_TYPE", "MODEL_NAME"] },
        { tableName: "CORTEX_SEARCH_SERVING_USAGE_HISTORY",         columns: ["START_TIME", "SERVICE_ID"] },
        { tableName: "CORTEX_SEARCH_REFRESH_HISTORY",               columns: ["DATABASE_NAME", "SCHEMA_NAME", "NAME", "REFRESH_START_TIME", "INDEX_PREPROCESSING_QUERY_ID", "INDEXING_QUERY_ID"] },
        { tableName: "SNOWPARK_CONTAINER_SERVICES_HISTORY",         columns: ["START_TIME", "COMPUTE_POOL_NAME", "APPLICATION_ID"] },
        { tableName: "CORTEX_AGENT_USAGE_HISTORY",                  columns: ["START_TIME", "REQUEST_ID", "USER_ID"] },
        { tableName: "CORTEX_AI_FUNCTIONS_USAGE_HISTORY",           columns: ["START_TIME", "QUERY_ID", "WAREHOUSE_ID"] },
        { tableName: "CORTEX_SEARCH_BATCH_QUERY_USAGE_HISTORY",     columns: ["START_TIME", "SERVICE_ID", "SERVICE_NAME"] },
        { tableName: "CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY",         columns: ["USAGE_TIME", "REQUEST_ID", "USER_ID"] },
        { tableName: "CORTEX_CODE_CLI_USAGE_HISTORY",               columns: ["USAGE_TIME", "REQUEST_ID", "USER_ID"] },
        { tableName: "CORTEX_AI_GUARDRAILS_USAGE_HISTORY",          columns: ["USAGE_TIME", "GUARDRAILS_SIGNAL"] },
        { tableName: "CORTEX_DOCUMENT_PROCESSING_USAGE_HISTORY",    columns: ["START_TIME", "FUNCTION_NAME", "MODEL_NAME"] },
        { tableName: "QUERY_ATTRIBUTION_HISTORY",                   columns: ["QUERY_ID", "START_TIME"] },
        { tableName: "SNOWFLAKE_INTELLIGENCE_USAGE_HISTORY",        columns: ["START_TIME", "REQUEST_ID", "USER_ID"] },
        { tableName: "CORTEX_REST_API_RATE_LIMIT_POLICIES",         columns: ["MODEL_NAME"] }
    ];

    for (var i = 0; i < tablesToInsert.length; i++) {
        var table = tablesToInsert[i];
        var tableName = table.tableName;
        var insertSql = "";

        try {
            // --- 1. Get Source Columns ---
            // UPPER() on both sides so this works regardless of the case the
            // caller passed for SOURCE_SCHEMA / target names. Snowflake stores
            // unquoted identifiers as uppercase, but the values passed as
            // procedure arguments are compared as raw string literals — a
            // mixed-case argument like 'my_schema' will NOT match the stored
            // 'MY_SCHEMA' without this normalization.
            var srcColsStmt = snowflake.execute({
                sqlText: `SELECT COLUMN_NAME
                          FROM ${SOURCE_DB}.INFORMATION_SCHEMA.COLUMNS
                          WHERE UPPER(TABLE_SCHEMA) = UPPER('${SOURCE_SCHEMA}')
                            AND UPPER(TABLE_NAME)   = UPPER('${tableName}')
                            AND UPPER(COLUMN_NAME) != 'STATUS_DATE'`
            });
            var srcCols = [];
            while (srcColsStmt.next()) {
                srcCols.push(srcColsStmt.getColumnValue(1));
            }

            // --- 2. Get Destination Columns ---
            var destColsStmt = snowflake.execute({
                sqlText: `SELECT COLUMN_NAME
                          FROM ${TARGET_DB}.INFORMATION_SCHEMA.COLUMNS
                          WHERE UPPER(TABLE_SCHEMA) = UPPER('${TARGET_SCHEMA}')
                            AND UPPER(TABLE_NAME)   = UPPER('${tableName}')
                            AND UPPER(COLUMN_NAME) != 'STATUS_DATE'`
            });
            var destCols = [];
            while (destColsStmt.next()) {
                destCols.push(destColsStmt.getColumnValue(1));
            }

            // --- 3. Find Common Columns ---
            var commonCols = srcCols.filter(c => destCols.includes(c));
            if (commonCols.length === 0) {
                result += `No common columns found for ${tableName}\n`;
                continue;
            }

            // Quote all column names
            var quotedCols = commonCols.map(c => `"${c}"`);
            var selectCols = quotedCols.join(", ");
            var insertCols = quotedCols.join(", ");

            // Handle key columns
            var keyCols = table.columns.length > 0 ? table.columns : commonCols;
            // Filter keys to only those that exist in both
            var validKeyCols = keyCols.filter(c => commonCols.includes(c));
            if (validKeyCols.length === 0) validKeyCols = commonCols;
            var quotedKeyCols = validKeyCols.map(c => `"${c}"`);

            // EQUAL_NULL(a,b) is like a = b except that (NULL, NULL) => TRUE.
            // Necessary because several key columns for the cortex tables are
            // legitimately NULL for certain row types (non-app compute pools,
            // in-progress search refreshes, etc.). See note above the table
            // list for details.
            var conditions = quotedKeyCols.map(c => `EQUAL_NULL(T.${c}, S.${c})`).join(" AND ");

            // --- 4. Build INSERT Query ---
            insertSql = `
                INSERT INTO ${TARGET_DB}.${TARGET_SCHEMA}.${tableName} (${insertCols}, STATUS_DATE)
                SELECT ${selectCols}, CURRENT_DATE
                FROM ${SOURCE_DB}.${SOURCE_SCHEMA}.${tableName} T
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM ${TARGET_DB}.${TARGET_SCHEMA}.${tableName} S
                    WHERE ${conditions}
                )
            `;

            snowflake.execute({ sqlText: insertSql });

        } catch (err) {
            result += `Error processing ${tableName}: ${err.message}\n`;
        }
    }

    return result.length > 0 ? result : "SUCCESS";

} catch (err) {
    return "Error: " + err.message;
}
$$;


CREATE OR REPLACE PROCEDURE CREATE_BACKUP_DB_CORTEX_TABLES(SOURCE_DB STRING, SOURCE_SCHEMA STRING, TARGET_DB STRING, TARGET_SCHEMA STRING)
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

    // Same table list as TP_CDC_CORTEX_METADATA — must stay in sync.
    var tablesToCreate = [
        { tableName: "CORTEX_AISQL_USAGE_HISTORY",                  columns: ["QUERY_ID", "MODEL_NAME", "FUNCTION_NAME", "WAREHOUSE_ID", "USAGE_TIME"] },
        { tableName: "CORTEX_ANALYST_USAGE_HISTORY",                columns: ["START_TIME", "USERNAME"] },
        { tableName: "CORTEX_FINE_TUNING_USAGE_HISTORY",            columns: ["START_TIME", "MODEL_NAME"] },
        { tableName: "CORTEX_PROVISIONED_THROUGHPUT_USAGE_HISTORY", columns: ["PROVISIONED_THROUGHPUT_ID", "INTERVAL_START_TIME", "MODEL_NAME", "CLOUD_SERVICE_PROVIDER"] },
        { tableName: "CORTEX_REST_API_USAGE_HISTORY",               columns: ["REQUEST_ID", "MODEL_NAME", "START_TIME"] },
        { tableName: "CORTEX_SEARCH_DAILY_USAGE_HISTORY",           columns: ["USAGE_DATE", "SERVICE_ID", "CONSUMPTION_TYPE", "MODEL_NAME"] },
        { tableName: "CORTEX_SEARCH_SERVING_USAGE_HISTORY",         columns: ["START_TIME", "SERVICE_ID"] },
        { tableName: "CORTEX_SEARCH_REFRESH_HISTORY",               columns: ["DATABASE_NAME", "SCHEMA_NAME", "NAME", "REFRESH_START_TIME", "INDEX_PREPROCESSING_QUERY_ID", "INDEXING_QUERY_ID"] },
        { tableName: "SNOWPARK_CONTAINER_SERVICES_HISTORY",         columns: ["START_TIME", "COMPUTE_POOL_NAME", "APPLICATION_ID"] },
        { tableName: "CORTEX_AGENT_USAGE_HISTORY",                  columns: ["START_TIME", "REQUEST_ID", "USER_ID"] },
        { tableName: "CORTEX_AI_FUNCTIONS_USAGE_HISTORY",           columns: ["START_TIME", "QUERY_ID", "WAREHOUSE_ID"] },
        { tableName: "CORTEX_SEARCH_BATCH_QUERY_USAGE_HISTORY",     columns: ["START_TIME", "SERVICE_ID", "SERVICE_NAME"] },
        { tableName: "CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY",         columns: ["USAGE_TIME", "REQUEST_ID", "USER_ID"] },
        { tableName: "CORTEX_CODE_CLI_USAGE_HISTORY",               columns: ["USAGE_TIME", "REQUEST_ID", "USER_ID"] },
        { tableName: "CORTEX_AI_GUARDRAILS_USAGE_HISTORY",          columns: ["USAGE_TIME", "GUARDRAILS_SIGNAL"] },
        { tableName: "CORTEX_DOCUMENT_PROCESSING_USAGE_HISTORY",    columns: ["START_TIME", "FUNCTION_NAME", "MODEL_NAME"] },
        { tableName: "QUERY_ATTRIBUTION_HISTORY",                   columns: ["QUERY_ID", "START_TIME"] },
        { tableName: "SNOWFLAKE_INTELLIGENCE_USAGE_HISTORY",        columns: ["START_TIME", "REQUEST_ID", "USER_ID"] },
        { tableName: "CORTEX_REST_API_RATE_LIMIT_POLICIES",         columns: ["MODEL_NAME"] }
    ];
    var result = "";

    for (var i = 0; i < tablesToCreate.length; i++) {
        var table = tablesToCreate[i];
        try {
            var createTableQuery = `CREATE TRANSIENT TABLE IF NOT EXISTS  ${TARGET_DB}.${TARGET_SCHEMA}.${table.tableName}
                           WITH DATA_RETENTION_TIME_IN_DAYS = 0 LIKE  ${SOURCE_DB}.${SOURCE_SCHEMA}.${table.tableName}`;
            var createStatement = snowflake.createStatement({ sqlText: createTableQuery });
            createStatement.execute();

            var alterTable = `ALTER TABLE ${TARGET_DB}.${TARGET_SCHEMA}.${table.tableName}  ADD COLUMN status_date DATE `;
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


/**
   One-time setup: create the backup tables and do the first CDC pass.
   Replace SOURCE_DB / SOURCE_SCHEMA / TARGET_DB / TARGET_SCHEMA with the
   actual identifiers on your side (same convention as the parent CDC script).
*/
CALL CREATE_BACKUP_DB_CORTEX_TABLES('SOURCE_DB', 'SOURCE_SCHEMA', 'TARGET_DB', 'TARGET_SCHEMA');
CALL TP_CDC_CORTEX_METADATA('SOURCE_DB', 'SOURCE_SCHEMA', 'TARGET_DB', 'TARGET_SCHEMA');


/**
   Create scheduled task.

   Cadence: daily, matching the parent cdc_metadata task.
   Time:    02:30 UTC — offset 30 min from cdc_metadata (02:00 UTC) to avoid
            warehouse contention between the two CDC procedures.
            This also comfortably follows the last cortex replication run of
            the previous day (22:00 UTC), so the source has the freshest
            possible data before the CDC snapshot.
*/
CREATE OR REPLACE TASK cdc_cortex_metadata
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = 'USING CRON 30 2 * * * UTC'
AS
CALL TP_CDC_CORTEX_METADATA('SOURCE_DB', 'SOURCE_SCHEMA', 'TARGET_DB', 'TARGET_SCHEMA');

ALTER TASK cdc_cortex_metadata RESUME;
