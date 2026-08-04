-- Set the database and schema context
-- Note: Using the same database name (UNRAVEL_SHARE) and schema name (SCHEMA_4827_T) as defined in snow_share_procedure.sql
USE DATABASE UNRAVEL_SHARE;
USE SCHEMA SCHEMA_4827_T;

-- Procedure to create all required tables
CREATE OR REPLACE PROCEDURE CREATE_UNRAVEL_TABLES()
RETURNS STRING NOT NULL
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    
    -- Log the procedure execution start
    INSERT INTO REPLICATION_LOG
        (executionStatus, remarks, taskName)
    VALUES
        ('STARTED', 'CREATE_UNRAVEL_TABLES procedure started at ' || CURRENT_TIMESTAMP, 'CREATE_UNRAVEL_TABLES');
    
    CREATE TABLE IF NOT EXISTS T_UNRAVEL_CONFIG (
        TENANT_ID STRING,
        ACCT_ID STRING,
        OBJECT_TYPE STRING,
        OBJECT_NAME STRING,
        KEY STRING,
        VALUE STRING,
        CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );

    -- Log T_UNRAVEL_CONFIG table creation
    INSERT INTO REPLICATION_LOG
        (executionStatus, remarks, taskName)
    VALUES
        ('COMPLETED', 'T_UNRAVEL_CONFIG table created successfully at ' || CURRENT_TIMESTAMP, 'CREATE_UNRAVEL_TABLES');

    CREATE TABLE IF NOT EXISTS T_UNRAVEL_STATIC_SCHEDULE (
        TENANT_ID STRING,
        ACCT_ID STRING,
        WAREHOUSE_NAME STRING,
        DAY_OF_WEEK NUMBER,
        HOUR_OF_DAY NUMBER,
        TARGET_SIZE STRING,
        EXPECTED_QUEUE FLOAT,
        EXPECTED_SPILL FLOAT,
        EXPECTED_CREDITS FLOAT,
        CONFIDENCE FLOAT,
        GENERATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );

    -- Log T_UNRAVEL_STATIC_SCHEDULE table creation
    INSERT INTO REPLICATION_LOG
        (executionStatus, remarks, taskName)
    VALUES
        ('COMPLETED', 'T_UNRAVEL_STATIC_SCHEDULE table created successfully at ' || CURRENT_TIMESTAMP, 'CREATE_UNRAVEL_TABLES');

    CREATE TABLE IF NOT EXISTS T_UNRAVEL_EXECUTION_LOG (
        TENANT_ID STRING,
        ACCT_ID STRING,
        EXECUTION_ID STRING,
        EXECUTION_TIME TIMESTAMP,
        WAREHOUSE_NAME STRING,
        STRATEGY STRING,
        CURRENT_SIZE STRING,
        NEW_SIZE STRING,
        CURRENT_CLUSTER NUMBER,
        NEW_CLUSTER NUMBER,
        CURRENT_TIMEOUT NUMBER,
        NEW_TIMEOUT NUMBER,
        ACTION STRING,
        RESULT STRING,
        ERROR_MESSAGE STRING
    );

    -- Log T_UNRAVEL_EXECUTION_LOG table creation
    INSERT INTO REPLICATION_LOG
        (executionStatus, remarks, taskName)
    VALUES
        ('COMPLETED', 'T_UNRAVEL_EXECUTION_LOG table created successfully at ' || CURRENT_TIMESTAMP, 'CREATE_UNRAVEL_TABLES');

    -- Log the procedure execution completion
    INSERT INTO REPLICATION_LOG
        (executionStatus, remarks, taskName)
    VALUES
        ('COMPLETED', 'CREATE_UNRAVEL_TABLES procedure completed at ' || CURRENT_TIMESTAMP, 'CREATE_UNRAVEL_TABLES');

    RETURN 'SUCCESS';

END;
$$;

-- Create or replace the stored procedure for static rightsizing
-- Arguments:
--   db_name: UNRAVEL_SHARE (customer database)
--   schema_name: SCHEMA_4827_T (customer schema)
--   config_db_name: <CUSTOMER_NAME>_REVERSE_SHARE (unravel shared database)
--   config_schema_name: <ACCOUNT_NAME>_UNRAVEL_SHARE (unravel shared schema)
--   current_tenant_id: <TENANT_ID> (tenant identifier)
CREATE OR REPLACE PROCEDURE SP_EXECUTE_STATIC_RIGHTSIZING(db_name VARCHAR, schema_name VARCHAR, config_db_name VARCHAR, config_schema_name VARCHAR, current_tenant_id VARCHAR)
RETURNS STRING NOT NULL
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    current_day_of_week NUMBER;
    current_hour_of_day NUMBER;
    execution_id VARCHAR;
    active_query VARCHAR;
    suspended_query VARCHAR;
    active_res RESULTSET;
    suspended_res RESULTSET;
    last_sync_time TIMESTAMP;
    sync_lag_hours FLOAT;
BEGIN
    BEGIN
        -- Set context to the provided database and schema
        EXECUTE IMMEDIATE 'USE DATABASE "' || db_name || '"';
        EXECUTE IMMEDIATE 'USE SCHEMA "' || schema_name || '"';

        current_day_of_week := EXTRACT(DAYOFWEEK FROM CURRENT_DATE());
        current_hour_of_day := EXTRACT(HOUR FROM CURRENT_TIMESTAMP());
        execution_id := UUID_STRING();
        
        -- Log the procedure execution start
        INSERT INTO REPLICATION_LOG
            (executionStatus, remarks, taskName)
        VALUES
            ('STARTED', 'SP_EXECUTE_STATIC_RIGHTSIZING procedure started at ' || CURRENT_TIMESTAMP, 'SP_EXECUTE_STATIC_RIGHTSIZING');

        -- Step 1: Merge T_UNRAVEL_CONFIG from config schema to local schema to avoid duplicates
        BEGIN
            EXECUTE IMMEDIATE 'MERGE INTO "' || db_name || '"."' || schema_name || '".T_UNRAVEL_CONFIG target
                USING "' || config_db_name || '"."' || config_schema_name || '".T_UNRAVEL_CONFIG source
                ON target.TENANT_ID = source.TENANT_ID 
                    AND target.ACCT_ID = source.ACCT_ID 
                    AND target.OBJECT_TYPE = source.OBJECT_TYPE 
                    AND target.OBJECT_NAME = source.OBJECT_NAME 
                    AND target.KEY = source.KEY
                WHEN MATCHED AND source.UPDATED_AT > target.UPDATED_AT THEN
                    UPDATE SET target.VALUE = source.VALUE, target.UPDATED_AT = source.UPDATED_AT
                WHEN NOT MATCHED THEN
                    INSERT (TENANT_ID, ACCT_ID, OBJECT_TYPE, OBJECT_NAME, KEY, VALUE, CREATED_AT, UPDATED_AT)
                    VALUES (source.TENANT_ID, source.ACCT_ID, source.OBJECT_TYPE, source.OBJECT_NAME, source.KEY, source.VALUE, source.CREATED_AT, source.UPDATED_AT)';
        EXCEPTION WHEN OTHER THEN
            INSERT INTO REPLICATION_LOG
                (executionStatus, remarks, taskName)
            VALUES
                ('CRITICAL', 'T_UNRAVEL_CONFIG in Config schema unreachable: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE, 'SP_EXECUTE_STATIC_RIGHTSIZING');
        END;

        -- Step 2: Merge T_UNRAVEL_STATIC_SCHEDULE from config schema to local schema to avoid duplicates
        BEGIN
            EXECUTE IMMEDIATE 'MERGE INTO "' || db_name || '"."' || schema_name || '".T_UNRAVEL_STATIC_SCHEDULE target
                USING "' || config_db_name || '"."' || config_schema_name || '".T_UNRAVEL_STATIC_SCHEDULE source
                ON target.TENANT_ID = source.TENANT_ID 
                    AND target.ACCT_ID = source.ACCT_ID 
                    AND target.WAREHOUSE_NAME = source.WAREHOUSE_NAME 
                    AND target.DAY_OF_WEEK = source.DAY_OF_WEEK 
                    AND target.HOUR_OF_DAY = source.HOUR_OF_DAY
                WHEN MATCHED AND source.GENERATED_AT > target.GENERATED_AT THEN
                    UPDATE SET target.TARGET_SIZE = source.TARGET_SIZE, target.EXPECTED_QUEUE = source.EXPECTED_QUEUE, 
                               target.EXPECTED_SPILL = source.EXPECTED_SPILL, target.EXPECTED_CREDITS = source.EXPECTED_CREDITS, 
                               target.CONFIDENCE = source.CONFIDENCE, target.GENERATED_AT = source.GENERATED_AT
                WHEN NOT MATCHED THEN
                    INSERT (TENANT_ID, ACCT_ID, WAREHOUSE_NAME, DAY_OF_WEEK, HOUR_OF_DAY, TARGET_SIZE, EXPECTED_QUEUE, EXPECTED_SPILL, EXPECTED_CREDITS, CONFIDENCE, GENERATED_AT)
                    VALUES (source.TENANT_ID, source.ACCT_ID, source.WAREHOUSE_NAME, source.DAY_OF_WEEK, source.HOUR_OF_DAY, source.TARGET_SIZE, source.EXPECTED_QUEUE, source.EXPECTED_SPILL, source.EXPECTED_CREDITS, source.CONFIDENCE, source.GENERATED_AT)';
        EXCEPTION WHEN OTHER THEN
            INSERT INTO REPLICATION_LOG
                (executionStatus, remarks, taskName)
            VALUES
                ('CRITICAL', 'T_UNRAVEL_STATIC_SCHEDULE in Config schema unreachable: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE, 'SP_EXECUTE_STATIC_RIGHTSIZING');
        END;

        -- Check data sync status
        BEGIN
            SELECT MAX(GENERATED_AT) INTO last_sync_time
            FROM T_UNRAVEL_STATIC_SCHEDULE
            WHERE TENANT_ID = current_tenant_id;

            sync_lag_hours := DATEDIFF('hour', last_sync_time, CURRENT_TIMESTAMP());

            IF (sync_lag_hours > 2 OR last_sync_time IS NULL) THEN
                INSERT INTO REPLICATION_LOG (executionStatus, remarks, taskName)
                VALUES ('CRITICAL', 
                    'Data out of sync: last sync was ' || sync_lag_hours::VARCHAR || ' hours ago at ' || last_sync_time::VARCHAR,
                    'SP_EXECUTE_STATIC_RIGHTSIZING');
            END IF;
        END;

        -- Build dynamic query for ACTIVE warehouses using local tables
        active_query := 'SELECT c.OBJECT_NAME as WAREHOUSE_NAME, c.ACCT_ID,
            MAX(CASE WHEN c.KEY = ''CURRENT_SIZE'' THEN c.VALUE END) as CURRENT_SIZE,
            MAX(CASE WHEN c.KEY = ''STRATEGY'' THEN c.VALUE END) as STRATEGY,
            s.TARGET_SIZE
            FROM "' || db_name || '"."' || schema_name || '".T_UNRAVEL_CONFIG c
            INNER JOIN "' || db_name || '"."' || schema_name || '".T_UNRAVEL_STATIC_SCHEDULE s
                ON c.TENANT_ID = s.TENANT_ID AND c.ACCT_ID = s.ACCT_ID AND c.OBJECT_NAME = s.WAREHOUSE_NAME
            WHERE c.TENANT_ID = ''' || current_tenant_id || '''
                AND s.DAY_OF_WEEK = ' || current_day_of_week::VARCHAR || '
                AND s.HOUR_OF_DAY = ' || current_hour_of_day::VARCHAR || '
                AND c.OBJECT_TYPE = ''WAREHOUSE''
            GROUP BY c.OBJECT_NAME, c.ACCT_ID, s.TARGET_SIZE
            HAVING MAX(CASE WHEN c.KEY = ''STRATEGY'' THEN c.VALUE END) = ''STATIC''
                AND MAX(CASE WHEN c.KEY = ''STATUS'' THEN c.VALUE END) = ''ACTIVE''
                AND COALESCE(MAX(CASE WHEN c.KEY = ''ENABLED'' THEN c.VALUE END), ''true'') != ''false''';

        active_res := (EXECUTE IMMEDIATE :active_query);

        -- Process ACTIVE warehouses
        FOR warehouse_record IN active_res DO
            BEGIN
                LET wh_name VARCHAR := warehouse_record.WAREHOUSE_NAME;
                LET wh_acct_id VARCHAR := warehouse_record.ACCT_ID;
                LET wh_current_size VARCHAR := warehouse_record.CURRENT_SIZE;
                LET wh_strategy VARCHAR := warehouse_record.STRATEGY;
                LET wh_target_size VARCHAR := warehouse_record.TARGET_SIZE;

                EXECUTE IMMEDIATE 'ALTER WAREHOUSE "' || wh_name || '" SET WAREHOUSE_SIZE = ' || wh_target_size;
                
                INSERT INTO T_UNRAVEL_EXECUTION_LOG
                    (WAREHOUSE_NAME, ACTION, RESULT, EXECUTION_TIME, TENANT_ID, NEW_SIZE, EXECUTION_ID, ACCT_ID, STRATEGY, CURRENT_SIZE)
                VALUES
                    (:wh_name, 'RESIZE', 'SUCCESS', CURRENT_TIMESTAMP, :current_tenant_id, :wh_target_size, :execution_id, :wh_acct_id, :wh_strategy, :wh_current_size);
            EXCEPTION WHEN OTHER THEN
                INSERT INTO T_UNRAVEL_EXECUTION_LOG
                    (WAREHOUSE_NAME, ACTION, RESULT, ERROR_MESSAGE, EXECUTION_TIME, TENANT_ID, NEW_SIZE, EXECUTION_ID, ACCT_ID, STRATEGY, CURRENT_SIZE)
                VALUES
                    (:wh_name, 'RESIZE', 'FAILED', SQLERRM, CURRENT_TIMESTAMP, :current_tenant_id, :wh_target_size, :execution_id, :wh_acct_id, :wh_strategy, :wh_current_size);
            END;
        END FOR;

        -- Build dynamic query for SUSPENDED warehouses using local tables
        suspended_query := 'SELECT c.OBJECT_NAME as WAREHOUSE_NAME, c.ACCT_ID,
            MAX(CASE WHEN c.KEY = ''CURRENT_SIZE'' THEN c.VALUE END) as CURRENT_SIZE,
            MAX(CASE WHEN c.KEY = ''STRATEGY'' THEN c.VALUE END) as STRATEGY,
            s.TARGET_SIZE
            FROM "' || db_name || '"."' || schema_name || '".T_UNRAVEL_CONFIG c
            INNER JOIN "' || db_name || '"."' || schema_name || '".T_UNRAVEL_STATIC_SCHEDULE s
                ON c.TENANT_ID = s.TENANT_ID AND c.ACCT_ID = s.ACCT_ID AND c.OBJECT_NAME = s.WAREHOUSE_NAME
            WHERE c.TENANT_ID = ''' || current_tenant_id || '''
                AND s.DAY_OF_WEEK = ' || current_day_of_week::VARCHAR || '
                AND s.HOUR_OF_DAY = ' || current_hour_of_day::VARCHAR || '
                AND c.OBJECT_TYPE = ''WAREHOUSE''
            GROUP BY c.OBJECT_NAME, c.ACCT_ID, s.TARGET_SIZE
            HAVING MAX(CASE WHEN c.KEY = ''STRATEGY'' THEN c.VALUE END) = ''STATIC''
                AND MAX(CASE WHEN c.KEY = ''STATUS'' THEN c.VALUE END) = ''SUSPENDED''
                AND COALESCE(MAX(CASE WHEN c.KEY = ''ENABLED'' THEN c.VALUE END), ''true'') != ''false''';

        suspended_res := (EXECUTE IMMEDIATE :suspended_query);

        -- Process SUSPENDED warehouses
        FOR warehouse_record IN suspended_res DO
            BEGIN
                LET wh_name VARCHAR := warehouse_record.WAREHOUSE_NAME;
                LET wh_acct_id VARCHAR := warehouse_record.ACCT_ID;
                LET wh_current_size VARCHAR := warehouse_record.CURRENT_SIZE;
                LET wh_strategy VARCHAR := warehouse_record.STRATEGY;
                LET wh_target_size VARCHAR := warehouse_record.TARGET_SIZE;

                EXECUTE IMMEDIATE 'ALTER WAREHOUSE "' || wh_name || '" SET WAREHOUSE_SIZE = ' || wh_target_size;
                
                INSERT INTO T_UNRAVEL_EXECUTION_LOG
                    (WAREHOUSE_NAME, ACTION, RESULT, EXECUTION_TIME, TENANT_ID, NEW_SIZE, EXECUTION_ID, ACCT_ID, STRATEGY, CURRENT_SIZE)
                VALUES
                    (:wh_name, 'RESIZE', 'SUCCESS', CURRENT_TIMESTAMP, :current_tenant_id, :wh_target_size, :execution_id, :wh_acct_id, :wh_strategy, :wh_current_size);
            EXCEPTION WHEN OTHER THEN
                INSERT INTO T_UNRAVEL_EXECUTION_LOG
                    (WAREHOUSE_NAME, ACTION, RESULT, ERROR_MESSAGE, EXECUTION_TIME, TENANT_ID, NEW_SIZE, EXECUTION_ID, ACCT_ID, STRATEGY, CURRENT_SIZE)
                VALUES
                    (:wh_name, 'RESIZE', 'FAILED', SQLERRM, CURRENT_TIMESTAMP, :current_tenant_id, :wh_target_size, :execution_id, :wh_acct_id, :wh_strategy, :wh_current_size);
            END;
        END FOR;

        -- Log the procedure execution completion
        INSERT INTO REPLICATION_LOG
            (executionStatus, remarks, taskName)
        VALUES
            ('COMPLETED', 'SP_EXECUTE_STATIC_RIGHTSIZING procedure completed at ' || CURRENT_TIMESTAMP, 'SP_EXECUTE_STATIC_RIGHTSIZING');
        
        RETURN 'SUCCESS';
    EXCEPTION WHEN OTHER THEN
        INSERT INTO REPLICATION_LOG (executionStatus, remarks, taskName)
        VALUES ('CRITICAL', 
            'Unhandled error in SP_EXECUTE_STATIC_RIGHTSIZING: ' || SQLERRM || ' | SQLSTATE: ' || SQLSTATE,
            'SP_EXECUTE_STATIC_RIGHTSIZING');
        RETURN 'FAILED: ' || SQLERRM;
    END;
END;
$$;

-- Create or replace a procedure to share all tables
-- Note: Using the same share name (S_SECURE_SHARE) as defined in snow_share_procedure.sql
CREATE OR REPLACE PROCEDURE SP_SHARE_EXECUTION_LOG()
RETURNS STRING NOT NULL
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Grant select on all tables to the share
    GRANT SELECT ON TABLE T_UNRAVEL_CONFIG TO SHARE S_SECURE_SHARE;
    GRANT SELECT ON TABLE T_UNRAVEL_STATIC_SCHEDULE TO SHARE S_SECURE_SHARE;
    GRANT SELECT ON TABLE T_UNRAVEL_EXECUTION_LOG TO SHARE S_SECURE_SHARE;
    
    RETURN 'SUCCESS';
END;
$$;

-- Step-1: Call procedure to create all tables
CALL CREATE_UNRAVEL_TABLES();

-- Step-2: Call the procedure to share T_UNRAVEL_EXECUTION_LOG table with the target account
CALL SP_SHARE_EXECUTION_LOG();

-- Step-3: Create or replace a scheduled task to execute static rightsizing
-- Note: UNRAVEL_SHARE database and SCHEMA_4827_T schema is customer shared to unravel database and schema
-- <CUSTOMER_NAME>_REVERSE_SHARE database and <ACCOUNT_NAME>_UNRAVEL_SHARE schema is unravel shared to customer database and schema
-- Arguments:
--   db_name: UNRAVEL_SHARE (customer database)
--   schema_name: SCHEMA_4827_T (customer schema)
--   config_db_name: <CUSTOMER_NAME>_REVERSE_SHARE (unravel shared database)
--   config_schema_name: <ACCOUNT_NAME>_UNRAVEL_SHARE (unravel shared schema)
--   current_tenant_id: <TENANT_ID> (tenant identifier)
CREATE OR REPLACE TASK SP_EXECUTE_STATIC_RIGHTSIZING_TASK
 WAREHOUSE = UNRAVELDATA
 SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
CALL SP_EXECUTE_STATIC_RIGHTSIZING('UNRAVEL_SHARE', 'SCHEMA_4827_T', '<CUSTOMER_NAME>_REVERSE_SHARE', '<ACCOUNT_NAME>_UNRAVEL_SHARE', '<TENANT_ID>');

-- Step-4: Resume the task to enable it
ALTER TASK SP_EXECUTE_STATIC_RIGHTSIZING_TASK RESUME;
