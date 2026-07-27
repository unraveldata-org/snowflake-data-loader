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
    
    CREATE TABLE IF NOT EXISTS T_UNRAVEL_EXECUTION_LOG (
        TENANT_ID STRING,
        ACCT_ID STRING,
        EXECUTION_ID STRING,
        EXECUTION_TIME TIMESTAMP,
        WAREHOUSE_NAME STRING,
        WAREHOUSE_ID NUMBER,
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
    current_day_of_week NUMBER DEFAULT EXTRACT(DAYOFWEEK FROM CURRENT_DATE);
    current_hour_of_day NUMBER DEFAULT EXTRACT(HOUR FROM CURRENT_TIMESTAMP);
    execution_id VARCHAR;
    active_query VARCHAR;
    suspended_query VARCHAR;
    active_res RESULTSET;
    suspended_res RESULTSET;
BEGIN
    -- Set context to the provided database and schema
    EXECUTE IMMEDIATE 'USE DATABASE "' || db_name || '"';
    EXECUTE IMMEDIATE 'USE SCHEMA "' || schema_name || '"';
    
    execution_id := UUID_STRING();
    
    -- Log the procedure execution start
    INSERT INTO REPLICATION_LOG
        (executionStatus, remarks, taskName)
    VALUES
        ('STARTED', 'SP_EXECUTE_STATIC_RIGHTSIZING procedure started at ' || CURRENT_TIMESTAMP, 'SP_EXECUTE_STATIC_RIGHTSIZING');

    -- Build dynamic query for ACTIVE warehouses
    active_query := 'SELECT c.WAREHOUSE_NAME, c.WAREHOUSE_ID, c.ACCT_ID, c.CURRENT_SIZE, c.STRATEGY, s.TARGET_SIZE
        FROM ' || config_db_name || '.' || config_schema_name || '.T_UNRAVEL_WAREHOUSE_CONFIG c
        INNER JOIN ' || config_db_name || '.' || config_schema_name || '.T_UNRAVEL_STATIC_SCHEDULE s
            ON c.TENANT_ID = s.TENANT_ID
        WHERE c.TENANT_ID = ''' || current_tenant_id || '''
            AND s.DAY_OF_WEEK = ' || current_day_of_week || '
            AND s.HOUR_OF_DAY = ' || current_hour_of_day || '
            AND c.STRATEGY = ''STATIC''
            AND c.STATUS = ''ACTIVE''';

    active_res := (EXECUTE IMMEDIATE :active_query);

    -- Process ACTIVE warehouses
    FOR warehouse_record IN active_res DO
        BEGIN
            LET wh_name VARCHAR := warehouse_record.WAREHOUSE_NAME;
            LET wh_acct_id VARCHAR := warehouse_record.ACCT_ID;
            LET wh_warehouse_id NUMBER := warehouse_record.WAREHOUSE_ID;
            LET wh_current_size VARCHAR := warehouse_record.CURRENT_SIZE;
            LET wh_strategy VARCHAR := warehouse_record.STRATEGY;
            LET wh_target_size VARCHAR := warehouse_record.TARGET_SIZE;

            EXECUTE IMMEDIATE 'ALTER WAREHOUSE "' || wh_name || '" SET WAREHOUSE_SIZE = ' || wh_target_size;
            
            INSERT INTO T_UNRAVEL_EXECUTION_LOG
                (WAREHOUSE_NAME, WAREHOUSE_ID, ACTION, RESULT, EXECUTION_TIME, TENANT_ID, NEW_SIZE, EXECUTION_ID, ACCT_ID, STRATEGY, CURRENT_SIZE)
            VALUES
                (:wh_name, :wh_warehouse_id, 'RESIZE', 'SUCCESS', CURRENT_TIMESTAMP, :current_tenant_id, :wh_target_size, :execution_id, :wh_acct_id, :wh_strategy, :wh_current_size);
        EXCEPTION WHEN OTHERS THEN
            INSERT INTO T_UNRAVEL_EXECUTION_LOG
                (WAREHOUSE_NAME, WAREHOUSE_ID, ACTION, RESULT, ERROR_MESSAGE, EXECUTION_TIME, TENANT_ID, NEW_SIZE, EXECUTION_ID, ACCT_ID, STRATEGY, CURRENT_SIZE)
            VALUES
                (:wh_name, :wh_warehouse_id, 'RESIZE', 'FAILED', SQLERRM, CURRENT_TIMESTAMP, :current_tenant_id, :wh_target_size, :execution_id, :wh_acct_id, :wh_strategy, :wh_current_size);
        END;
    END FOR;

    -- Build dynamic query for SUSPENDED warehouses
    suspended_query := 'SELECT c.WAREHOUSE_NAME, c.WAREHOUSE_ID, c.ACCT_ID, c.CURRENT_SIZE, c.STRATEGY, s.TARGET_SIZE
        FROM ' || config_db_name || '.' || config_schema_name || '.T_UNRAVEL_WAREHOUSE_CONFIG c
        INNER JOIN ' || config_db_name || '.' || config_schema_name || '.T_UNRAVEL_STATIC_SCHEDULE s
            ON c.TENANT_ID = s.TENANT_ID
        WHERE c.TENANT_ID = ''' || current_tenant_id || '''
            AND s.DAY_OF_WEEK = ' || current_day_of_week || '
            AND s.HOUR_OF_DAY = ' || current_hour_of_day || '
            AND c.STRATEGY = ''STATIC''
            AND c.STATUS = ''SUSPENDED''';

    suspended_res := (EXECUTE IMMEDIATE :suspended_query);

    -- Process SUSPENDED warehouses
    FOR warehouse_record IN suspended_res DO
        BEGIN
            LET wh_name VARCHAR := warehouse_record.WAREHOUSE_NAME;
            LET wh_warehouse_id NUMBER := warehouse_record.WAREHOUSE_ID;
            LET wh_acct_id VARCHAR := warehouse_record.ACCT_ID;
            LET wh_current_size VARCHAR := warehouse_record.CURRENT_SIZE;
            LET wh_strategy VARCHAR := warehouse_record.STRATEGY;
            LET wh_target_size VARCHAR := warehouse_record.TARGET_SIZE;

            EXECUTE IMMEDIATE 'ALTER WAREHOUSE "' || wh_name || '" SET WAREHOUSE_SIZE = ' || wh_target_size;
            
            INSERT INTO T_UNRAVEL_EXECUTION_LOG
                (WAREHOUSE_NAME, WAREHOUSE_ID, ACTION, RESULT, EXECUTION_TIME, TENANT_ID, NEW_SIZE, EXECUTION_ID, ACCT_ID, STRATEGY, CURRENT_SIZE)
            VALUES
                (:wh_name, :wh_warehouse_id, 'RESIZE', 'SUCCESS', CURRENT_TIMESTAMP, :current_tenant_id, :wh_target_size, :execution_id, :wh_acct_id, :wh_strategy, :wh_current_size);
        EXCEPTION WHEN OTHERS THEN
            INSERT INTO T_UNRAVEL_EXECUTION_LOG
                (WAREHOUSE_NAME, WAREHOUSE_ID, ACTION, RESULT, ERROR_MESSAGE, EXECUTION_TIME, TENANT_ID, NEW_SIZE, EXECUTION_ID, ACCT_ID, STRATEGY, CURRENT_SIZE)
            VALUES
                (:wh_name, :wh_warehouse_id, 'RESIZE', 'FAILED', SQLERRM, CURRENT_TIMESTAMP, :current_tenant_id, :wh_target_size, :execution_id, :wh_acct_id, :wh_strategy, :wh_current_size);
        END;
    END FOR;

    -- Log the procedure execution completion
    INSERT INTO REPLICATION_LOG
        (executionStatus, remarks, taskName)
    VALUES
        ('COMPLETED', 'SP_EXECUTE_STATIC_RIGHTSIZING procedure completed at ' || CURRENT_TIMESTAMP, 'SP_EXECUTE_STATIC_RIGHTSIZING');
    
    RETURN 'SUCCESS';
END;
$$;

-- Create or replace a procedure to share T_UNRAVEL_EXECUTION_LOG table
-- Note: Using the same share name (S_SECURE_SHARE) as defined in snow_share_procedure.sql
CREATE OR REPLACE PROCEDURE SP_SHARE_EXECUTION_LOG()
RETURNS STRING NOT NULL
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Grant select on T_UNRAVEL_EXECUTION_LOG table to the share
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
