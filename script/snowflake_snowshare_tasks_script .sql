-- create account usage tables Task
CREATE OR REPLACE TASK replicate_metadata
  WAREHOUSE = UNRAVELDATA
  SCHEDULE = '60 MINUTE'
AS
call REPLICATE_ACCOUNT_USAGE('UNRAVEL_SHARE','SCHEMA_4823',2);

-- create warehouse replicate Task
CREATE OR REPLACE TASK createWarehouseTable
  WAREHOUSE = UNRAVELDATA
  SCHEDULE = '60 MINUTE'
AS
call warehouse_proc('UNRAVEL_SHARE','SCHEMA_4823');

-- create profile replicate task
CREATE OR REPLACE TASK createProfileTable
  WAREHOUSE = UNRAVELDATA
  SCHEDULE = '60 MINUTE'
AS
call create_query_profile('UNRAVEL_SHARE','SCHEMA_4823');

-- create Task for replicating information schema query history
CREATE OR REPLACE TASK replicate_realtime_query
  WAREHOUSE = UNRAVELDATA
  SCHEDULE = '10 MINUTE'
AS
call REPLICATE_REALTIME_QUERY('UNRAVEL_SHARE','SCHEMA_4823',10);

ALTER TASK replicate_metadata RESUME;

ALTER TASK createWarehouseTable RESUME;

ALTER TASK createProfileTable RESUME;

ALTER TASK replicate_realtime_query RESUME;