create or replace procedure create_query_profile()
  returns string
  language python
  runtime_version = '3.8'
  packages = ('snowflake-snowpark-python')
  handler = 'run'
as
$$
def run(session):
    try :
        session.sql("CREATE TABLE IF NOT EXISTS query_history_temp AS SELECT query_id, start_time FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE START_TIME > dateadd(day, -14, CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP))) ORDER BY start_time").collect()

        session.sql("""
        CREATE TABLE IF NOT EXISTS QUERY_PROFILE (
            START_TIME TIMESTAMP_LTZ(6),
            QUERY_ID VARCHAR(16777216),
            STEP_ID NUMBER(38, 0),
            OPERATOR_ID NUMBER(38,0),
            PARENT_OPERATORS ARRAY,
            OPERATOR_TYPE VARCHAR(16777216),
            OPERATOR_STATISTICS VARIANT,
            EXECUTION_TIME_BREAKDOWN VARIANT,
            OPERATOR_ATTRIBUTES VARIANT
        )
        """).collect()

        chunk_size = 100
        offset = 0
        query = None

        while True:
            count_of_records = session.sql("select count(*) from QUERY_PROFILE").collect()
            for row in count_of_records:
                count = row[0]

            if count == 0:
                query = f"SELECT query_id, start_time FROM query_history_temp ORDER BY start_time LIMIT {chunk_size} OFFSET {offset}"
            else:
                query = f"SELECT qht.query_id as query_id, qht.start_time as start_time FROM query_history_temp qht WHERE qht.start_time > (SELECT MAX(qp.START_TIME) FROM QUERY_PROFILE qp) ORDER BY qht.start_time LIMIT {chunk_size} OFFSET {offset};"

            query_ids_chunk = session.sql(query).collect()

            if not query_ids_chunk:
                break

            for query_id_row in query_ids_chunk:
                query_id = query_id_row[0]
                start_time = query_id_row[1]
                session.sql("INSERT INTO QUERY_PROFILE (START_TIME, QUERY_ID, STEP_ID, OPERATOR_ID, PARENT_OPERATORS, OPERATOR_TYPE, OPERATOR_STATISTICS, EXECUTION_TIME_BREAKDOWN,   OPERATOR_ATTRIBUTES) SELECT ? , QUERY_ID, STEP_ID, OPERATOR_ID, PARENT_OPERATORS, OPERATOR_TYPE, OPERATOR_STATISTICS, EXECUTION_TIME_BREAKDOWN, OPERATOR_ATTRIBUTES FROM table(get_query_operator_stats(?))", params=(start_time, query_id)).collect()

            offset += chunk_size

        return f"SUCCESS, count = {count} query = {str(query)}"

    except Exception as e:
        return f"ERROR: {str(e)}, query_id = {str(query_id)}"
$$;

CALL create_query_profile();
