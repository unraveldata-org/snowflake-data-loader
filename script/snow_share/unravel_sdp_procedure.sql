CREATE OR REPLACE PROCEDURE unravel_sdp_procedure("LOOKBACK_DAYS" NUMBER(38,0), "INPUT_TABLE" VARCHAR, "OUTPUT_TABLE" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('pandas==2.0.3','snowflake-snowpark-python==*','sqlglot==27.29.0')
HANDLER = 'main'
IMPORTS = (<path_to_unravel_sdp_package_in_stage>)
COMMENT='Unravel SDP Procedure'
EXECUTE AS OWNER
AS 'import snowflake.snowpark as snowpark
import pandas as pd
import re
from snowflake.snowpark.functions import pandas_udf, col, lit, to_timestamp, current_timestamp, current_date, max as max_
from snowflake.snowpark.types import (
    StringType,
    StructType,
    StructField,
    TimestampType
)
from datetime import datetime, timedelta
from unravel_sdp import mask_sql_query

def main(session: snowpark.Session, LOOKBACK_DAYS: int, INPUT_TABLE: str, OUTPUT_TABLE: str):
    """
    Mask SQL literals in query history with configurable parameters.
    
    Parameters:
    -----------
    session: snowpark.Session
        Active Snowpark session
    LOOKBACK_DAYS: int
        Number of days to look back for incremental processing
    INPUT_TABLE: str
        Fully qualified input table name (e.g., ''DB.SCHEMA.TABLE'')
    OUTPUT_TABLE: str
        Fully qualified output table name (e.g., ''DB.SCHEMA.TABLE'')
    
    Returns:
    --------
    String with processing summary
    """

    # ---- Configuration ----
    DEFAULT_CONFIG = {
        "string_replacement": "''''",
        "int_replacement": 1,
        "float_replacement": 0.0,
        "date_replacement": "''1970-01-01''",
        "datetime_replacement": "''1970-01-01 00:00:00''",
        "fail_safe_placeholder": ""
    }

    # ---- Vectorized UDF ----
    @pandas_udf(
        input_types=[StringType()],
        return_type=StringType()
    )
    def mask_queries_vectorized(queries: pd.Series) -> pd.Series:
        # return queries.apply(mask_sql_query)
        return queries.apply(mask_sql_query, args=(DEFAULT_CONFIG,))

    # ---- main logic ----

    print(f"Starting masking process...")
    print(f"Configuration:")
    print(f" - Lookback Days: {LOOKBACK_DAYS}")
    print(f" - Input Table: {INPUT_TABLE}")
    print(f" - Output Table: {OUTPUT_TABLE}")
    
    # Create output table if not exists (copy structure from input table)
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {OUTPUT_TABLE}
        AS
        SELECT *, CAST(NULL AS DATE) AS "STATUS_DATE" FROM {INPUT_TABLE} WHERE 1=0
    """).collect()

    # Ensure STATUS_DATE exists (for backward compatibility)
    session.sql(f"""
        ALTER TABLE {OUTPUT_TABLE}
        ADD COLUMN IF NOT EXISTS "STATUS_DATE" DATE
    """).collect()
    
    print(f"Output table validated: {OUTPUT_TABLE}")

    # Try to get max start_time from output table
    try:
        max_start_time_result = (
            session.table(OUTPUT_TABLE)
            .select(max_("START_TIME").alias("MAX_START_TIME"))
            .collect()
        )
    
        max_start_time = max_start_time_result[0]["MAX_START_TIME"]
    
        if max_start_time is not None:
            time_threshold = max_start_time
            print(f"Incremental load: Reading records after {time_threshold}")
        else:
            time_threshold = datetime.now() - timedelta(days=LOOKBACK_DAYS)
            print(f"Empty output table: Reading records from last {LOOKBACK_DAYS} days")
        
    except Exception as e:
        time_threshold = datetime.now() - timedelta(days=LOOKBACK_DAYS)
        print(f"Output table not found or error: Reading records from last {LOOKBACK_DAYS} days")
        print(f"Error details: {str(e)}")

    # Convert time_threshold to Snowpark column expression
    if isinstance(time_threshold, datetime):
        time_threshold_col = to_timestamp(lit(time_threshold.strftime(''%Y-%m-%d %H:%M:%S'')))
    else:
        time_threshold_col = lit(time_threshold)

    print(f"Computed look back time: {time_threshold}")
    
    # Step 1: Read only QUERY_ID and QUERY_TEXT for masking
    print(f"Reading data from input table: {INPUT_TABLE}")
    
    df_to_mask = (
        session.table(INPUT_TABLE)
        .filter(
            (col("QUERY_TEXT").is_not_null()) &
            (col("START_TIME") > time_threshold_col)
        )
        .select("QUERY_ID", "QUERY_TEXT")
    )

    record_count = df_to_mask.count()
    print(f"Total records fetched: {record_count}")

    if record_count == 0:
        print("No new records to process")
        return "No new records to process."

    # Step 2: Apply masking to QUERY_TEXT
    print("Applying masking to QUERY_TEXT...")
    
    df_masked = df_to_mask.select(
        col("QUERY_ID"),
        mask_queries_vectorized(col("QUERY_TEXT")).alias("MASKED_QUERY_TEXT")
    )
    
    # Step 3: Read all columns from source table (for the same records)
    print("Reading all columns from source table...")
    
    df_all_columns = (
        session.table(INPUT_TABLE)
        .filter(
            (col("QUERY_TEXT").is_not_null()) &
            (col("START_TIME") > time_threshold_col)
        )
    )
    
    # Step 4: Join to replace QUERY_TEXT with masked version
    print("Joining masked results with source data...")
    
    df_final = (
        df_all_columns
        .join(df_masked, on="QUERY_ID", how="inner")
        .drop("QUERY_TEXT")  # Drop original QUERY_TEXT
        .with_column_renamed("MASKED_QUERY_TEXT", "QUERY_TEXT")  # Rename masked column
        .with_column("STATUS_DATE", current_date())
    )
    
    # Step 5: Reorder columns to match output table structure
    print("Reordering columns to match output table schema...")
    
    # Get original column order from output table
    output_table_cols = session.table(OUTPUT_TABLE).columns
    df_final = df_final.select(*output_table_cols)
    
    # Step 6: Insert into output table
    print(f"Writing {record_count} records to {OUTPUT_TABLE}...")
    
    df_final.write.mode("append").save_as_table(OUTPUT_TABLE)
    
    print(f"Successfully inserted {record_count} records into {OUTPUT_TABLE}")
    
    # Return summary
    summary_message = f"Operation completed successfully! Processed {record_count} records from {INPUT_TABLE} to {OUTPUT_TABLE} (Lookback: {LOOKBACK_DAYS} days, Threshold: {time_threshold})"
    
    print(summary_message)

    return summary_message';
