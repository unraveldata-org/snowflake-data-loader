import great_expectations as ge
import argparse
from ruamel import yaml

parser = argparse.ArgumentParser()

parser.add_argument("--db_name", help="Database Name")
parser.add_argument("--table_name", help="Table Name")
parser.add_argument("--warehouse_name", help="Table Name")
parser.add_argument("--schema_name", help="Table Name")
args = parser.parse_args()

db_name = args.db_name
table_name = args.table_name
warehouse = args.warehouse_name
schema_name = args.schema_name

lr_url = f'http://localhost:4043'

#connection_string = f"snowflake://<username>:<password>@<accountname>/{db_name}/{schema_name}?warehouse={warehouse}&role=ACCOUNTADMIN&application=great_expectations_oss"
connection_string = f"snowflake://ssawant:Swat%402023@rtb81672.us-east-1/{db_name}/{schema_name}?warehouse={warehouse}&role=ACCOUNTADMIN&application=great_expectations_oss"
context = ge.get_context()

datasource_yaml = f"""
name: {db_name}
class_name: Datasource
execution_engine:
  class_name: SqlAlchemyExecutionEngine
  connection_string: {connection_string}
data_connectors:
   default_runtime_data_connector_name:
       class_name: RuntimeDataConnector
       batch_identifiers:
           - default_identifier_name
   default_inferred_data_connector_name:
       class_name: InferredAssetSqlDataConnector
       include_schema_name: false
"""

context.test_yaml_config(datasource_yaml)
context.add_datasource(**yaml.load(datasource_yaml))
suite = context.add_or_update_expectation_suite(expectation_suite_name="version-0.15.50 covid_expectation")
sql_query = f"SELECT * from {db_name}.{schema_name}.{table_name.lower()} LIMIT 1000"
context.save_expectation_suite(expectation_suite=suite)
checkpoint_yaml = f"""
name: test_checkpoint
config_version: 1
class_name: Checkpoint
run_name_template: "%Y-%M-foo-bar-template"
validations:
  - batch_request:
      datasource_name: {db_name}
      data_connector_name: default_runtime_data_connector_name
      data_asset_name: {table_name}
      batch_identifiers:
        default_identifier_name: default_identifier
      runtime_parameters:
        query: {sql_query}
    action_list:
      - name: UnravelAction
        action:
          class_name: UnravelAction
          module_name: unravelaction
          lr_url: "{lr_url}"
          lr_version: "v2"
          index: "events_sf_t1-"
    expectation_suite_name: covid_expectation
"""

context.add_checkpoint(**yaml.safe_load(checkpoint_yaml))

results = context.run_checkpoint(
    checkpoint_name="test_checkpoint"
)

print(results)
