# snowflake-data-loader

This program enables you to retrieve account usage information from one Snowflake account and upload to a second account. 

The script will perform the following actions:
- Create stage in the source Snowflake account
- Save source Snowflake account usage to stage 
- Download account usage information from the source Snowflake account to local
- Create stage in the target Snowflake account
- Upload the account usage information to the target Snowflake account
- (Optional) Create dedicate user, schema, table in the target Snowflake account for Unravel

## Prerequisites
- Source account user must have the permissions to access `SNOWFLAKE.ACCOUNT_USAGE` and `SNOWFLAKE.INFORMATION_SCHEMA` schema.
- `CREATE STAGE` permission on the source account database and schema.
- `CREATE STAGE` permission on the target account database and schema.
- `Insert`, `Select`, `Update` permission on the target account database and schema.

In the case to create Unravel dedicate snowflake user, grant following extra permissions are needed to the target user uses to upload data 
- `CREATE USER`, `CREATE ROLE` at account level;
- `MODIFY`, `USAGE`, `CREATE` `SCHEMA`, `CREATE PROCEDURE`, `CREATE STAGE` on the database;

It requires certain arguments to be passed via the command line, which can also be prompted for in case they are missing. The script then returns the parsed arguments to the calling function.

The following arguments are required:
* `--source_user`: Your source Snowflake account username.
* `--source_password`: Your source Snowflake account password. If source_login_method is `password`, this argument is required.
* `--private_key_path`: The path to your private key file. If source_login_method or target_login_method is `keypair`, this argument is required. The key will be used for both source and target accounts.
* `--source_account`: Your source Snowflake account ID.
* `--source_warehouse`: The name of the source warehouse you wish to retrieve data from.
* `--source_database`: The name of the source account database where the stage will be created.
* `--source_schema`: The name of the source account schema where the stage will be created.
* `--source_role`: The name of the source account role.
* `--target_user`: Your target Snowflake account username.
* `--target_password`: Your target Snowflake account password.
* `--target_account`: Your target Snowflake account ID.
* `--target_warehouse`: The name of the target warehouse you wish to upload the data to.
* `--target_database`: The name of the target database.
* `--target_schema`: The name of the target schema.
* `--target_role`: The name of the target role.

The following arguments are optional:
* `--source_login_method`: The login method for the source account. Possible options are password (default), oauth, sso, okta, or keypair.
* `--target_login_method`: The login method for the target account. Possible options are password (default), oauth, sso, okta, or keypair.
* `--source_private_link`: The private link for the source account e.g testaccount.us-east-1.privatelink.snowflakecomputing.com.
* `--target_private_link`: The private link for the target account e.g testaccount.us-east-1.privatelink.snowflakecomputing.com.
* `--create`: This flag will trigger creation of the new user, schema, table in the target accounts to be used for Unravel.
* `--target_new_user`: The new username to be created in the target account. If this argument is not provided, the script will random generate a username.
* `--target_new_user_pass`: The password for the new user in target account. If this argument is not provided, the script will random generate a password.
* `--target_new_role`: The new role to be created in the target account. If this argument is not provided, the script will random generate a role name.
* `--source_okta_url`: The okta url for the source account e.g https://testaccount.okta.com.
* `--target_okta_url`: The okta url for the target account e.g https://testaccount.okta.com.
* `--source_passcode`: Your source Snowflake account MFA password.
* `--target_passcode`: Your target Snowflake account MFA password.
* `--stage`: The name of the stage. Default is `unravel_stage`.
* `--out`: The directory to save output files. Default is current directory.
* `--file_format`: The name of the file format. Default is `unravel_file_format`.
* `--debug`: Prints debug messages when set.
* `--save-sql`: This flag saves all queries as SQL files instead of running them.
* `--disable-cleanup`: This will skip the local temporary file cleanup process
* `--look-back-days`: The number of days to look back for account usage information. Default is 15 days.
* `--custom-sql`: custom sql file to run instead of default sql, the file name must be either upload_data.sql or download_data.sql and comma separated if both are used

**If any of the required arguments are missing, you will be prompted to enter them.** 

The script will also replace `-` with `_` for the value of `--stage` argument.

## Usage Example
Download the latest release for your platform from the release page
https://github.com/unraveldata-org/snowflake-data-loader/releases

```shell
# Mac may prompt you to trust the binary to run
# “snowflake-data-loader” cannot be opened because it is from an unidentified developer.
# If that is the case trust the binary by running the following command
xattr -d com.apple.quarantine  <path_to_the_binary_directory>/snowflake-data-loader

# Command to run the binary please refer to the linux examples below
```

```bash
# Linux login with private keypair
./snowflake-data-loader \
--source_login_method keypair \
--target_login_method password \
--source_user <source_user> \
--private_key_path <private_key_path> \
--source_account <source_account> \
--source_warehouse <source_warehouse> \
--source_database <source_database> \
--source_schema <source_schema> \
--source_role <source_role> \
--target_user <target_user> \
--target_password <target_password> \
--target_account <target_account> \
--target_warehouse <target_warehouse> \
--target_database <target_database> \
--target_schema <target_schema> \
--target_role <target_role>
```

```powershell
# Windows login with password
snowflake-data-loader.exe \
--source_user <source_user> \
--source_password <source_password> \
--source_account <source_account> \
--source_warehouse <source_warehouse> \
--source_database <source_database> \
--source_schema <source_schema> \
--source_role <source_role> \
--target_user <target_user> \
--target_password <target_password> \
--target_account <target_account> \
--target_warehouse <target_warehouse> \
--target_database <target_database> \
--target_schema <target_schema>
```

```shell
# Print sql queries to files instead of running them
# download_data.sql and upload_data.sql will be created in the output directory
./snowflake-data-loader \
--save-sql \
--source_warehouse <source_warehouse> \
--target_warehouse <target_warehouse>
```

```shell
# Run the command in docker
# Build the docker image
docker build -t snowflake-data-loader .
# Run directly from docker
docker run -it --rm snowflake-data-loader \
--source_user <source_user> \
...

```shell
# By providing existing user with enough permissions, the script can create a new user, schema, table in the target account with minimal permissions
# with the flag "--create" or "--actions create"
./snowflake-data-loader \
--actions create \
--target_new_user <target_new_user> \
--target_new_role <target_new_role> \
--target_user <target_user> \
--target_password <target_password> \
--target_account <target_account> \
--target_warehouse <target_warehouse> \
--target_database <target_database> \
--target_schema <target_schema>
```
```
