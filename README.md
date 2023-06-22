# snowflake-data-loader

This program enables you to retrieve account usage information from one Snowflake account and upload to a second account. 

The script will perform the following actions:
- Create stage in the source Snowflake account
- Save source Snowflake account usage to stage 
- Download account usage information from the source Snowflake account to local
- Create stage in the target Snowflake account
- Upload the account usage information to the target Snowflake account

## Prerequisites
- Source account user must have the `ACCOUNTADMIN` equivalent permission to access SNOWFLAKE.ACCOUNT_USAGE schema.
- Create stage permission on the source account database and schema.
- Create stage permission on the target account database and schema.
- Insert, Select, Update permission on the target account database and schema.

It requires certain arguments to be passed via the command line, which can also be prompted for in case they are missing. The script then returns the parsed arguments to the calling function.

The following arguments are required:
* `--source_user`: Your source Snowflake account username.
* `--source_password`: Your source Snowflake account password. If source_login_method is `password`, this argument is required.
* `--private_key_path`: The path to your private key file. If source_login_method is `keypair`, this argument is not required.
* `--source_account`: Your source Snowflake account ID.
* `--source_warehouse`: The name of the source warehouse you wish to retrieve details for.
* `--source_database`: The name of the source account database where the stage will be created.
* `--source_schema`: The name of the source account schema where the stage will be created.
* `--source_role`: The name of the source account role.
* `--target_user`: Your target Snowflake account username.
* `--target_password`: Your target Snowflake account password.
* `--target_account`: Your target Snowflake account ID.
* `--target_warehouse`: The name of the target warehouse you wish to retrieve details for.
* `--target_database`: The name of the target database.
* `--target_schema`: The name of the target schema.
* `--target_role`: The name of the target role.

The following arguments are optional:
* `--source_login_method`: The login method for the source account. Possible options are password (default), oauth, or keypair.
* `--source_passcode`: Your source Snowflake account MFA password.
* `--stage`: The name of the stage. Default is `unravel_stage`.
* `--out`: The path to the output file. Default is current directory.
* `--file_format`: The name of the file format. Default is `unravel_file_format`.
* `--debug`: This flag adds debug messages when set.
* `--save-sql`: This flag saves all queries as SQL files instead of running them.

If any of the required arguments are missing, you will be prompted to enter them. 

The script will also replace `-` with `_` for the value of `--stage` argument.

## Usage Example
Download the latest release for your platform from the release page

```bash
# Linux login with private keypair
./snowflake-data-loader-linux-amd64 \
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
snowflake-data-loader-windows-amd64.exe \
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
./snowflake-data-loader-linux-amd64 \
--save-sql \
--source_warehouse <source_warehouse> \
--target_warehouse <target_warehouse> \
```
