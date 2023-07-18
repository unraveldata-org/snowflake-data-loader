package main

import (
	_ "embed"
)

//go:embed download_data.sql.template
var downloadSqlScriptTemplate string

//go:embed upload_data.sql.template
var uploadSqlScriptTemplate string

//go:embed create_schema_procedure.sql.template
var createSchemaProcedureSqlScriptTemplate string
