package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	log "github.com/sirupsen/logrus"
)

var (
	version            = "dev"
	date               = "unknown"
	whParamCsvFileName = "warehouse_parameters.csv"
	wsCsvFileName      = "warehouses.csv"
)

func init() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func printSeparator() {
	log.Info("------------------------------------------------------------")
}

// downloadData downloads data from source Snowflake account
// Access to SNOWFLAKE.ACCOUNT_USAGE is required
// Permission to create a stage in the source account is required
func downloadData(sfClient *SnowflakeDBClient, args *Args) {
	printSeparator()
	log.Printf("Downloading data from source account %s", args.SrcAccount)
	downloadSqlScript := &bytes.Buffer{}
	err := template.Must(template.New("").Parse(downloadSqlScriptTemplate)).Execute(downloadSqlScript, args)
	if err != nil {
		log.Info(err)
	}
	if args.SaveSql {
		savePath := filepath.Join(args.Out, "download_data.sql")
		log.Infof("Saving sql script to file %s", savePath)
		err = os.WriteFile(savePath, downloadSqlScript.Bytes(), 0644)
		if err != nil {
			log.Infof("Error saving sql script to file %s: %s", savePath, err)
		}
		return
	}
	queries := strings.Split(downloadSqlScript.String(), "\n")
	for _, query := range queries {
		if query == "" {
			continue
		}
		log.Infof("Running query: %s", query)
		rows, err := sfClient.Query(query)
		if err != nil {
			log.Errorf(fmt.Sprintf("Error running query: %s", query))
		} else {
			log.Infof("%d rows affected", rows.RowAffected())
			rows.Close()
		}
	}
}

// getWarehouseParameters gets warehouse parameters from source Snowflake account and save to csv file
// The format for the csv file is as follows:
// warehouse_name,parameter_name,parameter_values...
func getWarehouseParameters(sfClient *SnowflakeDBClient, args *Args) {
	printSeparator()
	log.Info("Getting warehouse parameters from source account")
	if args.SaveSql {
		sPath := filepath.Join(args.Out, whParamCsvFileName)
		log.Infof(
			"Please export the result of 'show warehouses' from the source account and save as a CSV file %s manually",
			sPath,
		)
		return
	}

	log.Infof("Running query SHOW WAREHOUSES")
	rows, err := sfClient.Query("SHOW WAREHOUSES")
	if err != nil {
		log.Infof("Error running SHOW WAREHOUSES: %s", err)
	}
	defer rows.Close()
	wsMap := rows.ResultToMap(true)
	saveToCsv(filepath.Join(args.Out, wsCsvFileName), wsMap)

	warehouseParameters := sfClient.GetAllWarehouseParameters()
	saveToCsv(filepath.Join(args.Out, whParamCsvFileName), warehouseParameters)
}

// uploadData uploads data to target Snowflake account
func uploadData(sfClient *SnowflakeDBClient, args *Args) {
	printSeparator()
	log.Infof("Uploading data to target account %s", args.TgtAccount)
	uploadSqlScript := &bytes.Buffer{}
	err := template.Must(template.New("").Parse(uploadSqlScriptTemplate)).Execute(uploadSqlScript, args)
	if err != nil {
		log.Error(err)
	}
	if args.SaveSql {
		savePath := filepath.Join(args.Out, "upload_data.sql")
		log.Infof("Saving sql script to file %s", savePath)
		err = os.WriteFile(savePath, uploadSqlScript.Bytes(), 0644)
		if err != nil {
			log.Infof("Error saving sql script to file %s: %s", savePath, err)
		}
		return
	}
	_ = sfClient.UseSchema(args.TgtSchema)
	queries := strings.Split(uploadSqlScript.String(), "\n")
	for _, query := range queries {
		if query == "" {
			continue
		}
		log.Infof("Running query: %s", query)
		rows, err := sfClient.Query(query)
		if err != nil {
			log.Infof("Error running query: %s", query)
		} else {
			log.Infof("%d rows affected", rows.RowAffected())
			rows.Close()
		}
	}
}

func createResources(sfClient *SnowflakeDBClient, args *Args) {
	printSeparator()
	log.Infof("Creating resources in target account %s", args.TgtAccount)
	if args.TgtNewRole == "" {
		args.TgtNewRole = fmt.Sprintf("UNRAVEL_%s", generateStr(5, false, true, true, false))
	}
	log.Infof("Creating role %s", args.TgtNewRole)
	_ = sfClient.CreateRole(args.TgtNewRole)

	log.Infof("Creating schema %s and table in database %s", args.TgtSchema, args.TgtDatabase)
	_ = sfClient.CreateSchemaTable(args.TgtDatabase, args.TgtSchema)

	log.Infof("Granting Database USAGE permission to role %s", args.TgtNewRole)
	_ = sfClient.GrantDatabasePermissions(args.TgtDatabase, args.TgtNewRole, "USAGE")

	log.Infof("Granting Schema USAGE, CREATE FILE FORMAT, CREATE STAGE permissions to role %s", args.TgtNewRole)
	_ = sfClient.GrantSchemaPermissions(args.TgtSchema, args.TgtNewRole, "USAGE", "CREATE FILE FORMAT", "CREATE STAGE")

	log.Infof("Granting Warehouse USAGE permission to role %s", args.TgtNewRole)
	_ = sfClient.GrantWarehousePermissions(args.TgtWarehouse, args.TgtNewRole, "USAGE")

	log.Infof("Granting all tables read write on schema %s to role %s", args.TgtSchema, args.TgtNewRole)
	_ = sfClient.GrantReadWriteAllTables(args.TgtSchema, args.TgtNewRole, "INSERT", "SELECT", "UPDATE", "TRUNCATE")

	if args.TgtNewUser == "" {
		args.TgtNewUser = fmt.Sprintf("%s_USER_%s", args.TgtNewRole, generateStr(5, false, true, false, false))
	}
	if args.TgtNewUserPass == "" {
		args.TgtNewUserPass = generateStr(8, true, true, true, true)

	}
	log.Infof("Creating user %s", args.TgtNewUser)
	_ = sfClient.CreateUser(args.TgtNewUser, args.TgtNewUserPass)

	log.Infof("Granting role %s to user %s", args.TgtNewRole, args.TgtNewUser)
	_ = sfClient.GrantUserRole(args.TgtNewUser, args.TgtNewRole)
}

func printSummary(args *Args) {
	if !contains(args.Actions, "create") {
		return
	}
	if args.TgtNewUser != "" {
		log.Infof("New user: %s", args.TgtNewUser)
		log.Infof("New user password: %s", args.TgtNewUserPass)
	}
	if args.TgtNewRole != "" {
		log.Infof("New role: %s", args.TgtNewRole)
	}
}

// cleanUp removes all temporary files download by this tool
func cleanUp(args *Args) {
	cleanUPCandidates := []string{
		wsCsvFileName,
		whParamCsvFileName,
		"query_history.csv*",
		"warehouse_load_history.csv*",
		"warehouse_events_history.csv*",
		"warehouse_metering_history.csv*",
		"access_history.csv*",
		"metering_history.csv*",
		"metering_daily_history.csv*",
		"tables.csv*",
	}
	count := 0
	for _, f := range cleanUPCandidates {
		fs, err := filepath.Glob(filepath.Join(args.Out, f))
		if err != nil {
			log.Infof("Error getting file list: %s", err)
		}
		for _, f := range fs {
			log.Debugf("Removing file %s", f)
			err := os.Remove(f)
			if err != nil {
				log.Errorf("Error removing file %s: %s", f, err)
			} else {
				log.Debugf("Removed file %s", f)
				count++
			}
		}
	}
	log.Infof("Removed %d files", count)
}

func main() {
	log.Infof("Snowflake Warehouse Migration Tool %s; built at %s", version, date)
	args := getArgs()
	var srcPrivateKeyPath, tgtPrivateKeyPath string

	// Create source account Snowflake client
	switch args.SrcLoginMethod {
	case "keypair":
		srcPrivateKeyPath = args.PrivateKeyPath
	}

	var srcClient, tgtClient *SnowflakeDBClient
	var err, err1 error

	if contains(args.Actions, "download") {
		srcClient, err = NewSnowflakeClient(
			args.SrcLoginMethod, args.SrcUser, args.SrcPassword, args.SrcAccount, args.SrcWarehouse, args.SrcDatabase,
			args.SrcSchema, args.SrcRole, args.SrcPasscode, srcPrivateKeyPath, args.SrcPrivateLink, args.SrcOktaURL,
			args.Debug,
		)
	}

	// Create target account Snowflake client
	switch args.TgtLoginMethod {
	case "keypair":
		tgtPrivateKeyPath = args.PrivateKeyPath
	}
	if contains(args.Actions, "create") || contains(args.Actions, "upload") {
		tgtClient, err1 = NewSnowflakeClient(
			args.TgtLoginMethod, args.TgtUser, args.TgtPassword, args.TgtAccount, args.TgtWarehouse, args.TgtDatabase,
			"", args.TgtRole, args.TgtPasscode, tgtPrivateKeyPath, args.TgtPrivateLink, args.TgtOktaURL,
			args.Debug,
		)
	}
	if err != nil || err1 != nil {
		if !args.SaveSql {
			log.Fatalf("failed to create snowflake client, src account err: %s, target account err: %s", err, err1)
		}
	}

	defer printSummary(args)
	// Clean up temporary files after complete
	if !args.DisableCleanup {
		defer cleanUp(args)
	}

	if contains(args.Actions, "download") {
		downloadData(srcClient, args)
		getWarehouseParameters(srcClient, args)
	}
	if contains(args.Actions, "create") {
		createResources(tgtClient, args)
	}
	if contains(args.Actions, "upload") {
		uploadData(tgtClient, args)
	}
}
