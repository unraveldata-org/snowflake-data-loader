package main

import (
	"bytes"
	"fmt"
	"html/template"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var (
	version            = "dev"
	date               = "unknown"
	whParamCsvFileName = "warehouse_parameters.csv"
	wsCsvFileName      = "warehouses.csv"
)

func printSeparator() {
	log.Println("------------------------------------------------------------")
}

// downloadData downloads data from source Snowflake account
// Access to SNOWFLAKE.ACCOUNT_USAGE is required
// Permission to create a stage in the source account is required
func downloadData(sfClient *SnowflakeDBClient, args Args) {
	printSeparator()
	log.Printf("Downloading data from source account %s\n", args.SrcAccount)
	downloadSqlScript := &bytes.Buffer{}
	err := template.Must(template.New("").Parse(downloadSqlScriptTemplate)).Execute(downloadSqlScript, args)
	if err != nil {
		log.Println(err)
	}
	if args.SaveSql {
		savePath := filepath.Join(args.Out, "download_data.sql")
		log.Printf("Saving sql script to file %s\n", savePath)
		err = os.WriteFile(savePath, downloadSqlScript.Bytes(), 0644)
		if err != nil {
			log.Printf("Error saving sql script to file %s: %s\n", savePath, err)
		}
		return
	}
	queries := strings.Split(downloadSqlScript.String(), "\n")
	for _, query := range queries {
		if query == "" {
			continue
		}
		log.Printf("Running query: %s\n", query)
		rows, err := sfClient.Query(query)
		if err != nil {
			log.Fatalf(fmt.Sprintf("Error running query: %s", query))
		}
		log.Printf("%d rows affected", sfClient.RowAffected(rows))
	}
}

// getWarehouseParameters gets warehouse parameters from source Snowflake account and save to csv file
// The format for the csv file is as follows:
// warehouse_name,parameter_name,parameter_values...
func getWarehouseParameters(sfClient *SnowflakeDBClient, args Args) {
	printSeparator()
	log.Println("Getting warehouse parameters from source account")
	if args.SaveSql {
		sPath := filepath.Join(args.Out, whParamCsvFileName)
		log.Printf(
			"Please export the result of 'show warehouses' from the source account and save as a CSV file %s manually",
			sPath,
		)
		return
	}

	rows, err := sfClient.Query("show warehouses")
	if err != nil {
		log.Printf("Error running show warehouses: %s\n", err)
	}
	wsMap := sfClient.ResultToMap(rows, true)
	saveToCsv(filepath.Join(args.Out, wsCsvFileName), wsMap)

	warehouseParameters := sfClient.GetAllWarehouseParameters()
	saveToCsv(filepath.Join(args.Out, whParamCsvFileName), warehouseParameters)
}

// uploadData uploads data to target Snowflake account
func uploadData(sfClient *SnowflakeDBClient, args Args) {
	printSeparator()
	log.Printf("Uploading data to target account %s\n", args.TgtAccount)
	uploadSqlScript := &bytes.Buffer{}
	err := template.Must(template.New("").Parse(uploadSqlScriptTemplate)).Execute(uploadSqlScript, args)
	if err != nil {
		log.Println(err)
	}
	if args.SaveSql {
		savePath := filepath.Join(args.Out, "upload_data.sql")
		log.Printf("Saving sql script to file %s\n", savePath)
		err = os.WriteFile(savePath, uploadSqlScript.Bytes(), 0644)
		if err != nil {
			log.Printf("Error saving sql script to file %s: %s\n", savePath, err)
		}
		return
	}
	queries := strings.Split(uploadSqlScript.String(), "\n")
	for _, query := range queries {
		if query == "" {
			continue
		}
		log.Printf("Running query: %s\n", query)
		rows, err := sfClient.Query(query)
		if err != nil {
			log.Println(fmt.Sprintf("Error running query: %s", query))
		}
		log.Printf("%d rows affected", sfClient.RowAffected(rows))
	}
}

// cleanUp removes all temporary files download by this tool
func cleanUp(args Args) {
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
	for _, f := range cleanUPCandidates {
		fs, err := filepath.Glob(filepath.Join(args.Out, f))
		if err != nil {
			log.Printf("Error getting file list: %s\n", err)
		}
		for _, f := range fs {
			log.Printf("Removing file %s\n", f)
			err := os.Remove(f)
			if err != nil {
				log.Printf("Error removing file %s: %s\n", f, err)
			} else {
				log.Printf("Removed file %s\n", f)
			}
		}
	}
}

func main() {
	log.Printf("Snowflake Warehouse Migration Tool %s; built at %s\n", version, date)
	args := getArgs()
	srcClient, err := NewSnowflakeClient(args.SrcUser, args.SrcPassword, args.SrcAccount, args.SrcWarehouse, args.SrcDatabase, args.SrcSchema, args.SrcRole, args.SrcPasscode, args.PrivateKeyPath)
	tgtClient, err1 := NewSnowflakeClient(args.TgtUser, args.TgtPassword, args.TgtAccount, args.TgtWarehouse, args.TgtDatabase, args.TgtSchema, args.TgtRole, args.TgtPasscode, "")
	if err != nil || err1 != nil {
		if !args.SaveSql {
			log.Fatal(err, err1)
		}
	}
	if !args.DisableCleanup {
		defer cleanUp(args)
	}
	downloadData(srcClient, args)
	getWarehouseParameters(srcClient, args)
	uploadData(tgtClient, args)
}
