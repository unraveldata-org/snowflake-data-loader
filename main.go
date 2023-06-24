package main

import (
	"bytes"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"strings"

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
func downloadData(sfClient *SnowflakeDBClient, args Args) {
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
		}
		if args.Debug && rows != nil {
			sfClient.DebugPrint(rows)
		}
		log.Infof("%d rows affected", sfClient.RowAffected(rows))
	}
}

// getWarehouseParameters gets warehouse parameters from source Snowflake account and save to csv file
// The format for the csv file is as follows:
// warehouse_name,parameter_name,parameter_values...
func getWarehouseParameters(sfClient *SnowflakeDBClient, args Args) {
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

	rows, err := sfClient.Query("show warehouses")
	if err != nil {
		log.Infof("Error running show warehouses: %s", err)
	}
	wsMap := sfClient.ResultToMap(rows, true)
	if args.Debug {
		sfClient.DebugPrint(rows)
	}
	saveToCsv(filepath.Join(args.Out, wsCsvFileName), wsMap)

	warehouseParameters := sfClient.GetAllWarehouseParameters()
	saveToCsv(filepath.Join(args.Out, whParamCsvFileName), warehouseParameters)
}

// uploadData uploads data to target Snowflake account
func uploadData(sfClient *SnowflakeDBClient, args Args) {
	printSeparator()
	log.Infof("Uploading data to target account %s", args.TgtAccount)
	uploadSqlScript := &bytes.Buffer{}
	err := template.Must(template.New("").Parse(uploadSqlScriptTemplate)).Execute(uploadSqlScript, args)
	if err != nil {
		log.Info(err)
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
	queries := strings.Split(uploadSqlScript.String(), "\n")
	for _, query := range queries {
		if query == "" {
			continue
		}
		log.Infof("Running query: %s", query)
		rows, err := sfClient.Query(query)
		if err != nil {
			log.Infof("Error running query: %s", query)
		}
		if args.Debug && rows != nil {
			sfClient.DebugPrint(rows)
		}
		log.Infof("%d rows affected", sfClient.RowAffected(rows))
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
	srcClient, err := NewSnowflakeClient(
		args.SrcLoginMethod, args.SrcUser, args.SrcPassword, args.SrcAccount, args.SrcWarehouse, args.SrcDatabase,
		args.SrcSchema, args.SrcRole, args.SrcPasscode, srcPrivateKeyPath, args.SrcPrivateLink, args.SrcOktaURL,
	)

	// Create target account Snowflake client
	switch args.TgtLoginMethod {
	case "keypair":
		tgtPrivateKeyPath = args.PrivateKeyPath
	}
	tgtClient, err1 := NewSnowflakeClient(
		args.TgtLoginMethod, args.TgtUser, args.TgtPassword, args.TgtAccount, args.TgtWarehouse, args.TgtDatabase,
		args.TgtSchema, args.TgtRole, args.TgtPasscode, tgtPrivateKeyPath, args.TgtPrivateLink, args.TgtOktaURL,
	)
	if err != nil || err1 != nil {
		if !args.SaveSql {
			log.Fatal(err, err1)
		}
	}
	// Clean up temporary files after complete
	if !args.DisableCleanup {
		defer cleanUp(args)
	}
	downloadData(srcClient, args)
	getWarehouseParameters(srcClient, args)
	uploadData(tgtClient, args)
}
