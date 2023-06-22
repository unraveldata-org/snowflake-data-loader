package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"html/template"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var (
	Version     = "dev"
	CsvFileName = "warehouses_parameters.csv"
)

func printSeparator() {
	log.Println("------------------------------------------------------------")
}

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

func getWarehouseParameters(sfClient *SnowflakeDBClient, args Args) {
	printSeparator()
	log.Println("Getting warehouse parameters from source account")
	if args.SaveSql {
		sPath := filepath.Join(args.Out, CsvFileName)
		log.Printf(
			"Please export the result of 'show warehouses' from the source account and save as a CSV file %s manually",
			sPath,
		)
		return
	}
	warehouseParameters := sfClient.GetAllWarehouseParameters()
	// save to csv file
	csvFile, err := os.Create(filepath.Join(args.Out, CsvFileName))
	if err != nil {
		log.Printf("Error creating csv file: %s\n", err)
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()
	for _, wp := range warehouseParameters {
		err := csvWriter.Write(wp)
		if err != nil {
			log.Printf("Error writing csv file: %s\n", err)
		}
	}
}

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

func cleanUp(args Args) {
	cleanUPCandidates := []string{
		CsvFileName,
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
	log.Printf("Snowflake Warehouse Migration Tool %s\n", Version)
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
