package main

import (
	"database/sql"
	"fmt"
	"net/url"

	"github.com/jedib0t/go-pretty/v6/table"
	log "github.com/sirupsen/logrus"
	"github.com/snowflakedb/gosnowflake"
	_ "github.com/snowflakedb/gosnowflake"
)

type SnowflakeDBClient struct {
	Db *sql.DB
}

func (s *SnowflakeDBClient) Close() error {
	return s.Db.Close()
}

func (s *SnowflakeDBClient) Query(query string) (*sql.Rows, error) {
	return s.Db.Query(query)
}

func (s *SnowflakeDBClient) RowAffected(rows *sql.Rows) int64 {
	count := int64(0)
	if rows == nil {
		return count
	}
	for rows.Next() {
		count++
	}
	return count
}

func (s *SnowflakeDBClient) GetWarehouses() []string {
	rows, err := s.Query("show warehouses")
	if err != nil {
		log.Errorf("Error running show warehouses: %s\n", err)
		return []string{}
	}
	var warehouses []string
	cols, _ := rows.Columns()
	for rows.Next() {
		vals := make([]any, len(cols))
		for i := range cols {
			vals[i] = new(string)
		}
		err = rows.Scan(vals...)
		if err != nil {
			log.Errorf("Error scanning row: %s\n", err)
			continue
		}
		warehouses = append(warehouses, fmt.Sprintf("%s", *vals[0].(*string)))
	}
	return warehouses
}

func (s *SnowflakeDBClient) GetAllWarehouseParameters() (warehousesParameters [][]string) {
	ws := s.GetWarehouses()
	for _, w := range ws {
		warehousesParameters = append(warehousesParameters, s.GetWarehouseParameters(w)...)
	}
	return warehousesParameters
}

func (s *SnowflakeDBClient) GetWarehouseParameters(warehouse string) (warehousesParameters [][]string) {
	rows, err := s.Query(fmt.Sprintf("show parameters for warehouse %s", warehouse))
	if err != nil {
		log.Errorf("Error running show parameters for warehouse %s: %s\n", warehouse, err)
		return [][]string{}
	}
	cols, _ := rows.Columns()
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		for i := range cols {
			vals[i] = new(interface{})
		}
		err = rows.Scan(vals...)
		if err != nil {
			log.Errorf("Error scanning row: %s\n", err)
			continue
		}
		warehousesParameter := []string{warehouse}

		for _, v := range vals {
			warehousesParameter = append(warehousesParameter, fmt.Sprintf("%v", *v.(*interface{})))
		}
		warehousesParameters = append(warehousesParameters, warehousesParameter)
	}
	return warehousesParameters
}

func (s *SnowflakeDBClient) ResultToMap(rows *sql.Rows, includeHeader bool) (results [][]string) {
	cols, _ := rows.Columns()
	if includeHeader {
		results = append(results, cols)
	}
	for rows.Next() {
		row := make([]any, len(cols))
		for i := range cols {
			row[i] = new(any)
		}
		err := rows.Scan(row...)
		if err != nil {
			log.Errorf("Error scanning row: %s\n", err)
			continue
		}
		results = append(results, toStringSlice(row))
	}
	return results
}

// DebugPrint prints the results of a query in a table format
func (s *SnowflakeDBClient) DebugPrint(rows *sql.Rows) {
	results := s.ResultToMap(rows, true)
	t := table.NewWriter()
	for i, row := range results {
		// create table header
		if i == 0 {
			r := make([]interface{}, len(row))
			for j, v := range row {
				r[j] = v
			}
			t.AppendHeader(r)
			continue
		}
		// create table row
		r := make([]interface{}, len(row))
		for j, v := range row {
			r[j] = v
		}
		t.AppendRow(r)
	}
	// print table row
	log.Debugf("\n%s", t.Render())
}

// NewSnowflakeClient creates a new SnowflakeDBClient
func NewSnowflakeClient(logInMethod, user, password, account, warehouse, database, schema, role, passcode, privateKeyPath, privateLink, oktaUrl string) (*SnowflakeDBClient, error) {
	config := gosnowflake.Config{
		Account:      account,
		User:         user,
		Role:         role,
		Database:     database,
		Schema:       schema,
		Warehouse:    warehouse,
		LoginTimeout: 120,
	}
	if passcode != "" {
		config.Authenticator = gosnowflake.AuthTypeUsernamePasswordMFA
		config.Passcode = passcode
	}
	if privateKeyPath != "" {
		// https://docs.snowflake.com/en/user-guide/go-driver-use.html#using-jwt-authentication
		pKey := parsePrivateKeyFile(privateKeyPath)
		// base64 encode the private key
		config.PrivateKey = pKey
		config.Authenticator = gosnowflake.AuthTypeJwt
	} else {
		config.Password = password
	}
	if logInMethod == "oauth" {
		config.Authenticator = gosnowflake.AuthTypeOAuth
		config.Token = password
	} else if logInMethod == "sso" {
		config.Authenticator = gosnowflake.AuthTypeExternalBrowser
	} else if logInMethod == "okta" {
		config.Authenticator = gosnowflake.AuthTypeOkta
		config.OktaURL, _ = url.Parse(oktaUrl)
	}
	if privateLink != "" {
		config.Host = privateLink
	}
	dsn, err := gosnowflake.DSN(&config)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, err
	}
	return &SnowflakeDBClient{Db: db}, nil
}
