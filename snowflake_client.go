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

// SnowflakeRows wrapper of sql.rows
type SnowflakeRows struct {
	Rows      *sql.Rows
	debug     bool
	resultMap [][]string
}

func (s *SnowflakeRows) Next() bool {
	return s.Rows.Next()
}

func (s *SnowflakeRows) Scan(dest ...any) error {
	err := s.Rows.Scan(dest...)
	if s.debug && err == nil {
		s.resultMap = append(s.resultMap, toStringSlice(dest))
	}
	return err
}

func (s *SnowflakeRows) ResultToMap(includeHeader bool) (results [][]string) {
	if s.Rows == nil {
		return
	}
	defer s.Rows.Close()
	cols, _ := s.Rows.Columns()
	if includeHeader {
		results = append(results, cols)
	}
	for s.Next() {
		vals := make([]any, len(cols))
		for i := range cols {
			vals[i] = new(any)
		}
		err := s.Scan(vals...)
		if err != nil {
			log.Errorf("Error scanning row: %s\n", err)
			continue
		}
		var row []string
		row = toStringSlice(vals)
		results = append(results, row)
	}
	return results
}

func (s *SnowflakeRows) Close() error {
	if s.debug && s.Rows != nil {
		if len(s.resultMap) <= 1 {
			s.resultMap = s.ResultToMap(true)
		}
		s.debugPrint()
	}
	return s.Rows.Close()
}

func (s *SnowflakeRows) Columns() ([]string, error) {
	return s.Rows.Columns()
}

func (s *SnowflakeRows) RowAffected() int64 {
	r := s.ResultToMap(false)
	count := int64(len(r))
	return count
}

// debugPrint prints the results of a query in a table format
func (s *SnowflakeRows) debugPrint() {
	t := table.NewWriter()
	for i, row := range s.resultMap {
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
	log.Debugf("Row Result\n%s", t.Render())
}

func NewSnowFlakeRows(rows *sql.Rows, debug bool) *SnowflakeRows {
	header, _ := rows.Columns()
	return &SnowflakeRows{
		Rows:      rows,
		debug:     debug,
		resultMap: [][]string{header},
	}
}

type SnowflakeDBClient struct {
	Db    *sql.DB
	debug bool
}

func (s *SnowflakeDBClient) Close() error {
	return s.Db.Close()
}

func (s *SnowflakeDBClient) Query(query string) (*SnowflakeRows, error) {
	rows, err := s.Db.Query(query)
	if err != nil {
		return nil, err
	}
	sr := NewSnowFlakeRows(
		rows,
		s.debug,
	)
	return sr, err
}

func (s *SnowflakeDBClient) GetWarehouses() []string {
	rows, err := s.Query("show warehouses")
	if err != nil {
		log.Errorf("Error running show warehouses: %s\n", err)
		return []string{}
	}
	defer rows.Close()
	var warehouses []string
	cols, _ := rows.Columns()
	for rows.Next() {
		vals := make([]any, len(cols))
		for i := range cols {
			vals[i] = new(any)
		}
		err = rows.Scan(vals...)
		if err != nil {
			log.Errorf("Error scanning row: %s\n", err)
			continue
		}
		warehouses = append(warehouses, fmt.Sprintf("%s", *vals[0].(*any)))
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
	defer rows.Close()
	cols, _ := rows.Columns()
	for rows.Next() {
		vals := make([]any, len(cols))
		for i := range cols {
			vals[i] = new(any)
		}
		err = rows.Scan(vals...)
		if err != nil {
			log.Errorf("Error scanning row: %s\n", err)
			continue
		}
		warehousesParameter := []string{warehouse}

		for _, v := range vals {
			warehousesParameter = append(warehousesParameter, fmt.Sprintf("%v", *v.(*any)))
		}
		warehousesParameters = append(warehousesParameters, warehousesParameter)
	}
	return warehousesParameters
}

// NewSnowflakeClient creates a new SnowflakeDBClient
func NewSnowflakeClient(logInMethod, user, password, account, warehouse, database, schema, role, passcode, privateKeyPath, privateLink, oktaUrl string, debug bool) (*SnowflakeDBClient, error) {
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
	return &SnowflakeDBClient{Db: db, debug: debug}, nil
}
