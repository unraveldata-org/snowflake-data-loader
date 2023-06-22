package main

import (
	"database/sql"
	"fmt"
	"log"

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
	for rows.Next() {
		count++
	}
	return count
}

func (s *SnowflakeDBClient) GetWarehouses() []string {
	rows, err := s.Query("show warehouses")
	if err != nil {
		log.Printf("Error running show warehouses: %s\n", err)
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
			log.Printf("Error scanning row: %s\n", err)
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
		log.Printf("Error running show parameters for warehouse %s: %s\n", warehouse, err)
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
			log.Printf("Error scanning row: %s\n", err)
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
			row[i] = new(string)
		}
		err := rows.Scan(row...)
		if err != nil {
			log.Printf("Error scanning row: %s\n", err)
			continue
		}
		results = append(results, toStringSlice(row))
	}
	return results
}

// NewSnowflakeClient creates a new SnowflakeDBClient
func NewSnowflakeClient(user, password, account, warehouse, database, schema, role, passcode, privateKeyPath string) (*SnowflakeDBClient, error) {
	config := gosnowflake.Config{
		Account:   account,
		User:      user,
		Role:      role,
		Database:  database,
		Schema:    schema,
		Warehouse: warehouse,
	}
	if passcode != "" {
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
