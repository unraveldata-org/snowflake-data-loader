package main

import (
	"bufio"
	"crypto/rsa"
	"crypto/x509"
	"encoding/csv"
	"encoding/pem"
	"fmt"
	"os"
	"reflect"
	"syscall"

	log "github.com/sirupsen/logrus"
	"golang.org/x/term"
)

func toStringSlice(row []any) []string {
	s := make([]string, len(row))
	for i, v := range row {
		if v == nil {
			s[i] = ""
			continue
		}
		var value string
		switch reflect.TypeOf(v).Kind() {
		case reflect.Ptr:
			value = fmt.Sprintf("%v", *v.(*interface{}))
		case reflect.String:
			value = v.(string)
		case reflect.Int64:
			value = fmt.Sprintf("%d", v.(int64))
		case reflect.Int32:
			value = fmt.Sprintf("%d", v.(int32))
		case reflect.Int:
			value = fmt.Sprintf("%d", v.(int))
		case reflect.Float64:
			value = fmt.Sprintf("%f", v.(float64))
		case reflect.Float32:
			value = fmt.Sprintf("%f", v.(float32))
		case reflect.Bool:
			value = fmt.Sprintf("%t", v.(bool))
		case reflect.SliceOf(reflect.TypeOf(byte(0))).Kind():
			value = string(*v.(*[]byte))
		default:
			log.Errorf("Error converting row to string slice: %v\n", reflect.TypeOf(v).Kind())
			continue
		}
		s[i] = fmt.Sprintf("%s", value)
	}
	return s
}

func parsePrivateKeyFile(privateKeyPath string) *rsa.PrivateKey {
	f, err := os.ReadFile(privateKeyPath)
	if err != nil {
		log.Fatalf("Error reading private key file: %s\n", err)
	}
	block, _ := pem.Decode(f)
	res, _ := x509.ParsePKCS8PrivateKey(block.Bytes)
	return res.(*rsa.PrivateKey)
}

func contains[T comparable](ss []T, s T) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

func promptInput(promptStr string) (input string) {
	fmt.Print(promptStr)
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		input = scanner.Text()
	}
	return input
}

func promptSecureInput(promptStr string) (input string) {
	fmt.Print(promptStr)
	result, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		panic(err)
	}
	fmt.Print("\n")
	input = string(result)
	return input
}

// saveToCsv save map to csv file
func saveToCsv(path string, data [][]string) {
	csvFile, err := os.Create(path)
	if err != nil {
		log.Printf("Error creating csv file: %s\n", err)
	}
	defer csvFile.Close()
	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()
	for _, wp := range data {
		err := csvWriter.Write(wp)
		if err != nil {
			log.Printf("Error writing csv file: %s\n", err)
		}
	}
}
