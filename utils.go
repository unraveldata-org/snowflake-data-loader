package main

import (
	"bufio"
	"crypto/rsa"
	"crypto/x509"
	"encoding/csv"
	"encoding/pem"
	"fmt"
	"log"
	"os"
	"syscall"

	"golang.org/x/term"
)

func toStringSlice(row []any) []string {
	s := make([]string, len(row))
	for i := range row {
		s[i] = fmt.Sprintf("%s", *row[i].(*string))
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

func promptInput(promptStr string, input *string) {
	fmt.Print(promptStr)
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		*input = scanner.Text()
	}
}

func promptSecureInput(promptStr string, input *string) {
	fmt.Print(promptStr)
	result, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		panic(err)
	}
	fmt.Print("\n")
	*input = string(result)
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
