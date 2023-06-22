package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"syscall"

	"golang.org/x/term"
)

var (
	allowedLoginMethods = []string{"password", "oauth", "keypair"}
)

type Args struct {
	// arguments for source snowflake account
	SrcLoginMethod string
	SrcUser        string
	SrcPassword    string
	// MFA passcode
	SrcPasscode  string
	SrcAccount   string
	SrcWarehouse string
	SrcDatabase  string
	SrcSchema    string
	SrcRole      string

	// arguments for target snowflake account
	TgtUser     string
	TgtPassword string
	// MFA passcode
	TgtPasscode  string
	TgtAccount   string
	TgtWarehouse string
	TgtDatabase  string
	TgtSchema    string
	TgtRole      string

	// Other arguments
	Stage          string
	Out            string
	FileFormat     string
	Debug          bool
	SaveSql        bool
	DisableCleanup bool
	Actions        []string
	PrivateKeyPath string
}

func getArgs() Args {
	// login method
	srcLoginMethod := flag.String("source_login_method", "password", "source login method: password, oauth, or keypair")
	// arguments for source snowflake account
	srcUser := flag.String("source_user", "", "source Snowflake account username")
	srcPassword := flag.String("source_password", "", "source Snowflake account password")
	srcPasscode := flag.String("source_passcode", "", "source Snowflake account MFA passcode")
	srcAccount := flag.String("source_account", "", "source Snowflake account id")
	srcWarehouse := flag.String("source_warehouse", "", "source warehouse")
	srcDatabase := flag.String("source_database", "", "source database")
	srcSchema := flag.String("source_schema", "", "source schema")
	srcRole := flag.String("source_role", "", "source role")

	// arguments for target snowflake account
	tgtUser := flag.String("target_user", "", "target Snowflake account username")
	tgtPassword := flag.String("target_password", "", "target Snowflake account password")
	tgtPasscode := flag.String("target_passcode", "", "target Snowflake account MFA passcode")
	tgtAccount := flag.String("target_account", "", "target Snowflake account id")
	tgtWarehouse := flag.String("target_warehouse", "", "target warehouse")
	tgtDatabase := flag.String("target_database", "", "target database")
	tgtSchema := flag.String("target_schema", "", "target schema")
	tgtRole := flag.String("target_role", "", "target role")

	// Other arguments
	actions := flag.String("actions", "download,upload", "actions to perform: download, upload")
	stage := flag.String("stage", "unravel_stage", "stage name")
	out := flag.String("out", "", "source output file path default is current directory")
	fileFormat := flag.String("file_format", "unravel_file_format", "source file format name")
	debug := flag.Bool("debug", false, "print debug message")
	saveSql := flag.Bool("save-sql", false, "save all queries as sql file instead of running them")
	disableCleanup := flag.Bool("disable-cleanup", false, "clean up downloaded files")
	privateKeyPath := flag.String("private-key-path", "", "path to private key file for keypair login")
	flag.Parse()

	// prompt for missing args
	if *srcLoginMethod == "" {
		promptInput("Source login method: ", srcLoginMethod)
	} else if !contains(allowedLoginMethods, *srcLoginMethod) {
		log.Fatalf("Invalid source login method: %s must be %v", *srcLoginMethod, allowedLoginMethods)
	}
	if *actions == "" {
		promptInput("Actions to perform: ", actions)
	}
	if *srcAccount == "" && *saveSql == false {
		promptInput("Source Snowflake account ID: ", srcAccount)

	}
	if *srcUser == "" && *saveSql == false {
		promptInput("Source Snowflake account username: ", srcUser)
	}
	if *srcPassword == "" && *saveSql == false && *srcLoginMethod == "password" {
		promptSecureInput("Source password: ", srcPassword)
	}
	if *privateKeyPath == "" && *srcLoginMethod == "keypair" {
		promptInput("Private key path: ", privateKeyPath)
	}
	if *srcDatabase == "" {
		promptInput("Source database: ", srcDatabase)
	}
	if *srcSchema == "" {
		promptInput("Source schema: ", srcSchema)
	}
	if *srcWarehouse == "" {
		promptInput("Source warehouse: ", srcWarehouse)
	}
	if *srcRole == "" {
		promptInput("Source role: ", srcRole)
	}

	if *tgtAccount == "" && *saveSql == false {
		promptInput("Target Snowflake account ID: ", tgtAccount)
	}
	if *tgtUser == "" && *saveSql == false {
		promptInput("Target Snowflake account username: ", tgtUser)
	}
	if *tgtPassword == "" && *saveSql == false {
		promptSecureInput("Target password: ", tgtPassword)
	}
	if *tgtDatabase == "" {
		promptInput("Target database: ", tgtDatabase)
	}
	if *tgtSchema == "" {
		promptInput("Target schema: ", tgtSchema)
	}
	if *tgtWarehouse == "" {
		promptInput("Target warehouse: ", tgtWarehouse)
	}
	if *tgtRole == "" {
		promptInput("Target role: ", tgtRole)
	}
	if *out == "" {
		*out, _ = os.Getwd()
	}
	return Args{
		SrcLoginMethod: *srcLoginMethod,
		SrcUser:        *srcUser,
		SrcPassword:    *srcPassword,
		SrcPasscode:    *srcPasscode,
		SrcAccount:     *srcAccount,
		SrcWarehouse:   *srcWarehouse,
		SrcDatabase:    *srcDatabase,
		SrcSchema:      *srcSchema,
		SrcRole:        *srcRole,
		TgtUser:        *tgtUser,
		TgtPassword:    *tgtPassword,
		TgtPasscode:    *tgtPasscode,
		TgtAccount:     *tgtAccount,
		TgtWarehouse:   *tgtWarehouse,
		TgtDatabase:    *tgtDatabase,
		TgtSchema:      *tgtSchema,
		TgtRole:        *tgtRole,
		Stage:          *stage,
		Out:            *out,
		FileFormat:     *fileFormat,
		Debug:          *debug,
		SaveSql:        *saveSql,
		DisableCleanup: *disableCleanup,
		Actions:        strings.Split(*actions, ","),
		PrivateKeyPath: *privateKeyPath,
	}
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
