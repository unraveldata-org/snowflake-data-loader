package main

import (
	"flag"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
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
	TgtLoginMethod string
	TgtUser        string
	TgtPassword    string
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
	LookBackDays   uint
}

func getArgs() Args {
	// login method
	srcLoginMethod := flag.String("source_login_method", "password", "source login method: password, oauth, or keypair")
	tgtLoginMethod := flag.String("target_login_method", "password", "target login method: password, oauth, or keypair")
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
	out := flag.String("out", "", "source output directory path default is current directory")
	fileFormat := flag.String("file_format", "unravel_file_format", "source file format name")
	debug := flag.Bool("debug", false, "print debug message")
	saveSql := flag.Bool("save-sql", false, "save all queries as sql file instead of running them")
	disableCleanup := flag.Bool("disable-cleanup", false, "clean up downloaded files")
	privateKeyPath := flag.String("private-key-path", "", "path to private key file for keypair login for both source and target accounts if login method is keypair")
	lookBackDays := flag.Uint("look-back-days", 15, "number of days to look back for data to download to download all data set it to 0")
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
	if *privateKeyPath == "" && (*srcLoginMethod == "keypair" || *tgtLoginMethod == "keypair") && *saveSql == false {
		promptInput("Private key path: ", privateKeyPath)
	}
	if *srcDatabase == "" && *saveSql == false {
		promptInput("Source database: ", srcDatabase)
	}
	if *srcSchema == "" && *saveSql == false {
		promptInput("Source schema: ", srcSchema)
	}
	if *srcWarehouse == "" {
		promptInput("Source warehouse: ", srcWarehouse)
	}
	if *srcRole == "" && *saveSql == false {
		promptInput("Source role: ", srcRole)
	}

	if *tgtAccount == "" && *saveSql == false {
		promptInput("Target Snowflake account ID: ", tgtAccount)
	}
	if *tgtUser == "" && *saveSql == false {
		promptInput("Target Snowflake account username: ", tgtUser)
	}
	if *tgtPassword == "" && *saveSql == false && *tgtLoginMethod == "password" {
		promptSecureInput("Target password: ", tgtPassword)
	}
	if *tgtDatabase == "" && *saveSql == false {
		promptInput("Target database: ", tgtDatabase)
	}
	if *tgtSchema == "" && *saveSql == false {
		promptInput("Target schema: ", tgtSchema)
	}
	if *tgtWarehouse == "" {
		promptInput("Target warehouse: ", tgtWarehouse)
	}
	if *tgtRole == "" && *saveSql == false {
		promptInput("Target role: ", tgtRole)
	}
	if *out == "" {
		*out, _ = os.Getwd()
	} else {
		// ensure output directory exists and is directory
		stat, err := os.Stat(*out)
		if os.IsNotExist(err) {
			log.Fatalf("Output directory %s does not exist", *out)
		}
		if !stat.IsDir() {
			log.Fatalf("%s is not a directory", *out)
		}
	}
	if *lookBackDays == 0 {
		*lookBackDays = 365
	}
	if *debug {
		log.SetLevel(log.DebugLevel)
	}
	return Args{
		SrcLoginMethod: *srcLoginMethod,
		TgtLoginMethod: *tgtLoginMethod,
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
		LookBackDays:   *lookBackDays,
	}
}
