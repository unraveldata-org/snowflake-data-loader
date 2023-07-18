package main

import (
	"flag"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
)

var (
	allowedLoginMethods = []string{"password", "oauth", "keypair", "sso", "okta"}
)

type Args struct {
	// arguments for source snowflake account
	SrcLoginMethod string
	SrcUser        string
	SrcPassword    string
	// MFA passcode
	SrcPasscode    string
	SrcAccount     string
	SrcWarehouse   string
	SrcDatabase    string
	SrcSchema      string
	SrcRole        string
	SrcPrivateLink string
	SrcOktaURL     string

	// arguments for target snowflake account
	TgtLoginMethod string
	TgtUser        string
	TgtPassword    string
	// MFA passcode
	TgtPasscode    string
	TgtAccount     string
	TgtWarehouse   string
	TgtDatabase    string
	TgtSchema      string
	TgtRole        string
	TgtPrivateLink string
	TgtOktaURL     string
	TgtNewUser     string
	TgtNewUserPass string
	TgtNewRole     string

	// Other arguments
	Stage          string
	Out            string
	FileFormat     string
	Debug          bool
	SaveSql        bool
	DisableCleanup bool
	Actions        []string
	create         bool
	PrivateKeyPath string
	LookBackDays   uint
}

func getArgs() *Args {
	// login method
	srcLoginMethod := flag.String("source_login_method", "password", "source login method: password, oauth, keypair, okta, or sso")
	tgtLoginMethod := flag.String("target_login_method", "password", "target login method: password, oauth, keypair, okta or sso")
	// arguments for source snowflake account
	srcUser := flag.String("source_user", "", "source Snowflake account username")
	srcPassword := flag.String("source_password", "", "source Snowflake account password/oauth token")
	srcPasscode := flag.String("source_passcode", "", "source Snowflake account MFA passcode")
	srcAccount := flag.String("source_account", "", "source Snowflake account id")
	srcWarehouse := flag.String("source_warehouse", "", "source warehouse")
	srcDatabase := flag.String("source_database", "", "source database")
	srcSchema := flag.String("source_schema", "", "source schema")
	srcRole := flag.String("source_role", "", "source role")
	srcPrivateLink := flag.String("source_private_link", "", "source account private link")
	srcOktaURL := flag.String("source_okta_url", "", "source account okta url")

	// arguments for target snowflake account
	tgtUser := flag.String("target_user", "", "target Snowflake account username")
	tgtPassword := flag.String("target_password", "", "target Snowflake account password/oauth token")
	tgtPasscode := flag.String("target_passcode", "", "target Snowflake account MFA passcode")
	tgtAccount := flag.String("target_account", "", "target Snowflake account id")
	tgtWarehouse := flag.String("target_warehouse", "", "target warehouse")
	tgtDatabase := flag.String("target_database", "", "target database")
	tgtSchema := flag.String("target_schema", "", "target schema")
	tgtRole := flag.String("target_role", "", "target role")
	tgtPrivateLink := flag.String("target_private_link", "", "target account private link")
	tgtOktaURL := flag.String("target_okta_url", "", "target account okta url")
	tgtNewUser := flag.String("target_new_user", "", "target account new user name to be created")
	tgtNewUserPass := flag.String("target_new_user_pass", "", "target account new user password to be created")
	tgtNewRole := flag.String("target_new_role", "", "target account new role to be created")

	// Other arguments
	actions := flag.String("actions", "download,upload", "actions to perform: download, upload, create")
	create := flag.Bool("create", false, "create snowflake objects (user, role, schema, and grant minimal permissions) in target account to be used by Unravel")
	stage := flag.String("stage", "unravel_stage", "stage name")
	out := flag.String("out", "", "source output directory path default is current directory")
	fileFormat := flag.String("file_format", "unravel_file_format", "source file format name")
	debug := flag.Bool("debug", false, "print debug message")
	saveSql := flag.Bool("save-sql", false, "save all queries as sql file instead of running them")
	disableCleanup := flag.Bool("disable-cleanup", false, "clean up downloaded files")
	privateKeyPath := flag.String("private-key-path", "", "path to private key file for keypair login for both source and target accounts if login method is keypair")
	lookBackDays := flag.Uint("look-back-days", 15, "number of days to look back for data to download to download all data set it to 0")
	flag.Parse()

	args := Args{
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
		SrcPrivateLink: *srcPrivateLink,
		SrcOktaURL:     *srcOktaURL,
		TgtUser:        *tgtUser,
		TgtPassword:    *tgtPassword,
		TgtPasscode:    *tgtPasscode,
		TgtAccount:     *tgtAccount,
		TgtWarehouse:   *tgtWarehouse,
		TgtDatabase:    *tgtDatabase,
		TgtSchema:      *tgtSchema,
		TgtRole:        *tgtRole,
		TgtPrivateLink: *tgtPrivateLink,
		TgtOktaURL:     *tgtOktaURL,
		Stage:          *stage,
		Out:            *out,
		FileFormat:     *fileFormat,
		Debug:          *debug,
		SaveSql:        *saveSql,
		DisableCleanup: *disableCleanup,
		Actions:        strings.Split(*actions, ","),
		create:         *create,
		PrivateKeyPath: *privateKeyPath,
		LookBackDays:   *lookBackDays,
		TgtNewUser:     *tgtNewUser,
		TgtNewUserPass: *tgtNewUserPass,
		TgtNewRole:     *tgtNewRole,
	}
	argsCheck(&args)
	return &args
}

func argsCheck(args *Args) {
	// prompt for missing args
	if args.SrcLoginMethod == "" {
		args.SrcLoginMethod = promptInput("Source login method: ")
	} else if !contains(allowedLoginMethods, args.SrcLoginMethod) {
		log.Fatalf("Invalid source login method: %s must be %v", args.SrcLoginMethod, allowedLoginMethods)
	} else if args.SrcLoginMethod == "okta" && args.SrcOktaURL == "" {
		args.SrcOktaURL = promptInput("Source account okta url: ")
	}

	if args.TgtLoginMethod == "" {
		args.TgtLoginMethod = promptInput("Target login method: ")
	} else if !contains(allowedLoginMethods, args.TgtLoginMethod) {
		log.Fatalf("Invalid target login method: %s must be %v", args.TgtLoginMethod, allowedLoginMethods)
	} else if args.TgtLoginMethod == "okta" && args.TgtOktaURL == "" {
		args.TgtOktaURL = promptInput("Target account okta url: ")
	}
	if len(args.Actions) == 0 {
		args.Actions = strings.Split(promptInput("Actions to perform: "), ",")
	}
	if args.create {
		args.Actions = append(args.Actions, "create")
	}
	if contains(args.Actions, "download") {
		if args.SrcAccount == "" && args.SaveSql == false {
			args.SrcAccount = promptInput("Source Snowflake account ID: ")

		}
		if args.SrcUser == "" && args.SaveSql == false {
			args.SrcUser = promptInput("Source Snowflake account username: ")
		}
		if args.SrcPassword == "" && args.SaveSql == false && (args.SrcLoginMethod == "password" || args.SrcLoginMethod == "oauth") {
			args.SrcPassword = promptSecureInput("Source password/oauth token: ")
		}
		if args.PrivateKeyPath == "" && (args.SrcLoginMethod == "keypair" || args.TgtLoginMethod == "keypair") && args.SaveSql == false {
			args.PrivateKeyPath = promptInput("Private key path: ")
		}
		if args.SrcDatabase == "" && args.SaveSql == false {
			args.SrcDatabase = promptInput("Source database: ")
		}
		if args.SrcSchema == "" && args.SaveSql == false {
			args.SrcSchema = promptInput("Source schema: ")
		}
		if args.SrcWarehouse == "" {
			args.SrcWarehouse = promptInput("Source warehouse: ")
		}
		if args.SrcRole == "" && args.SaveSql == false {
			args.SrcRole = promptInput("Source role: ")
		}
	}

	if contains(args.Actions, "upload") || contains(args.Actions, "create") {
		if args.TgtAccount == "" && args.SaveSql == false {
			args.TgtAccount = promptInput("Target Snowflake account ID: ")
		}
		if args.TgtUser == "" && args.SaveSql == false {
			args.TgtUser = promptInput("Target Snowflake account username: ")
		}
		if args.TgtPassword == "" && args.SaveSql == false && (args.TgtLoginMethod == "password" || args.TgtLoginMethod == "oauth") {
			args.TgtPassword = promptSecureInput("Target password/oauth token: ")
		}
		if args.TgtDatabase == "" && args.SaveSql == false {
			args.TgtDatabase = promptInput("Target database: ")
		}
		if args.TgtSchema == "" && args.SaveSql == false {
			args.TgtSchema = promptInput("Target schema: ")
		}
		if args.TgtWarehouse == "" {
			args.TgtWarehouse = promptInput("Target warehouse: ")
		}
		if args.TgtRole == "" && args.SaveSql == false {
			args.TgtRole = promptInput("Target role: ")
		}
	}

	if args.Out == "" {
		args.Out, _ = os.Getwd()
	} else {
		// ensure output directory exists and is directory
		stat, err := os.Stat(args.Out)
		if os.IsNotExist(err) {
			log.Fatalf("Output directory %s does not exist", args.Out)
		}
		if !stat.IsDir() {
			log.Fatalf("%s is not a directory", args.Out)
		}
	}
	if args.LookBackDays == 0 {
		args.LookBackDays = 365
	}
	if args.Debug {
		log.SetLevel(log.DebugLevel)
	}
}
