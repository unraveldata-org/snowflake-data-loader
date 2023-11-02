package main

import (
	"bytes"
	"testing"
)

var (
	mockArgs = &Args{
		SrcLoginMethod: "password",
		SrcUser:        "src_user",
		SrcPassword:    "src_password",
		SrcAccount:     "src_account",
		SrcWarehouse:   "src_warehouse",
		SrcDatabase:    "src_database",
		SrcSchema:      "src_schema",
		SrcRole:        "src_role",
		SrcPasscode:    "src_passcode",
		SrcPrivateLink: "src_private_link",
		SrcOktaURL:     "src_okta_url",
		TgtLoginMethod: "password",
		TgtUser:        "tgt_user",
		TgtPassword:    "tgt_password",
		TgtAccount:     "tgt_account",
		TgtWarehouse:   "tgt_warehouse",
		TgtDatabase:    "tgt_database",
		TgtSchema:      "tgt_schema",
		TgtRole:        "tgt_role",
		TgtPasscode:    "tgt_passcode",
		TgtPrivateLink: "tgt_private_link",
		TgtOktaURL:     "tgt_okta_url",
		Debug:          false,
		Stage:          "stage",
		Actions:        []string{"download", "upload"},
		FileFormat:     "unravel_file_format",
	}
)

func Test_parseTemplate(t *testing.T) {
	type args struct {
		templateStr string
		buffer      *bytes.Buffer
		args        *Args
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "Test upload template parsing",
			args: args{
				args:        mockArgs,
				buffer:      bytes.NewBuffer([]byte{}),
				templateStr: uploadSqlScriptTemplate,
			},
			wantErr: false,
		},
		{
			name: "Test download template parsing",
			args: args{
				args:        mockArgs,
				buffer:      bytes.NewBuffer([]byte{}),
				templateStr: downloadSqlScriptTemplate,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := parseTemplate(tt.args.templateStr, tt.args.buffer, tt.args.args); (err != nil) != tt.wantErr {
				t.Errorf("parseTemplate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
