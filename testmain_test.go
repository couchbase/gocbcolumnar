package cbcolumnar_test

import (
	"flag"
	"os"
	"testing"

	cbcolumnar "github.com/couchbase/gocbcolumnar"
	"github.com/couchbase/gocbcolumnar/internal/leakcheck"
)

var TestOpts TestOptions

type TestOptions struct {
	Username        string
	Password        string
	OriginalConnStr string
	Database        string
	Scope           string
	Collection      string
}

func TestMain(m *testing.M) {
	var connStr = envFlagString("CBCONNSTR", "connstr", "",
		"Connection string to run tests with")

	var user = envFlagString("CBUSER", "user", "Administrator",
		"The username to use to authenticate when using a real server")

	var password = envFlagString("CBPASS", "pass", "password",
		"The password to use to authenticate when using a real server")

	var database = envFlagString("CBDB", "database", "default",
		"The database to use to authenticate when using a real server")

	var scope = envFlagString("CBSCOPE", "scope", "_default",
		"The scope to use to authenticate when using a real server")

	var collection = envFlagString("CBCOL", "collection", "_default",
		"The collection to use to authenticate when using a real server")

	flag.Parse()

	if *connStr == "" {
		panic("connstr cannot be empty")
	}

	TestOpts.OriginalConnStr = *connStr
	TestOpts.Username = *user
	TestOpts.Password = *password
	TestOpts.Database = *database
	TestOpts.Scope = *scope
	TestOpts.Collection = *collection

	leakcheck.EnableAll()

	result := m.Run()

	if !leakcheck.ReportAll() {
		result = 1
	}

	os.Exit(result)
}

func envFlagString(envName, name, value, usage string) *string {
	envValue := os.Getenv(envName)
	if envValue != "" {
		value = envValue
	}

	return flag.String(name, value, usage)
}

func DefaultOptions() *cbcolumnar.ClusterOptions {
	return cbcolumnar.NewClusterOptions().SetSecurityOptions(cbcolumnar.NewSecurityOptions().SetDisableServerCertificateVerification(true))
}
