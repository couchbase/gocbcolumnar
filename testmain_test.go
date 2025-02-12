package cbcolumnar_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10"

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

	var scope = envFlagString("CBSCOPE", "scope", "test",
		"The scope to use to authenticate when using a real server")

	flag.Parse()

	if *connStr == "" {
		panic("connstr cannot be empty")
	}

	TestOpts.OriginalConnStr = *connStr
	TestOpts.Username = *user
	TestOpts.Password = *password
	TestOpts.Database = *database
	TestOpts.Scope = *scope

	gocbcore.SetLogger(gocbcore.VerboseStdioLogger())

	leakcheck.EnableAll()

	setupColumnar()

	result := m.Run()

	if !leakcheck.ReportAll() {
		result = 1
	}

	os.Exit(result)
}

func setupColumnar() {
	cluster, err := cbcolumnar.NewCluster(TestOpts.OriginalConnStr, cbcolumnar.NewCredential(TestOpts.Username, TestOpts.Password), DefaultOptions())
	if err != nil {
		panic(err)
	}
	defer cluster.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err = cluster.ExecuteQuery(ctx, fmt.Sprintf("CREATE DATABASE `%s` IF NOT EXISTS", TestOpts.Database))
	if err != nil {
		panic(err)
	}

	_, err = cluster.ExecuteQuery(ctx, fmt.Sprintf("CREATE SCOPE `%s`.`%s` IF NOT EXISTS", TestOpts.Database, TestOpts.Scope))
	if err != nil {
		panic(err)
	}
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
