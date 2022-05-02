package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/jhunt/vcaptive"
)

const vcapServicesDbCredsJson = `{
	"aws-rds": [{
		"credentials": {
			"%s": "%s"
		},
		"tags": ["postgresql"]
	}]
}`

func setVcapServicesEnv(dbNameKey string, dbName string) {
	os.Setenv("VCAP_SERVICES", fmt.Sprintf(vcapServicesDbCredsJson, dbNameKey, dbName))
}

func TestGetDatabaseName(t *testing.T) {
	testCases := map[string]struct {
		dbNameKey  string
		dbName     string
		expectedOk bool
	}{
		"key is db_name": {
			dbNameKey:  "db_name",
			dbName:     "foobar",
			expectedOk: true,
		},
		"key is name": {
			dbNameKey:  "name",
			dbName:     "foobar2",
			expectedOk: true,
		},
		"key is database": {
			dbNameKey:  "database",
			dbName:     "foobar3",
			expectedOk: true,
		},
		"key is foo": {
			dbNameKey:  "foo",
			dbName:     "",
			expectedOk: false,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			setVcapServicesEnv(test.dbNameKey, test.dbName)

			services, err := vcaptive.ParseServices(os.Getenv("VCAP_SERVICES"))
			if err != nil {
				t.Fatalf(`encountered error: %s`, err)
			}

			instance, found := services.Tagged("postgresql")
			if !found {
				t.Fatal("could not find service")
			}

			value, ok := getDatabaseName(instance)
			if ok != test.expectedOk || value != test.dbName {
				t.Fatalf(`expected getDatabaseName = %q, got: %v`, test.dbName, value)
			}
		})
	}
}
