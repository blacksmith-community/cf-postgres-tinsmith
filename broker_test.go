package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jhunt/vcaptive"
	"github.com/lib/pq"
)

const vcapServicesDbCredsJson = `{
	"aws-rds": [{
		"credentials": {
			"%s": "%s"
		},
		"tags": ["postgresql"]
	}]
}`

func setVcapServicesEnv(credentialKey string, credentialValue string) {
	os.Setenv("VCAP_SERVICES", fmt.Sprintf(vcapServicesDbCredsJson, credentialKey, credentialValue))
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

func TestCreateBrokerDatabase(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	broker := &Broker{
		db: db,
	}

	mock.ExpectExec("CREATE DATABASE broker").WillReturnResult(sqlmock.NewResult(1, 1))

	dbErr := broker.createBrokerDb()
	if dbErr != nil {
		t.Fatalf(`unexpected error: %s`, dbErr)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestCreateBrokerDatabaseError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	broker := &Broker{
		db: db,
	}

	mock.ExpectExec("CREATE DATABASE broker").WillReturnError(&pq.Error{
		Code: "42P04",
	})

	dbErr := broker.createBrokerDb()
	if dbErr != nil {
		t.Fatalf(`unexpected error: %s`, dbErr)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
