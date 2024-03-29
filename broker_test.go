package main

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jhunt/vcaptive"
	"github.com/lib/pq"
	"github.com/pivotal-cf/brokerapi"
)

const vcapServicesDbCredsJson = `{
	"aws-rds": [{
		"credentials": {
			"%s": "%s"
		},
		"tags": ["postgresql"]
	}]
}`

const mockDbName string = "fakeDbName"
const usernameRegex string = "u[0-9|a-z]{16}"
const passwordRegex string = "[0-9|a-z]{64}"

type UsernameArgument struct{}

func (u UsernameArgument) Match(value driver.Value) bool {
	stringValue, ok := value.(string)
	if !ok {
		fmt.Fprintf(os.Stderr, "Could not convert value %s to string", value)
		return false
	}
	ok, err := regexp.Match(usernameRegex, []byte(stringValue))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Encontered err during regex matching: %s", err)
		return false
	}
	return ok
}

func UsernameArg() sqlmock.Argument {
	return UsernameArgument{}
}

type PasswordArgument struct{}

func (p PasswordArgument) Match(value driver.Value) bool {
	stringValue, ok := value.(string)
	if !ok {
		fmt.Fprintf(os.Stderr, "Could not convert value %s to string", value)
		return false
	}
	ok, err := regexp.Match(passwordRegex, []byte(stringValue))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Encontered err during regex matching: %s", err)
		return false
	}
	return ok
}

func PasswordArg() sqlmock.Argument {
	return PasswordArgument{}
}

type MockBroker struct {
	Broker

	wg sync.WaitGroup
}

func (mockBroker *MockBroker) generatedRandomDbName() string {
	return mockDbName
}

func (mockBroker *MockBroker) Setup(instance string, dbName string) {
	defer mockBroker.wg.Done()

	mockBroker.Broker.Setup(instance, dbName)
}

func (mockBroker *MockBroker) Provision(instance string, details brokerapi.ProvisionDetails, asyncAllowed bool) (brokerapi.ProvisionedServiceSpec, error) {
	spec := brokerapi.ProvisionedServiceSpec{IsAsync: true}

	dbName := mockBroker.generatedRandomDbName()
	go mockBroker.Setup(instance, dbName)

	return spec, nil
}

func (mockBroker *MockBroker) Teardown(instance string) {
	defer mockBroker.wg.Done()

	mockBroker.Broker.Teardown(instance)
}

func (mockBroker *MockBroker) Deprovision(instance string, details brokerapi.DeprovisionDetails, asyncAllowed bool) (brokerapi.IsAsync, error) {
	go mockBroker.Teardown(instance)
	return true, nil
}

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
			dbName:     random(8),
			expectedOk: true,
		},
		"key is name": {
			dbNameKey:  "name",
			dbName:     random(8),
			expectedOk: true,
		},
		"key is database": {
			dbNameKey:  "database",
			dbName:     random(8),
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

func TestCreateBrokerDatabaseSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{}

	mock.ExpectExec("CREATE DATABASE broker").WillReturnResult(sqlmock.NewResult(1, 1))

	dbErr := mockBroker.createBrokerDb(db)
	if dbErr != nil {
		t.Fatalf(`unexpected error: %s`, dbErr)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestCreateBrokerDatabaseAlreadyExists(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{}
	mock.ExpectExec("CREATE DATABASE broker").
		WillReturnError(&pq.Error{
			Code: "42P04",
		})

	dbErr := mockBroker.createBrokerDb(db)
	if dbErr != nil {
		t.Fatalf(`unexpected error: %s`, dbErr)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestCreateBrokerDatabaseUnexpectedError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{}
	expectedError := errors.New("random database error")
	mock.ExpectExec("CREATE DATABASE broker").
		WillReturnError(expectedError)

	dbErr := mockBroker.createBrokerDb(db)
	if !errors.Is(dbErr, expectedError) {
		t.Fatalf(`expected error: %s, got: %s`, expectedError, dbErr)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestCreateBrokerDatabaseSchemasSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mock.ExpectExec(regexp.QuoteMeta(`CREATE TYPE state AS ENUM ('setup', 'in-use', 'teardown', 'done', 'gone', 'failed', 'error')`)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`
	CREATE TABLE dbs (
	  instance CHAR(36)          UNIQUE,
	  name     CHAR(42) NOT NULL UNIQUE,
	  state    state,
	  expires  INTEGER
	)`)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`
	CREATE TABLE IF NOT EXISTS
	creds (
		binding CHAR(36) NOT NULL UNIQUE,
		name    CHAR(17) NOT NULL UNIQUE,
		pass    CHAR(64) NOT NULL,
		db      CHAR(42) NOT NULL
	)`)).WillReturnResult(sqlmock.NewResult(1, 1))

	dbErr := mockBroker.createBrokerDbSchemas()
	if dbErr != nil {
		t.Fatalf(`unexpected error: %s`, dbErr)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerProvisionDatabaseSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance := "instance-" + random(8)
	fakeDetails := brokerapi.ProvisionDetails{}

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO dbs (instance, name, state, expires) VALUES ($1, $2, $3, $4)`)).
		WithArgs(mockInstance, mockDbName, "setup", 0).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("CREATE DATABASE %s", mockDbName)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE dbs SET state = 'done' WHERE instance = $1`)).
		WithArgs(mockInstance).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mockBroker.wg.Add(1)
	_, dbErr := mockBroker.Provision(mockInstance, fakeDetails, true)
	mockBroker.wg.Wait()

	if dbErr != nil {
		t.Fatalf(`unexpected error: %s`, dbErr)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerDeprovisionDatabaseSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance := "instance-" + random(8)
	mockDetails := brokerapi.DeprovisionDetails{}

	dbColumns := []string{"state", "name"}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT state, name FROM dbs WHERE instance = $1`)).
		WithArgs(mockInstance).
		WillReturnRows(sqlmock.NewRows(dbColumns).AddRow("enabled", mockDbName))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE dbs SET state = 'teardown' WHERE instance = $1`)).
		WithArgs(mockInstance).
		WillReturnResult(sqlmock.NewResult(1, 1))

	credsColumns := []string{"name"}
	credsRows := []string{"fakeCreds"}
	mockedCredRows := sqlmock.NewRows(credsColumns).AddRow(credsRows[0])
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT creds.name FROM creds INNER JOIN dbs ON creds.db = dbs.name WHERE dbs.instance = $1`)).
		WithArgs(mockInstance).
		WillReturnRows(mockedCredRows)
	mock.ExpectExec(fmt.Sprintf("REVOKE ALL PRIVILEGES ON DATABASE %s FROM %s", mockDbName, credsRows[0])).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP USER %s", credsRows[0])).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec(fmt.Sprintf("DROP DATABASE %s", mockDbName)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM creds WHERE db = $1")).
		WithArgs(mockDbName).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE dbs SET state = 'gone', expires = extract(epoch from now()) + 3600 WHERE instance = $")).
		WithArgs(mockInstance).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mockBroker.wg.Add(1)
	_, dbErr := mockBroker.Deprovision(mockInstance, mockDetails, true)
	mockBroker.wg.Wait()

	if dbErr != nil {
		t.Fatalf(`unexpected error: %s`, dbErr)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerBindDatabaseSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	host, port := random(8), random(8)
	mockBroker := &MockBroker{
		Broker: Broker{
			Host: host,
			Port: port,
			db:   db,
		},
	}

	mockInstance, mockBindingId, mockDetails := "instance-"+random(8), "binding-"+random(8), brokerapi.BindDetails{}

	dbColumns := []string{"name", "state"}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT name, state FROM dbs WHERE instance = $1`)).
		WithArgs(mockInstance).
		WillReturnRows(sqlmock.NewRows(dbColumns).AddRow(mockDbName, "done"))
	mock.ExpectExec(`CREATE USER u[0-9|a-z]{16} WITH NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD \'[0-9|a-z]{64}\'`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO %s", mockDbName, usernameRegex)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO creds (binding, db, name, pass) VALUES ($1, $2, $3, $4)")).
		WithArgs(mockBindingId, mockDbName, UsernameArg(), PasswordArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	// TODO: do we want to test the shape of the returned binding?
	_, dbErr := mockBroker.Bind(mockInstance, mockBindingId, mockDetails)

	if dbErr != nil {
		t.Fatalf(`unexpected error: %s`, dbErr)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerBindDatabaseSelectCredsFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance := "instance-" + random(8)
	mockBindingId := "binding-" + random(8)
	mockDetails := brokerapi.BindDetails{}
	expectedDbError := errors.New("select creds error")

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT name, state FROM dbs WHERE instance = $1`)).
		WithArgs(mockInstance).
		WillReturnError(expectedDbError)

	_, dbErr := mockBroker.Bind(mockInstance, mockBindingId, mockDetails)
	if dbErr == nil {
		t.Fatalf(`expected error, got: %s`, dbErr)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerBindDatabaseNotReadyFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance := "instance-" + random(8)
	mockBindingId := "binding-" + random(8)
	mockDetails := brokerapi.BindDetails{}

	dbColumns := []string{"name", "state"}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT name, state FROM dbs WHERE instance = $1`)).
		WithArgs(mockInstance).
		// mock state to not equal "done"
		WillReturnRows(sqlmock.NewRows(dbColumns).AddRow(mockDbName, "not ready"))

	_, dbErr := mockBroker.Bind(mockInstance, mockBindingId, mockDetails)
	if dbErr == nil {
		t.Fatalf(`expected error, got: %s`, dbErr)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerBindDatabaseCreateUserFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance := "instance-" + random(8)
	mockBindingId := "binding-" + random(8)
	mockDetails := brokerapi.BindDetails{}
	expectedDbError := errors.New("create user error")

	dbColumns := []string{"name", "state"}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT name, state FROM dbs WHERE instance = $1`)).
		WithArgs(mockInstance).
		WillReturnRows(sqlmock.NewRows(dbColumns).AddRow(mockDbName, "done"))
	mock.ExpectExec(`CREATE USER u[0-9|a-z]{16} WITH NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD \'[0-9|a-z]{64}\'`).
		WillReturnError(expectedDbError)

	_, dbErr := mockBroker.Bind(mockInstance, mockBindingId, mockDetails)
	if dbErr == nil || !errors.Is(dbErr, expectedDbError) {
		t.Fatalf(`expected error: %s, got: %s`, expectedDbError, dbErr)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerBindDatabaseGrantPrivilegesFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance := "instance-" + random(8)
	mockBindingId := "binding-" + random(8)
	mockDetails := brokerapi.BindDetails{}
	expectedDbError := errors.New("grant privileges error")

	dbColumns := []string{"name", "state"}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT name, state FROM dbs WHERE instance = $1`)).
		WithArgs(mockInstance).
		WillReturnRows(sqlmock.NewRows(dbColumns).AddRow(mockDbName, "done"))
	mock.ExpectExec(fmt.Sprintf(`CREATE USER %s WITH NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD \'%s\'`, usernameRegex, passwordRegex)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO %s", mockDbName, usernameRegex)).
		WillReturnError(expectedDbError)
	mock.ExpectExec(fmt.Sprintf("DROP USER %s", usernameRegex)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, dbErr := mockBroker.Bind(mockInstance, mockBindingId, mockDetails)
	if dbErr == nil || !errors.Is(dbErr, expectedDbError) {
		t.Fatalf(`expected error: %s, got: %s`, expectedDbError, dbErr)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerBindDatabaseInsertCredentialsFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance := "instance-" + random(8)
	mockBindingId := "binding-" + random(8)
	mockDetails := brokerapi.BindDetails{}
	expectedDbError := errors.New("insert credentials error")

	dbColumns := []string{"name", "state"}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT name, state FROM dbs WHERE instance = $1`)).
		WithArgs(mockInstance).
		WillReturnRows(sqlmock.NewRows(dbColumns).AddRow(mockDbName, "done"))
	mock.ExpectExec(fmt.Sprintf(`CREATE USER %s WITH NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD \'%s\'`, usernameRegex, passwordRegex)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO %s", mockDbName, usernameRegex)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO creds (binding, db, name, pass) VALUES ($1, $2, $3, $4)")).
		WithArgs(mockBindingId, mockDbName, UsernameArg(), PasswordArg()).
		WillReturnError(expectedDbError)
	mock.ExpectExec(fmt.Sprintf("DROP USER %s", usernameRegex)).
		WillReturnResult(sqlmock.NewResult(1, 1))

	_, dbErr := mockBroker.Bind(mockInstance, mockBindingId, mockDetails)
	if dbErr == nil || !errors.Is(dbErr, expectedDbError) {
		t.Fatalf(`expected error: %s, got: %s`, expectedDbError, dbErr)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerUnbindDatabaseSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance, mockBindingId, mockDetails := "instance-"+random(8), "binding-"+random(8), brokerapi.UnbindDetails{}

	dbColumns := []string{"state", "name", "db"}
	dbRowValues := []driver.Value{"done", random(5), mockDbName}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT dbs.state, creds.name, creds.db FROM creds INNER JOIN dbs ON creds.db = dbs.name WHERE creds.binding = $1`)).
		WithArgs(mockBindingId).
		WillReturnRows(sqlmock.NewRows(dbColumns).AddRow(dbRowValues...))
	mock.ExpectExec(fmt.Sprintf("REVOKE ALL PRIVILEGES ON DATABASE %s FROM %s", mockDbName, dbRowValues[1])).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP USER %s", dbRowValues[1])).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM creds WHERE name = $1`)).
		WithArgs(dbRowValues[1]).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = mockBroker.Unbind(mockInstance, mockBindingId, mockDetails)
	if err != nil {
		t.Fatalf(`unexpected error: %s`, err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerUnbindDatabaseSelectCredsFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance, mockBindingId, mockDetails := "instance-"+random(8), "binding-"+random(8), brokerapi.UnbindDetails{}

	expectedErr := errors.New("select creds error")

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT dbs.state, creds.name, creds.db FROM creds INNER JOIN dbs ON creds.db = dbs.name WHERE creds.binding = $1`)).
		WithArgs(mockBindingId).
		WillReturnError(expectedErr)

	err = mockBroker.Unbind(mockInstance, mockBindingId, mockDetails)
	if err != expectedErr {
		t.Fatalf(`expected error: %s, got: %s`, expectedErr, err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerUnbindDatabaseSelectCredsEmpty(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance, mockBindingId, mockDetails := "instance-"+random(8), "binding-"+random(8), brokerapi.UnbindDetails{}

	dbColumns := []string{"state", "name", "db"}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT dbs.state, creds.name, creds.db FROM creds INNER JOIN dbs ON creds.db = dbs.name WHERE creds.binding = $1`)).
		WithArgs(mockBindingId).
		WillReturnRows(sqlmock.NewRows(dbColumns))

	err = mockBroker.Unbind(mockInstance, mockBindingId, mockDetails)
	if err == nil {
		t.Fatal("expected error but received nil")
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerUnbindDatabaseNotDoneError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance, mockBindingId, mockDetails := "instance-"+random(8), "binding-"+random(8), brokerapi.UnbindDetails{}

	dbColumns := []string{"state", "name", "db"}
	// mock "state" value to not be "done"
	dbRowValues := []driver.Value{"not done", random(5), mockDbName}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT dbs.state, creds.name, creds.db FROM creds INNER JOIN dbs ON creds.db = dbs.name WHERE creds.binding = $1`)).
		WithArgs(mockBindingId).
		WillReturnRows(sqlmock.NewRows(dbColumns).AddRow(dbRowValues...))

	err = mockBroker.Unbind(mockInstance, mockBindingId, mockDetails)
	if err == nil {
		t.Fatal("expected error but received nil")
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerUnbindDatabaseRevokePrivilegesError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance, mockBindingId, mockDetails := "instance-"+random(8), "binding-"+random(8), brokerapi.UnbindDetails{}

	dbColumns := []string{"state", "name", "db"}
	dbRowValues := []driver.Value{"done", random(5), mockDbName}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT dbs.state, creds.name, creds.db FROM creds INNER JOIN dbs ON creds.db = dbs.name WHERE creds.binding = $1`)).
		WithArgs(mockBindingId).
		WillReturnRows(sqlmock.NewRows(dbColumns).AddRow(dbRowValues...))

	expectedErr := errors.New("revoke privileges error")
	mock.ExpectExec(fmt.Sprintf("REVOKE ALL PRIVILEGES ON DATABASE %s FROM %s", mockDbName, dbRowValues[1])).
		WillReturnError(expectedErr)

	err = mockBroker.Unbind(mockInstance, mockBindingId, mockDetails)
	if !errors.Is(err, expectedErr) {
		t.Fatalf(`expected error %s to wrap %s`, err, expectedErr)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerUnbindDatabaseDropUserFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance, mockBindingId, mockDetails := "instance-"+random(8), "binding-"+random(8), brokerapi.UnbindDetails{}

	dbColumns := []string{"state", "name", "db"}
	dbRowValues := []driver.Value{"done", random(5), mockDbName}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT dbs.state, creds.name, creds.db FROM creds INNER JOIN dbs ON creds.db = dbs.name WHERE creds.binding = $1`)).
		WithArgs(mockBindingId).
		WillReturnRows(sqlmock.NewRows(dbColumns).AddRow(dbRowValues...))
	mock.ExpectExec(fmt.Sprintf("REVOKE ALL PRIVILEGES ON DATABASE %s FROM %s", mockDbName, dbRowValues[1])).
		WillReturnResult(sqlmock.NewResult(1, 1))

	dropUserErr := errors.New("drop user error")
	mock.ExpectExec(fmt.Sprintf("DROP USER %s", dbRowValues[1])).
		WillReturnError(dropUserErr)

	// error to DROP USER **should not** cause unbind to fail.
	// for example: DROP USER may fail if there are still database objects
	// created/owned by that user, which cannot be resolved without deleting
	// those objects
	err = mockBroker.Unbind(mockInstance, mockBindingId, mockDetails)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerUnbindDatabaseDeleteCredsFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance, mockBindingId, mockDetails := "instance-"+random(8), "binding-"+random(8), brokerapi.UnbindDetails{}

	dbColumns := []string{"state", "name", "db"}
	dbRowValues := []driver.Value{"done", random(5), mockDbName}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT dbs.state, creds.name, creds.db FROM creds INNER JOIN dbs ON creds.db = dbs.name WHERE creds.binding = $1`)).
		WithArgs(mockBindingId).
		WillReturnRows(sqlmock.NewRows(dbColumns).AddRow(dbRowValues...))
	mock.ExpectExec(fmt.Sprintf("REVOKE ALL PRIVILEGES ON DATABASE %s FROM %s", mockDbName, dbRowValues[1])).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(fmt.Sprintf("DROP USER %s", dbRowValues[1])).
		WillReturnResult(sqlmock.NewResult(1, 1))

	deleteCredsErr := errors.New("delete creds error")
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM creds WHERE name = $1`)).
		WithArgs(dbRowValues[1]).
		WillReturnError(deleteCredsErr)

	err = mockBroker.Unbind(mockInstance, mockBindingId, mockDetails)
	if err != nil {
		t.Fatalf("unexpected err: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerLastOperationSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance := "instance-" + random(8)

	dbColumns := []string{"state"}
	dbRowValues := []driver.Value{"done"}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT state FROM dbs WHERE instance = $1`)).
		WithArgs(mockInstance).
		WillReturnRows(sqlmock.NewRows(dbColumns).AddRow(dbRowValues...))

	_, err = mockBroker.LastOperation(mockInstance)
	if err != nil {
		t.Fatalf(`unexpected error: %s`, err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerLastOperationQueryFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance := "instance-" + random(8)
	expectedErr := errors.New("select query error")
	expectedOperation := brokerapi.LastOperation{State: "failed"}

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT state FROM dbs WHERE instance = $1`)).
		WithArgs(mockInstance).
		WillReturnError(expectedErr)

	lastOperation, err := mockBroker.LastOperation(mockInstance)
	if err != nil {
		t.Fatalf(`unexpected error: %s`, err)
	}
	if lastOperation != expectedOperation {
		t.Fatalf("expected operation: %s, received: %s", expectedOperation, lastOperation)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerLastOperationNoResultsFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance := "instance-" + random(8)
	expectedOperation := brokerapi.LastOperation{State: "failed"}

	dbColumns := []string{"state"}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT state FROM dbs WHERE instance = $1`)).
		WithArgs(mockInstance).
		WillReturnRows(sqlmock.NewRows(dbColumns))

	lastOperation, err := mockBroker.LastOperation(mockInstance)
	if err != nil {
		t.Fatalf(`unexpected error: %s`, err)
	}
	if lastOperation != expectedOperation {
		t.Fatalf("expected operation: %s, received: %s", expectedOperation, lastOperation)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestBrokerLastOperationUnexpectedState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	mockBroker := &MockBroker{
		Broker: Broker{
			db: db,
		},
	}

	mockInstance := "instance-" + random(8)

	dbColumns := []string{"state"}
	dbRowValues := []driver.Value{"unexpected"}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT state FROM dbs WHERE instance = $1`)).
		WithArgs(mockInstance).
		WillReturnRows(sqlmock.NewRows(dbColumns).AddRow(dbRowValues...))

	_, err = mockBroker.LastOperation(mockInstance)
	if err == nil {
		t.Fatal("expected error but received nil")
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
