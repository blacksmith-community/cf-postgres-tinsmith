package main

import (
	"fmt"
	"os"

	"database/sql"

	"github.com/jhunt/vcaptive"
	"github.com/lib/pq"

	"github.com/pivotal-cf/brokerapi"
)

type Broker struct {
	Description   string
	Tags          []string
	Service, Plan struct {
		Name string
		ID   string
	}

	Host            string
	Port            string
	Username        string
	Password        string
	InitialDatabase string

	db *sql.DB
}

func getDatabaseName(instance vcaptive.Instance) (string, bool) {
	if s, ok := instance.GetString("db_name"); ok {
		return s, ok
	}
	if s, ok := instance.GetString("name"); ok {
		return s, ok
	}
	if s, ok := instance.GetString("database"); ok {
		return s, ok
	}
	return "", false
}

func (b *Broker) Init() error {
	info("initializing broker\n")

	services, err := vcaptive.ParseServices(os.Getenv("VCAP_SERVICES"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "VCAP_SERVICES: %s\n", err)
		os.Exit(1)
	}

	var (
		found    bool
		instance vcaptive.Instance
	)
	if name := os.Getenv("USE_SERVICE"); name != "" {
		instance, found = services.Named(name)
		if !found {
			fmt.Fprintf(os.Stderr, "VCAP_SERVICES: no service named '%s' found\n", name)
			os.Exit(2)
		}
	} else {
		instance, found = services.Tagged("postgres", "postgresql")
		if !found {
			fmt.Fprintf(os.Stderr, "VCAP_SERVICES: no 'postgres' service found\n")
			os.Exit(2)
		}
	}

	if s, ok := instance.GetString("username"); ok {
		b.Username = s
	} else {
		fmt.Fprintf(os.Stderr, "VCAP_SERVICES: '%s' service has no 'username' credential\n", instance.Label)
		os.Exit(3)
	}
	if s, ok := instance.GetString("password"); ok {
		b.Password = s
	} else {
		fmt.Fprintf(os.Stderr, "VCAP_SERVICES: '%s' service has no 'password' credential\n", instance.Label)
		os.Exit(3)
	}
	if s, ok := instance.GetString("host"); ok {
		b.Host = s
	} else {
		fmt.Fprintf(os.Stderr, "VCAP_SERVICES: '%s' service has no 'host' credential\n", instance.Label)
		os.Exit(3)
	}
	if s, ok := getDatabaseName(instance); ok {
		b.InitialDatabase = s
	} else {
		fmt.Fprintf(os.Stderr, "VCAP_SERVICES: '%s' service has no database name credential\n", instance.Label)
		os.Exit(3)
	}
	if s, ok := instance.GetString("port"); ok {
		b.Port = s
	} else {
		fmt.Fprintf(os.Stderr, "VCAP_SERVICES: '%s' service has no 'port' credential; using default of 5432\n", instance.Label)
		b.Port = "5432"
	}

	b.openDbConnection(b.InitialDatabase)
	b.createBrokerDb()
	b.db.Close()

	b.openDbConnection("broker")
	b.createBrokerDbSchemas()

	return nil
}

func (b *Broker) openDbConnection(dbName string) error {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", b.Username, b.Password, b.Host, b.Port, dbName)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to open database connection: %s\n", err)
		return err
	}

	b.db = db
	return nil
}

func (b *Broker) createBrokerDb() error {
	_, createBrokerDbErr := b.db.Exec(`CREATE DATABASE broker`)
	if createBrokerDbErr != nil {
		createBrokerDbPqErr, ok := createBrokerDbErr.(*pq.Error)
		if ok && createBrokerDbPqErr.Code == "42P04" {
			info("broker database already exists, continuing\n")
		} else {
			return fmt.Errorf("error creating broker database: %w", createBrokerDbErr)
		}
	}
	return nil
}

func (b *Broker) createBrokerDbSchemas() error {
	b.db.Exec(`CREATE TYPE state AS ENUM ('setup', 'in-use', 'teardown', 'done', 'gone', 'failed', 'error')`)
	b.db.Exec(`
CREATE TABLE dbs (
  instance CHAR(36)          UNIQUE,
  name     CHAR(42) NOT NULL UNIQUE,
  state    state,
  expires  INTEGER
)`)
	b.db.Exec(`
CREATE TABLE IF NOT EXISTS
creds (
  binding CHAR(36) NOT NULL UNIQUE,
  name    CHAR(17) NOT NULL UNIQUE,
  pass    CHAR(64) NOT NULL,
  db      CHAR(42) NOT NULL
)`)
	return nil
}

func (b *Broker) Exists(instance string) bool {
	r, err := b.db.Query(`SELECT name FROM dbs WHERE instance = $1`, instance)
	return err == nil && r.Next()
}

func (b *Broker) fail(what, instance string, err error) {
	fmt.Fprintf(os.Stderr, "failed %s: %s\n", what, err)
	b.db.Exec(`UPDATE dbs SET state = 'failed'::state WHERE instance = $1`, instance)
}

func (b *Broker) generatedRandomDbName() string {
	db := "db" + random(40)
	return db
}

func (b *Broker) Setup(instance string, dbName string) {
	_, err := b.db.Exec(`INSERT INTO dbs (instance, name, state, expires) VALUES ($1, $2, $3, $4)`,
		instance, dbName, "setup", 0)
	if err != nil {
		b.fail("creating `dbs` entry", instance, err)
		return
	}

	_, err = b.db.Exec(`CREATE DATABASE ` + dbName)
	if err != nil {
		b.fail("creating instance database", instance, err)
		return
	}

	if _, err := b.db.Exec(`UPDATE dbs SET state = 'done' WHERE instance = $1`, instance); err != nil {
		fmt.Fprintf(os.Stderr, "unable to transition instance from [setup] -> [done]: %s\n", err)
	}
}

func (b *Broker) CheckOn(instance string) string {
	r, err := b.db.Query(`SELECT state FROM dbs WHERE instance = $1`, instance)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to retrieve instance [%s] database state: %s\n", instance, err)
		return "error"
	}
	if !r.Next() {
		fmt.Fprintf(os.Stderr, "failed to retrieve instance [%s] database state: no entry in dbs table\n", instance)
		return "error"
	}

	var state string
	if err := r.Scan(&state); err != nil {
		fmt.Fprintf(os.Stderr, "failed to retrieve instance [%s] database state: %s\n", instance, err)
		return "error"
	}

	return state
}

func (b *Broker) Grant(instance, binding string) (string, string, string, error) {
	var db, state string
	r, err := b.db.Query(`SELECT name, state FROM dbs WHERE instance = $1`, instance)
	if err != nil || !r.Next() || r.Scan(&db, &state) != nil {
		return "", "", "", fmt.Errorf("failed to retrieve database instance")
	}
	if state != "done" {
		return "", "", "", fmt.Errorf("database is still in '%s' state", state)
	}

	user := "u" + random(16)
	pass := random(64)

	_, err = b.db.Exec(`CREATE USER ` + user + ` WITH NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD '` + pass + `'`)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to provision a user: %w", err)
	}

	_, err = b.db.Exec(`GRANT ALL PRIVILEGES ON DATABASE ` + db + ` TO ` + user)
	if err != nil {
		b.db.Exec(`DROP USER ` + user)
		return "", "", "", fmt.Errorf("failed to grant db access to user: %w", err)
	}

	_, err = b.db.Exec(`INSERT INTO creds (binding, db, name, pass) VALUES ($1, $2, $3, $4)`,
		binding, db, user, pass)
	if err != nil {
		b.db.Exec(`DROP USER ` + user)
		return "", "", "", fmt.Errorf("failed to grant db access to user: %w", err)
	}

	return user, pass, db, nil
}

func (b *Broker) Revoke(instance, binding string) error {
	var state, db, user string
	r, err := b.db.Query(`SELECT dbs.state, creds.name, creds.db FROM creds INNER JOIN dbs ON creds.db = dbs.name WHERE creds.binding = $1`, binding)
	if err != nil {
		return err
	}
	if !r.Next() {
		return fmt.Errorf("database binding not %s not found", binding)
	}
	if err := r.Scan(&state, &user, &db); err != nil {
		return err
	}
	if state != "done" {
		return fmt.Errorf("database is still in '%s' state", state)
	}

	_, err = b.db.Exec(`REVOKE ALL PRIVILEGES ON DATABASE ` + db + ` FROM ` + user)
	if err != nil {
		return fmt.Errorf("failed to revoke privileges: %w", err)
	}

	_, err = b.db.Exec(`DROP USER ` + user)
	if err != nil {
		return fmt.Errorf("failed to drop user: %w", err)
	}

	_, err = b.db.Exec(`DELETE FROM creds WHERE name = $1`, user)
	if err != nil {
		return fmt.Errorf("failed to delete creds: %w", err)
	}
	return nil
}

func (b *Broker) Teardown(instance string) {
	var state, db, user string
	r, err := b.db.Query(`SELECT state, name FROM dbs WHERE instance = $1`, instance)
	if err != nil || !r.Next() || r.Scan(&state, &db) != nil {
		b.fail("retrieving instance database entry", instance, err)
		return
	}

	b.db.Exec(`UPDATE dbs SET state = 'teardown' WHERE instance = $1`, instance)

	r, err = b.db.Query(`SELECT creds.name FROM creds INNER JOIN dbs ON creds.db = dbs.name WHERE dbs.instance = $1`, instance)
	if err != nil {
		b.fail("retreiving instance database credentials", instance, err)
		return
	}

	for r.Next() {
		if r.Scan(&user) != nil {
			continue
		}

		b.db.Exec(`REVOKE ALL PRIVILEGES ON DATABASE ` + db + ` FROM ` + user)
		b.db.Exec(`DROP USER ` + user)
	}

	b.db.Exec(`DROP DATABASE ` + db)
	b.db.Exec(`DELETE FROM creds WHERE db = $1`, db)
	b.db.Exec(`UPDATE dbs SET state = 'gone', expires = extract(epoch from now()) + 3600 WHERE instance = $1`, instance)
}

func (b *Broker) Track(instance, db, state string) {
	b.db.Exec(`INSERT INTO dbs (instance, name, state, expires) VALUES ($1, $2, $3, 0)`, instance, db, state)
}

/*************************************************************/

func (b *Broker) Services() []brokerapi.Service {
	return []brokerapi.Service{
		brokerapi.Service{
			ID:          b.Service.ID,
			Name:        b.Service.Name,
			Description: b.Description,
			Bindable:    true,
			Tags:        b.Tags,
			Plans: []brokerapi.ServicePlan{
				brokerapi.ServicePlan{
					ID:          b.Plan.ID,
					Name:        b.Plan.Name,
					Description: b.Description,
				},
			},
		},
	}
}

func (b *Broker) Provision(instance string, details brokerapi.ProvisionDetails, asyncAllowed bool) (brokerapi.ProvisionedServiceSpec, error) {
	spec := brokerapi.ProvisionedServiceSpec{IsAsync: true}

	info("somebody wants to provision a %s/%s\n", details.ServiceID, details.PlanID)
	if details.ServiceID != b.Service.ID && details.PlanID != b.Plan.ID {
		/* we only allow one service/plan */
		oops("invalid plan %s/%s (we only accept %s/%s)\n", details.ServiceID, details.PlanID, b.Service.ID, b.Plan.ID)
		return spec, fmt.Errorf("invalid plan %s/%s", details.ServiceID, details.PlanID)
	}

	dbName := b.generatedRandomDbName()

	go b.Setup(instance, dbName)
	return spec, nil
}

func (b *Broker) Deprovision(instance string, details brokerapi.DeprovisionDetails, asyncAllowed bool) (brokerapi.IsAsync, error) {
	info("somebody wants to deprovision %s (a %s/%s)\n", instance, details.ServiceID, details.PlanID)

	if !b.Exists(instance) {
		/* return a 410 Gone to the caller */
		return false, brokerapi.ErrInstanceDoesNotExist
	}

	go b.Teardown(instance)
	return true, nil
}

func (b *Broker) LastOperation(instance string) (brokerapi.LastOperation, error) {
	info("somebody wants to know how instance %s is progressing...\n", instance)

	state := b.CheckOn(instance)
	switch state {
	case "setup", "teardown":
		return brokerapi.LastOperation{State: "in progress"}, nil
	case "done", "gone":
		return brokerapi.LastOperation{State: "succeeded"}, nil
	case "failed", "error":
		return brokerapi.LastOperation{State: "failed"}, nil
	default:
		return brokerapi.LastOperation{}, fmt.Errorf("invalid state '%s'", state)
	}
}

func (b *Broker) Bind(instance, bindingID string, details brokerapi.BindDetails) (brokerapi.Binding, error) {
	var binding brokerapi.Binding

	info("somebody wants to bind service instance %s...\n", instance)
	user, pass, db, err := b.Grant(instance, bindingID)
	if err != nil {
		oops("failed to bind %s: %s\n", instance, err)
		return binding, err
	}

	binding.Credentials = map[string]interface{}{
		"username": user,
		"password": pass,
		"database": db,
		"host":     b.Host,
		"port":     b.Port,
		"dsn":      fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, b.Host, b.Port, db),
	}

	info("bound %s:%s@%s:%s/%s\n", user, pass, b.Host, b.Port, db)
	info("creds = %v\n", binding.Credentials)
	return binding, nil
}

func (b *Broker) Unbind(instance, bindingID string, details brokerapi.UnbindDetails) error {
	info("somebody wants to unbind %s from service instance %s...\n", bindingID, instance)

	err := b.Revoke(instance, bindingID)
	if err != nil {
		oops("failed to unbind %s from %s: %s\n", bindingID, instance, err)
		return err
	}

	info("unbound %s from %s\n", bindingID, instance)
	return nil
}

func (b *Broker) Update(instance string, details brokerapi.UpdateDetails, asyncAllowed bool) (brokerapi.IsAsync, error) {
	oops("update operation not implemented")
	return false, fmt.Errorf("not implemented")
}
