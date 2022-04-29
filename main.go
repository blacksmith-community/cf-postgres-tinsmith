package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/jhunt/vcaptive"
	"github.com/pivotal-cf/brokerapi"
	"github.com/pivotal-golang/lager"
)

func cfg(deflt, env string) string {
	if s := os.Getenv(env); s != "" {
		return s
	}
	return deflt
}

func main() {
	broker := &Broker{}
	broker.Service.ID = cfg("postgres-c504319a-61e7-459e-83ac-01243787689b", "SERVICE_ID")
	broker.Service.Name = cfg("postgres", "SERVICE_NAME")
	broker.Plan.ID = cfg("postgres-c504319a-61e7-459e-83ac-01243787689b", "PLAN_ID")
	broker.Plan.Name = cfg("shared", "PLAN_NAME")
	broker.Description = cfg("A shared PostgreSQL database", "DESCRIPTION")
	broker.Tags = strings.Split(cfg("shared,postgres,postgresql,tinsmith", "TAGS"), ",")

	app, err := vcaptive.ParseApplication(os.Getenv("VCAP_APPLICATION"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "VCAP_APPLICATION: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("running v%s of %s at http://%s\n", app.Version, app.Name, app.URIs[0])
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
		broker.Username = s
	} else {
		fmt.Fprintf(os.Stderr, "VCAP_SERVICES: '%s' service has no 'username' credential\n", instance.Label)
		os.Exit(3)
	}
	if s, ok := instance.GetString("password"); ok {
		broker.Password = s
	} else {
		fmt.Fprintf(os.Stderr, "VCAP_SERVICES: '%s' service has no 'password' credential\n", instance.Label)
		os.Exit(3)
	}
	if s, ok := instance.GetString("host"); ok {
		broker.Host = s
	} else {
		fmt.Fprintf(os.Stderr, "VCAP_SERVICES: '%s' service has no 'host' credential\n", instance.Label)
		os.Exit(3)
	}
	if s, ok := instance.GetString("db_name"); ok {
		broker.InitialDatabase = s
	} else {
		fmt.Fprintf(os.Stderr, "VCAP_SERVICES: '%s' service has no 'db_name' credential\n", instance.Label)
		os.Exit(3)
	}
	if s, ok := instance.GetString("port"); ok {
		broker.Port = s
	} else {
		fmt.Fprintf(os.Stderr, "VCAP_SERVICES: '%s' service has no 'port' credential; using default of 5432\n", instance.Label)
		broker.Port = "5432"
	}

	if err := broker.Init(); err != nil {
		panic(err)
	}

	http.Handle("/", brokerapi.New(
		broker,
		lager.NewLogger("postgres-tinsmith"),
		brokerapi.BrokerCredentials{
			Username: cfg("b-postgres", "SB_BROKER_USERNAME"),
			Password: cfg("postgres", "SB_BROKER_PASSWORD"),
		},
	))
	err = http.ListenAndServe(":"+cfg("3000", "PORT"), nil)
	fmt.Fprintf(os.Stderr, "http server exited: %s\n", err)
}
