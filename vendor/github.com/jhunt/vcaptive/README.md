vcaptive
========

`vcaptive` is a small Go library for consuming Cloud Foundry
`$VCAP_SERVICES`-style service and credential definitions, and
the `$VCAP_APPLICATION` environment variable for introspection
of things like URLs and application versions.

$VCAP\_APPLICATION
------------------

You can parse the `$VCAP_APPLICATION` environment variable to
retrieve information about your bound routes / URIs, version IDs,
application names, and more:

`vcaptive` takes this:

```
{
  "application_id": "e233016d-3bce-4e1e-9269-b1ad1555cf99",
  "application_name": "my-test-app",
  "application_uris": [
   "my-test-app.cfapps.io"
  ],
  "application_version": "35c179da-ae9a-4cb6-b787-98261b3bb183",
  "cf_api": "https://api.cfapps.io",
  "limits": {
   "disk": 1024,
   "fds": 16384,
   "mem": 1024
  },
  "name": "my-test-app",
  "space_id": "1afffefc-6318-4b72-8383-7bac3fdc6ec6",
  "space_name": "stark-and-wayne",
  "uris": [
   "my-test-app.cfapps.io"
  ],
  "users": null,
  "version": "35c179da-ae9a-4cb6-b787-98261b3bb183"
}
```

And lets you do this:

```
package main

import (
  "fmt"
  "os"

  "github.com/jhunt/vcaptive"
)

func main() {
  app, err := vcaptive.ParseApplication(os.Getenv("VCAP_APPLICATION"))
  if err != nil {
    fmt.Fprintf(os.Stderr, "VCAP_APPLICATION%s\n", err)
    os.Exit(1)
  }

  fmt.Printf("running v%s of %s at http://%s\n", app.Version, app.Name, app.URIs[0])
  // ...
}
```

$VCAP\_SERVICES
---------------

To handle services, `vcaptive` takes this:

```
{
  "elephantsql": [
    {
      "name": "elephantsql-c6c60",
      "label": "elephantsql",
      "tags": [
        "postgres",
        "postgresql",
        "relational"
      ],
      "plan": "turtle",
      "credentials": {
        "uri": "postgres://exampleuser:examplepass@babar.elephantsql.com:5432/exampleuser",
        "user": "exampleuser",
        "pass": "examplepass",
        "host": "babar.elephantsql.com",
        "port": 5432,
        "db": "exampleuser"
      }
    }
  ],
  "sendgrid": [
    {
      "name": "mysendgrid",
      "label": "sendgrid",
      "tags": [
        "smtp"
      ],
      "plan": "free",
      "credentials": {
        "hostname": "smtp.sendgrid.net",
        "username": "QvsXMbJ3rK",
        "password": "HCHMOYluTv"
      }
    }
  ]
}
```

And lets you do this:

```
package main

import (
  "fmt"
  "os"

  "github.com/jhunt/vcaptive"
)

func main() {
  services, err := vcaptive.ParseServices(os.Getenv("VCAP_SERVICES"))
  if err != nil {
    fmt.Fprintf(os.Stderr, "VCAP_SERVICES: %s\n", err)
    os.Exit(1)
  }

  instance, found := services.Tagged("postgres", "postgresql")
  if !found {
    fmt.Fprintf(os.Stderr, "VCAP_SERVICES: no 'postgres' service found\n")
    os.Exit(2)
  }

  host, ok := instance.GetString("host")
  if !ok {
    fmt.Fprintf(os.Stderr, "VCAP_SERVICES: '%s' service has no 'host' credential\n", instance.Label)
    os.Exit(3)
  }
  port, ok := instance.GetUint("port")
  if !ok {
    fmt.Fprintf(os.Stderr, "VCAP_SERVICES: '%s' service has no 'port' credential\n", instance.Label)
    os.Exit(3)
  }

  fmt.Printf("Connecting to %s:%d...\n", host, port)
  // ...
}
```

If you don't have tags, you can also retrieve the first service
that has a given set of credentials:

```
inst, found := services.WithCredentials("smtp_host", "smtp_username")
```

Resources
---------

- [Cloud Foundry (OSS) Documentation on `VCAP_SERVICES`][1]

Contributing
------------

I wrote this tool because I needed it, and no one else had written
it.  My hope is that you find it useful as well.  If it's close,
but not 100% there, why not fork it, fix what needs fixing, and
submit a pull request?

Happy Hacking!


[1]: https://docs.cloudfoundry.org/devguide/deploy-apps/environment-variable.html#VCAP-SERVICES
