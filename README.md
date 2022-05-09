PostgreSQL Tinsmith
===================

A _tinsmith_, in Blacksmith Services parlance, is a Cloud Foundry
Service Broker that runs as a Cloud Foundry Application, consumes
a provisioned, on-demand Blacksmith Service, and makes it
available to the rest of the world as a shared, multi-tenant
service.

A picture may make things clearer:

![Tinsmith Architectural Diagram](docs/tinsmith.png)

This Tinsmith, in particular, takes a dedicated PostgreSQL
instance (most likely deployed via the [PostgreSQL Blacksmith
Forge][pg-forge]) and carves up the dedicated host / cluster into
individual databases for each service.  Bound applications will
have free run of their own database, but will be unable to
interact with other shared databases on the same installation.

Deploying
---------

To deploy this Tinsmith, you need the code, a Cloud Foundry, and a
PostgreSQL service from your CF Marketplace.  We heartily
recommend a service deployed via the [Blacksmith][blacksmith] and
its [PostgreSQL Forge][pg-forge], but any service that provides the following 
information in its `$VCAP_SERVICES` credentials block should do:

- `host`
- `port` (as a number)
- `username`
- `password`  
- `db_name` or `name` or `database`

To deploy and start the Tinsmith application:

```shell
git clone https://github.com/blacksmith-community/cf-postgres-tinsmith
cd cf-postgres-tinsmith

# push the code for tinsmith
cf push --no-start

# bind your database service to tinsmith
cf bind-service postgres-tinsmith YOUR-DATABASE-SERVICE

# tag your database service so that tinsmith can find it
# See "Configuration" below
cf update-service YOUR-DATABASE-SERVICE -t "postgresql"

# you may want to set some other environment variables at
# this stage; see "Configuration", below.
cf set-env postgres-tinsmith SB_BROKER_USERNAME my-broker
cf set-env postgres-tinsmith SB_BROKER_PASSWORD a-secret

# start the app
cf start postgres-tinsmith
```

Then, create the Tinsmith service broker. If you have the administrator role:

```shell
cf create-service-broker postgres-tinsmith my-broker a-secret \
  https://postgres-tinsmith.$APP_DOMAIN
```

If you only have the space developer role:

```shell
# make sure to target your space
cf target -s <SPACE>

cf create-service-broker postgres-tinsmith my-broker a-secret \
  https://postgres-tinsmith.$APP_DOMAIN --space-scoped
```  

Lastly, enable the new `postgres` service provided by this Tinsmith and verify it:

```shell
# enable the service
cf enable-service-access postgres

# verify service appears in the list
cf marketplace
```

Configuring
-----------

This tinsmith is configured entirely through environment
variables.

There are environment variables for governing the presentation of
this brokers service / plan in the marketplace:

- `SERVICE_ID` - The internal ID of the service that this broker
  provides to the marketplace.
- `SERVICE_NAME` - The CLI-friendly name of the service.
- `PLAN_ID` - The internal ID of the plan that this broker
  provides to the marketplace.
- `DESCRIPTION` - A human-friendly description of the service /
  plan, to be displayed in the marketplace
- `TAGS` - A comma-separated list of tags to apply to instances
  of the service.

There are environment variables for controlling the security and
authentication parameters of the broker:

- `SB_BROKER_USERNAME` - The HTTP Basic Auth username that must
  be used to access this broker.  Defaults to `b-postgres`.
- `SB_BROKER_PASSWORD` - The HTTP Basic Auth password that must
  be used to access this broker.  Defaults to `postgres`.

You can also override the service selection logic and force it to
pick a specific, named service by setting the `USE_SERVICE`
environment variable to its name.  Otherwise, the broker will look
for bound services that are tagged `postgres` or `postgresql`.





[pg-forge]: https://github.com/blacksmith-community/postgresql-forge-boshrelease
[blacksmith]: https://github.com/cloudfoundry-community/blacksmith
