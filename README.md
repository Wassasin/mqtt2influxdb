# mqtt2influxdb
CLI tool to push MQTT messages to InfluxDB2.

## Interface

```bash
$ ./mqtt2influxdb -h
mqtt2influxdb 0.1.0
CLI tool to push MQTT messages to InfluxDB2

USAGE:
    mqtt2influxdb [OPTIONS] --influxdb-bucket <INFLUXDB_BUCKET> --influxdb-org <INFLUXDB_ORG> --influxdb-jwt <INFLUXDB_JWT> --config <CONFIG>

OPTIONS:
        --config <CONFIG>
            Path to the mapping configuration file used to translate MQTT messages to InfluxDB2
            points [env: CONFIG=]

    -h, --help
            Print help information

        --influxdb-bucket <INFLUXDB_BUCKET>
            InfluxDB2 bucket to write all the data to [env: INFLUXDB_BUCKET=]

        --influxdb-jwt <INFLUXDB_JWT>
            InfluxDB2 secret token for the account to use [env: INFLUXDB_JWT=]

        --influxdb-org <INFLUXDB_ORG>
            InfluxDB2 organization for the database. [env: INFLUXDB_ORG=]

        --influxdb-url <INFLUXDB_URL>
            Url for the InfluxDB2 server to connect to [env: INFLUXDB_URL=] [default:
            http://localhost:8086]

        --mqtt-client-id <MQTT_CLIENT_ID>
            Client ID used by this application to identify itself to the MQTT server [env:
            MQTT_CLIENT_ID=] [default: mqtt2influxdb]

        --mqtt-url <MQTT_URL>
            Url for the MQTT server to connect to [env: MQTT_URL=] [default: mqtt://localhost]

    -V, --version
            Print version information
```

Can use both ENV variables and command line flags to configure the application, but I highly recommend **not** using the command line flag for `--influxdb-jwt` because this will be available to any process running on the system. The mapping configuration is done exclusively using the configuration file pointed to by `--config`.

For examples of configuration files check out the `./examples` folder in the repository.

## Design goals
* Easy to use
* Flexible configuration
* Suitable as continuously running daemon

## TODO
If you want to contribute, these features are yet to be implemented or figured out:
* Figure out how to map parts of a MQTT topic to a field or tag. i.e. `/devicetype/<identifier>/temperature` to tag `identifier=<identifier>`.
* Map value-type MQTT messages to another InfluxDB type other than Text/String.
* Support MQTT paradigms such as [Homie](https://homieiot.github.io/).
* Figure out why InfluxDB2 requires an organisation to be sent.
* Figure out why an InfluxDB2 token with minimal write priviliges fails.
* Add documentation on how to run as a daemon on Linux.
