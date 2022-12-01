pub mod config;

use std::{fs::File, path::PathBuf, time::Duration};

use clap::Parser;
use config::Configuration;
use influxdb_rs::{Point, Precision};
use rumqttc::{
    matches, AsyncClient, ConnAck, ConnectReturnCode, MqttOptions, Packet, Publish, QoS,
};
use url::Url;

#[derive(Parser)]
#[clap(version, about)]
struct Args {
    /// Url for the MQTT server to connect to.
    #[clap(env, long, default_value = "mqtt://localhost")]
    mqtt_url: Url,

    /// Client ID used by this application to identify itself to the MQTT server.
    #[clap(env, long, default_value = "mqtt2influxdb")]
    mqtt_client_id: String,

    /// Url for the InfluxDB2 server to connect to.
    #[clap(env, long, default_value = "http://localhost:8086")]
    influxdb_url: Url,

    /// InfluxDB2 bucket to write all the data to.
    #[clap(env, long)]
    influxdb_bucket: String,

    /// InfluxDB2 organization for the database.
    #[clap(env, long)]
    influxdb_org: String, // (why do we need to send this?)

    /// InfluxDB2 secret token for the account to use.
    #[clap(env, long)]
    influxdb_jwt: String,

    /// Path to the mapping configuration file used to translate MQTT messages to InfluxDB2 points.
    #[clap(env, long)]
    config: PathBuf,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = Args::parse();

    let configuration =
        serde_yaml::from_reader::<_, Configuration>(File::open(args.config).unwrap()).unwrap();

    let mut mqtt_url = args.mqtt_url.clone();
    mqtt_url
        .query_pairs_mut()
        .append_pair("client_id", &args.mqtt_client_id);

    log::debug!("Connecting to MQTT server: {}", mqtt_url);

    let mut options = MqttOptions::parse_url(mqtt_url).unwrap();
    options.set_keep_alive(Duration::from_secs(5));

    log::debug!("Connecting to InfluxDB server: {}", args.influxdb_url);

    let influxdb = influxdb_rs::Client::new(
        args.influxdb_url,
        args.influxdb_bucket,
        args.influxdb_org,
        args.influxdb_jwt,
    )
    .await
    .unwrap();

    if let Ok(true) = influxdb.ping().await.await {
        log::info!("Successfully pinged InfluxDB");
    } else {
        log::error!("Failed to ping InfluxDB");
    }

    let (mqtt_client, mut mqtt_eventloop) = AsyncClient::new(options, 10);

    for e in configuration.entries.iter() {
        mqtt_client
            .subscribe(&e.src_topic, QoS::AtMostOnce)
            .await
            .unwrap();
    }

    loop {
        let notification = mqtt_eventloop.poll().await.unwrap();

        match notification {
            rumqttc::Event::Incoming(Packet::ConnAck(ConnAck {
                code: ConnectReturnCode::Success,
                ..
            })) => {
                log::info!("Connected to MQTT");
            }
            rumqttc::Event::Incoming(Packet::Publish(Publish { topic, payload, .. })) => {
                if let Some(entry) = configuration
                    .entries
                    .iter()
                    .find(|e| matches(&topic, &e.src_topic))
                {
                    let point = Point::new(&entry.dst_name);
                    let point = entry.fields.extract(&payload, point);
                    log::info!("Received {:?}", point);

                    if let Err(e) = influxdb
                        .write_point(point, Some(Precision::Milliseconds), None)
                        .await
                    {
                        log::error!("Failed to query InfluxDB: {:?}", e);
                    }
                }
            }
            rumqttc::Event::Incoming(_) => {}
            rumqttc::Event::Outgoing(_) => {}
        }
    }
}
