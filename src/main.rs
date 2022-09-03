use std::{fs::File, path::PathBuf, time::Duration};

use clap::Parser;
use influxdb_rs::{Point, Precision, Value as DBValue};
use rumqttc::{
    matches, AsyncClient, ConnAck, ConnectReturnCode, MqttOptions, Packet, Publish, QoS,
};
use serde::Deserialize;
use serde_json::Value;
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

#[derive(Debug, Deserialize)]
enum DstVariant {
    Field,
    Tag,
}

impl Default for DstVariant {
    fn default() -> Self {
        Self::Field
    }
}

impl DstVariant {
    pub fn write_to<'a>(&self, name: &str, value: DBValue<'a>, point: Point<'a>) -> Point<'a> {
        match self {
            DstVariant::Field => point.add_field(name, value),
            DstVariant::Tag => point.add_tag(name, value),
        }
    }
}

#[derive(Debug, Deserialize)]
struct JsonField {
    src_path: String,
    #[serde(default = "DstVariant::default")]
    dst_variant: DstVariant,
    dst_name: Option<String>,
}

impl JsonField {
    pub fn src_path_parts(&self) -> impl Iterator<Item = &str> {
        self.src_path.split('.')
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Fields {
    SingleText {
        #[serde(default = "DstVariant::default")]
        dst_variant: DstVariant,
        dst_name: String,
    },
    Json {
        fields: Vec<JsonField>,
    },
}

fn json_to_influxdb(value: &Value) -> DBValue<'static> {
    match value {
        Value::Null => unimplemented!(),
        Value::Bool(b) => DBValue::Boolean(*b),
        Value::Number(n) => DBValue::Float(n.as_f64().unwrap()),
        Value::String(s) => DBValue::String(s.to_owned().into()),
        Value::Array(a) => DBValue::String(serde_json::to_string(&a).unwrap().into()),
        Value::Object(o) => DBValue::String(serde_json::to_string(&o).unwrap().into()),
    }
}

impl Fields {
    pub fn extract<'a>(&self, value: &[u8], mut point: Point<'a>) -> Point<'a> {
        match self {
            Fields::SingleText {
                dst_variant,
                dst_name,
            } => {
                let value = std::str::from_utf8(value).unwrap().to_owned();
                let value = DBValue::String(value.into());

                point = dst_variant.write_to(dst_name, value, point);
            }
            Fields::Json { fields } => {
                let value: Value = serde_json::from_slice(value).unwrap();

                for field in fields {
                    let dst_name = field.dst_name.as_ref().unwrap_or(&field.src_path);

                    let mut value = &value;
                    for p in field.src_path_parts() {
                        if let Some(v) = match value {
                            Value::Array(a) => {
                                if let Ok(i) = p.parse::<usize>() {
                                    a.get(i)
                                } else {
                                    continue;
                                }
                            }
                            Value::Object(o) => o.get(p),
                            _ => continue,
                        } {
                            value = v;
                        } else {
                            continue;
                        }
                    }

                    let value = json_to_influxdb(value);
                    point = field.dst_variant.write_to(dst_name, value, point);
                }
            }
        }

        point
    }
}

#[derive(Debug, Deserialize)]
struct Entry {
    src_topic: String,
    dst_name: String,
    #[serde(flatten)]
    fields: Fields,
}

#[derive(Debug, Deserialize)]
struct Configuration {
    entries: Vec<Entry>,
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
