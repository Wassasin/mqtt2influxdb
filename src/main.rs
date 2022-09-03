use std::{collections::BTreeMap, fs::File, path::PathBuf, time::Duration};

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
    #[clap(long, default_value = "mqtt://localhost?client_id=slimmemeter")]
    mqtt_url: Url,

    #[clap(long, default_value = "http://localhost:8086")]
    influxdb_url: Url,

    #[clap(long)]
    influxdb_bucket: String,

    #[clap(long)]
    influxdb_org: String,

    #[clap(long)]
    influxdb_jwt: String,

    #[clap(long)]
    config: PathBuf,
}

#[derive(Debug, Deserialize)]
struct JsonField {
    src_path: String,
    dst_field: Option<String>,
}

impl JsonField {
    pub fn src_path_parts(&self) -> impl Iterator<Item = &str> {
        self.src_path.split('.')
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Fields {
    SingleText { dst_field: String },
    Json { fields: Vec<JsonField> },
}

fn json_to_influxdb(value: Value) -> DBValue<'static> {
    match value {
        Value::Null => unimplemented!(),
        Value::Bool(b) => DBValue::Boolean(b),
        Value::Number(n) => DBValue::Float(n.as_f64().unwrap()),
        Value::String(s) => DBValue::String(s.into()),
        Value::Array(a) => DBValue::String(serde_json::to_string(&a).unwrap().into()),
        Value::Object(o) => DBValue::String(serde_json::to_string(&o).unwrap().into()),
    }
}

impl Fields {
    pub fn extract(&self, value: &[u8]) -> BTreeMap<String, DBValue> {
        match self {
            Fields::SingleText { dst_field } => {
                let value = std::str::from_utf8(value).unwrap().to_owned();
                std::iter::once((dst_field.clone(), DBValue::String(value.into()))).collect()
            }
            Fields::Json { fields } => {
                let value: Value = serde_json::from_slice(value).unwrap();

                fields
                    .iter()
                    .filter_map(|field| {
                        let dst_field = field
                            .dst_field
                            .as_ref()
                            .unwrap_or_else(|| &field.src_path)
                            .clone();

                        let mut value = &value;
                        for p in field.src_path_parts() {
                            if let Some(v) = match value {
                                Value::Array(a) => {
                                    if let Ok(i) = p.parse::<usize>() {
                                        a.get(i)
                                    } else {
                                        return None;
                                    }
                                }
                                Value::Object(o) => o.get(p),
                                _ => return None,
                            } {
                                value = v;
                            } else {
                                return None;
                            }
                        }

                        Some((dst_field, json_to_influxdb(value.clone())))
                    })
                    .collect()
            }
        }
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

    let mut options = MqttOptions::parse_url(args.mqtt_url).unwrap();
    options.set_keep_alive(Duration::from_secs(5));

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
                    let values = entry.fields.extract(&payload);
                    log::info!("Received {:?} {:?}", entry, values);

                    let mut point = Point::new(&entry.dst_name);

                    for (field, value) in values {
                        point = point.add_field(field, value);
                    }

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
