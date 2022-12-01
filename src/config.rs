use influxdb_rs::{Point, Value as DBValue};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub enum DstVariant {
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
pub struct JsonField {
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
pub enum Fields {
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
pub struct Entry {
    pub src_topic: String,
    pub dst_name: String,
    #[serde(flatten)]
    pub fields: Fields,
}

#[derive(Debug, Deserialize)]
pub struct Configuration {
    pub entries: Vec<Entry>,
}
