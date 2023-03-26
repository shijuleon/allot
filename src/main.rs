use aws_sdk_dynamodb as dynamodb;
use std::collections::HashMap;
use std::io::BufReader;
use std::path::PathBuf;
use std::{fs::File, io::Read};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[tokio::main]
async fn main() {
  ()
}

#[derive(Serialize, Deserialize, Debug)]
struct Host {
  identifier: String,
  capacity: String,
  used: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct ClusterInfo {
  cluster_id: String,
  saturated_hosts_count: String,
  cluster: String,
  hosts: Vec<Host>,
  // version: String,
}

impl From<Host> for HashMap<String, dynamodb::model::AttributeValue> {
  fn from(h: Host) -> Self {
    HashMap::from([
      (
        "identifier".to_string(),
        dynamodb::model::AttributeValue::S(h.identifier),
      ),
      (
        "capacity".to_string(),
        dynamodb::model::AttributeValue::N(h.capacity),
      ),
      (
        "used".to_string(),
        dynamodb::model::AttributeValue::N(h.used),
      ),
    ])
  }
}

impl From<ClusterInfo> for HashMap<String, dynamodb::model::AttributeValue> {
  fn from(c: ClusterInfo) -> Self {
    let mut v: Vec<dynamodb::model::AttributeValue> = Vec::new();
    for h in c.hosts {
      v.push(dynamodb::model::AttributeValue::M(h.try_into().unwrap()));
    }

    let m = HashMap::from([
      (
        "cluster_id".to_string(),
        dynamodb::model::AttributeValue::N(c.cluster_id),
      ),
      (
        "saturated_hosts_count".to_string(),
        dynamodb::model::AttributeValue::N(c.saturated_hosts_count),
      ),
      (
        "cluster".to_string(),
        dynamodb::model::AttributeValue::S(c.cluster),
      ),
      ("hosts".to_string(), dynamodb::model::AttributeValue::L(v)),
    ]);
    m
  }
}

fn read_hosts_from_json(f: File) -> Result<ClusterInfo, std::io::Error> {
  let mut buf_reader = BufReader::new(f);
  let mut contents = String::new();
  buf_reader.read_to_string(&mut contents)?;

  let v: ClusterInfo = serde_json::from_str(&contents)?;

  Ok(v)
}

fn read_hosts_from_path(path: PathBuf) -> std::io::Result<File> {
  let file = File::open(path)?;
  Ok(file)
}

fn validate_hosts(hosts: Vec<Host>) {
  for host in hosts {
    if host.used > host.capacity {
      eprintln!("Used can't be greater than capacity");
    }
  }
}

async fn update_hosts(
  mut item: HashMap<String, dynamodb::model::AttributeValue>,
) -> Result<(), dynamodb::Error> {
  let config = aws_config::from_env()
    .endpoint_url("http://localhost:4566")
    .load()
    .await;
  let client = dynamodb::Client::new(&config);

  let version = Uuid::new_v4();
  item.insert("version".to_string(), dynamodb::model::AttributeValue::S(version.to_string()));

  let r = client
    .put_item()
    .table_name("servers".to_string())
    .set_item(Some(item));
  let resp = r.send().await?;
  let attributes = resp.attributes();

  println!("{:?}", attributes);
  Ok(())
}

async fn create_table() -> Result<(), dynamodb::Error> {
  let table = "servers";
  let key = "cluster_id";

  let config = aws_config::from_env()
    .endpoint_url("http://localhost:4566")
    .load()
    .await;
  let client = dynamodb::Client::new(&config);

  let a_name: String = key.into();
  let table_name: String = table.into();

  let ad = dynamodb::model::AttributeDefinition::builder()
    .attribute_name(&a_name)
    .attribute_type(dynamodb::model::ScalarAttributeType::N)
    .build();

  let ks = dynamodb::model::KeySchemaElement::builder()
    .attribute_name(&a_name)
    .key_type(dynamodb::model::KeyType::Hash)
    .build();

  let pt = dynamodb::model::ProvisionedThroughput::builder()
    .read_capacity_units(10)
    .write_capacity_units(5)
    .build();

  let create_table_response = client
    .create_table()
    .table_name(table_name)
    .key_schema(ks)
    .attribute_definitions(ad)
    .provisioned_throughput(pt)
    .send()
    .await;

  match create_table_response {
    Ok(_) => {
      println!("Added table {} with key {}", table, key);
      ()
    }
    Err(e) => {
      eprintln!("Got an error creating table:");
      eprintln!("{}", e);
      ()
    }
  }

  Ok(())
}

#[tokio::test]
async fn test_read_path() {
  let f = read_hosts_from_path(PathBuf::from("example.json")).unwrap();
  let hosts = read_hosts_from_json(f).unwrap();
  let m = hosts.try_into().unwrap();
  println!("{:?}", m);
  // create_table().await.unwrap();
  update_hosts(m).await.unwrap();
}
