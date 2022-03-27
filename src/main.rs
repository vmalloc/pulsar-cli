use anyhow::{format_err, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use colored_json::to_colored_json_auto;
use futures::TryStreamExt;
use itertools::Itertools;
use log::{info, LevelFilter};
use pulsar::{consumer::InitialPosition, ConsumerOptions, Pulsar, SubType, TokioExecutor};
use serde_json::{json, Value};
use std::{collections::HashMap, time::Duration};
use structopt::StructOpt;
use termion::color;
use url::Url;

#[derive(StructOpt)]
struct Opts {
    #[structopt(long, default_value = "pulsar://127.0.0.1")]
    url: Url,
    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt)]
enum Command {
    Consume {
        #[structopt(long)]
        topic: String,

        #[structopt(long, short = "s", default_value = "pulsar-cli")]
        subscription_name: String,

        #[structopt(long, short = "c", default_value = "pulsar-cli")]
        consumer_name: String,

        #[structopt(long)]
        durable: bool,

        #[structopt(long)]
        json: bool,

        #[structopt(long)]
        shared: bool,

        #[structopt(long)]
        earliest: bool,

        #[structopt(long)]
        ack: bool,

        #[structopt(long)]
        forward_to_topic: Option<String>,

        #[structopt(long)]
        forward_to_url: Option<Url>,
    },

    Produce {
        #[structopt(long)]
        topic: String,

        #[structopt(long, short = "p", default_value = "pulsar-cli")]
        producer_name: String,

        #[structopt(long, default_value = "5s")]
        interval: humantime::Duration,

        #[structopt(long = "prop")]
        properties: Vec<String>,
    },
}

async fn entry_point(opts: Opts) -> Result<()> {
    let retry_policy = again::RetryPolicy::exponential(Duration::from_secs(1));

    match &opts.command {
        Command::Consume {
            subscription_name,
            consumer_name,
            topic,
            durable,
            earliest,
            json,
            forward_to_topic,
            forward_to_url,
            shared,
            ack,
        } => {
            let mut consumer = retry_policy
                .retry(|| async {
                    let builder = Pulsar::builder(opts.url.as_str(), TokioExecutor)
                        .build()
                        .await
                        .map_err(|e| {
                            log::error!("Failed connecting to Pulsar: {:?}", e);
                            e
                        })?
                        .consumer()
                        .with_consumer_name(consumer_name)
                        .with_subscription(subscription_name)
                        .with_subscription_type(if *shared {
                            SubType::Shared
                        } else {
                            SubType::Exclusive
                        })
                        .with_topic(topic)
                        .with_options(ConsumerOptions {
                            durable: Some(*durable),
                            initial_position: if *earliest {
                                InitialPosition::Earliest
                            } else {
                                InitialPosition::default()
                            },
                            ..Default::default()
                        });

                    builder.build::<Vec<u8>>().await.map_err(|e| {
                        log::error!("Error trying to connect: {:?}. Retrying...", e);
                        e
                    })
                })
                .await?;

            let mut forward_producer = if let Some(topic) = forward_to_topic {
                let url = forward_to_url.as_ref().unwrap_or(&opts.url);
                Some(
                    Pulsar::builder(url.as_str(), TokioExecutor)
                        .build()
                        .await?
                        .producer()
                        .with_topic(topic)
                        .build()
                        .await?,
                )
            } else {
                None
            };

            loop {
                if let Some(message) = consumer.try_next().await? {
                    let publish_time = message
                        .metadata()
                        .event_time
                        .unwrap_or_else(|| message.metadata().publish_time);
                    let publish_time = DateTime::<Utc>::from_utc(
                        NaiveDateTime::from_timestamp(
                            (publish_time / 1000) as i64,
                            ((publish_time % 1000) * 1_000_000) as u32,
                        ),
                        Utc,
                    );
                    println!("-- {}:", publish_time);
                    if !message.metadata().properties.is_empty() {
                        for item in message.metadata().properties.iter() {
                            println!(
                                "{}{}={}{}",
                                color::Fg(color::Magenta),
                                item.key,
                                item.value,
                                color::Fg(color::Reset)
                            );
                        }
                    }
                    if *json {
                        match serde_json::from_slice::<Value>(&message.payload.data) {
                            Ok(val) => println!("{}", to_colored_json_auto(&val).unwrap()),
                            Err(_) => eprintln!(
                                "{}Value {:?} is not JSON{}",
                                color::Fg(color::Red),
                                String::from_utf8_lossy(&message.payload.data),
                                color::Fg(color::Reset)
                            ),
                        }
                    } else {
                        println!("{}", String::from_utf8_lossy(&message.payload.data));
                    }

                    if let Some(forwarder) = forward_producer.as_mut() {
                        forwarder
                            .send(pulsar::producer::Message {
                                payload: message.payload.data.clone(),
                                properties: message
                                    .payload
                                    .metadata
                                    .properties
                                    .iter()
                                    .cloned()
                                    .map(|i| (i.key, i.value))
                                    .collect(),
                                event_time: Some(publish_time.timestamp_millis() as u64),
                                ..Default::default()
                            })
                            .await?;
                    }

                    if *ack {
                        consumer.ack(&message).await?;
                    }
                }
            }
        }

        Command::Produce {
            topic,
            producer_name,
            interval,
            properties,
        } => {
            let properties = properties
                .iter()
                .map(|attr| {
                    let (key, value) = attr
                        .splitn(2, '=')
                        .tuples()
                        .next()
                        .ok_or_else(|| format_err!("Invalid attr: {:?}", attr))?;
                    Ok((key.to_owned(), value.to_owned()))
                })
                .collect::<Result<HashMap<_, _>>>()?;

            let mut producer = retry_policy
                .retry(|| async {
                    Pulsar::builder(opts.url.as_str(), TokioExecutor)
                        .build()
                        .await?
                        .producer()
                        .with_topic(topic)
                        .with_name(producer_name)
                        .build()
                        .await
                })
                .await?;
            info!("Connected to Pulsar");
            for i in 0.. {
                tokio::time::sleep((*interval).into()).await;
                let payload = serde_json::to_vec(&json!({
                    "iteration": i,
                    "timestamp": Utc::now(),
                }))?;
                let properties = properties.clone();

                let message = pulsar::producer::Message {
                    payload,
                    properties,
                    ..Default::default()
                };

                loop {
                    match tokio::time::timeout(
                        Duration::from_secs(30),
                        producer.send(message.clone()),
                    )
                    .await
                    .map_err(|_| anyhow::format_err!("Timeout"))
                    .and_then(|r| r.map_err(anyhow::Error::from))
                    {
                        Ok(_) => {
                            info!("Published message #{}", i);
                            break;
                        }
                        Err(e) => info!("Error publishing message: {:?} ", e),
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await
                }
            }
            Ok(())
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::from_args();
    env_logger::Builder::new()
        .filter_level(LevelFilter::Debug)
        .init();

    entry_point(opts).await
}
