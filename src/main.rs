use anyhow::Result;
use chrono::Utc;
use colored_json::to_colored_json_auto;
use futures::TryStreamExt;
use pulsar::{
    proto::command_subscribe::InitialPosition, ConsumerOptions, Pulsar, SubType, TokioExecutor,
};
use serde_json::{json, Value};
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
        subscriber_name: String,

        #[structopt(long)]
        latest_message: bool,

        #[structopt(long)]
        json: bool,
        #[structopt(long)]
        shared: bool,

        #[structopt(long)]
        ack: bool,
    },

    Publish {
        #[structopt(long)]
        topic: String,

        #[structopt(long, short = "p", default_value = "pulsar-cli")]
        producer_name: String,

        #[structopt(long, default_value = "5s")]
        interval: humantime::Duration,
    },
}

async fn entry_point(opts: Opts) -> Result<()> {
    match opts.command {
        Command::Consume {
            subscriber_name,
            topic,
            latest_message,
            json,
            shared,
            ack,
        } => {
            let mut builder = Pulsar::builder(opts.url.as_str(), TokioExecutor)
                .build()
                .await?
                .consumer()
                .with_subscription(subscriber_name)
                .with_subscription_type(if shared {
                    SubType::Shared
                } else {
                    SubType::Exclusive
                })
                .with_topic(topic);

            if latest_message {
                builder = builder.with_options(ConsumerOptions {
                    initial_position: Some(InitialPosition::Latest as i32),
                    ..Default::default()
                });
            }
            let mut consumer = builder.build::<Vec<u8>>().await?;

            while let Some(message) = consumer.try_next().await? {
                println!("--");
                if json {
                    match serde_json::from_slice::<Value>(&message.payload.data) {
                        Ok(val) => println!("{}", to_colored_json_auto(&val).unwrap()),
                        Err(_) => eprintln!(
                            "{}Value {:?} is not JSON",
                            color::Fg(color::Red),
                            String::from_utf8_lossy(&message.payload.data)
                        ),
                    }
                } else {
                    println!("{}", String::from_utf8_lossy(&message.payload.data));
                }
                if ack {
                    consumer.ack(&message).await?;
                }
            }
            Ok(())
        }

        Command::Publish {
            topic,
            producer_name,
            interval,
        } => {
            let mut producer = Pulsar::builder(opts.url.as_str(), TokioExecutor)
                .build()
                .await?
                .producer()
                .with_topic(topic)
                .with_name(producer_name)
                .build()
                .await?;

            for i in 0.. {
                tokio::time::delay_for(interval.into()).await;
                producer
                    .send(
                        serde_json::to_vec(&json!({
                            "iteration": i,
                            "timestamp": Utc::now(),
                        }))?
                        .as_slice(),
                    )
                    .await?;
            }
            Ok(())
        }
    }
}

#[tokio::main]
async fn main() {
    let opts = Opts::from_args();

    entry_point(opts).await.expect("Error encountered")
}
