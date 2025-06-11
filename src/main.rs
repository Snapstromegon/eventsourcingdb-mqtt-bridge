use std::time::Duration;

use clap::Parser;
use eventsourcingdb::{
    client::{
        Client as DbClient,
        request_options::{Bound, BoundType, ObserveEventsRequestOptions},
    },
    event::{EventCandidate, TraceInfo},
};
use futures::StreamExt;
use rumqttc::{Incoming, Publish};
use tokio::{select, time::sleep};
use tracing::{info, warn};
use url::Url;

#[derive(Debug, Clone, Parser)]
struct EventSourcingDbArgs {
    #[clap(
        long("db-url"),
        env = "DB_URL",
        default_value = "http://localhost:3000"
    )]
    url: Url,
    #[clap(long("db-token"), env = "DB_TOKEN")]
    token: String,
}

#[derive(Debug, Clone, Parser)]
struct MqttArgs {
    #[clap(
        long("mqtt-id"),
        env = "MQTT_ID",
        default_value = "eventsourcingdb-bridge"
    )]
    id: String,
    #[clap(long("mqtt-host"), env = "MQTT_HOST", default_value = "localhost")]
    host: String,
    #[clap(long("mqtt-port"), env = "MQTT_PORT", default_value = "1883")]
    port: u16,
}

#[derive(Debug, Clone, Parser)]
struct Args {
    #[clap(flatten)]
    mqtt: MqttArgs,
    #[clap(flatten)]
    eventsourcingdb: EventSourcingDbArgs,
    #[clap(
        long,
        env = "MQTT_INPUT_TOPIC",
        default_value = "eventsourcingdb/input"
    )]
    mqtt_input_topic: String,
    #[clap(
        long,
        env = "MQTT_OUTPUT_TOPIC",
        default_value = "eventsourcingdb/output"
    )]
    mqtt_output_topic: String,
    #[clap(
        long,
        env = "MQTT_CONTROL_TOPIC",
        default_value = "eventsourcingdb/control"
    )]
    mqtt_control_topic: String,
    #[clap(long, env = "DB_SUBSCRIPTION", default_value = "/")]
    db_subscription: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        // .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(true)
        .init();
    info!("Starting eventsourcingdb bridge");
    if let Err(e) = dotenvy::dotenv() {
        warn!("Failed to load .env file: {}", e);
    }
    info!("Loaded environment variables");

    let args = Args::parse();
    info!("Parsed command line arguments");

    let db_client = DbClient::new(args.eventsourcingdb.url, args.eventsourcingdb.token);

    db_client.ping().await?;
    info!("Connected to EventSourcingDB");

    let mqtt_options = rumqttc::MqttOptions::new(args.mqtt.id, args.mqtt.host, args.mqtt.port)
        .set_keep_alive(Duration::from_secs(60))
        .set_clean_session(true)
        .to_owned();

    let (mqtt_client, mut mqtt_eventloop) = rumqttc::AsyncClient::new(mqtt_options, 10);

    mqtt_client
        .subscribe(
            format!("{}/#", args.mqtt_input_topic.trim_end_matches('/')),
            rumqttc::QoS::ExactlyOnce,
        )
        .await?;
    mqtt_client
        .subscribe(
            format!("{}/#", args.mqtt_control_topic.trim_end_matches('/')),
            rumqttc::QoS::ExactlyOnce,
        )
        .await?;

    info!("Subscribed to all MQTT topics");

    let (control_tx, control_rx) = tokio::sync::mpsc::unbounded_channel::<ControlMessage>();

    select! {
        res = sync_mqtt_to_db(control_tx, &mut mqtt_eventloop, &db_client, &args.mqtt_input_topic, &args.mqtt_control_topic) => {
            info!("MQTT event loop exited unexpectedly with result: {:?}", res);
        }
        res = sync_db_to_mqtt(
            control_rx,
            &db_client,
            &mqtt_client,
            &args.db_subscription,
            &args.mqtt_output_topic,
            &args.mqtt_control_topic
        ) => {
            if let Err(e) = res {
                warn!("Error syncing DB to MQTT: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down");
        }
    }

    Ok(())
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct EventCandidateWithoutSubject {
    data: serde_json::Value,
    source: String,
    #[serde(rename = "type")]
    ty: String,
    traceinfo: Option<TraceInfo>,
}

impl EventCandidateWithoutSubject {
    fn into_event_candidate(self, subject: String) -> EventCandidate {
        EventCandidate {
            data: self.data,
            source: self.source,
            subject,
            ty: self.ty,
            traceinfo: self.traceinfo,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum ControlMessage {
    LastEvent(String),
    InitDone,
}

async fn sync_mqtt_to_db(
    control_tx: tokio::sync::mpsc::UnboundedSender<ControlMessage>,
    mqtt_eventloop: &mut rumqttc::EventLoop,
    db_client: &DbClient,
    mqtt_input_topic: &str,
    mqtt_control_topic: &str,
) -> anyhow::Result<()> {
    loop {
        if let rumqttc::Event::Incoming(Incoming::Publish(Publish { topic, payload, .. })) =
            mqtt_eventloop.poll().await?
        {
            info!("Received MQTT publish: {:?}", topic);
            if topic.starts_with(mqtt_input_topic) {
                let received_candidate: EventCandidateWithoutSubject =
                    serde_json::from_slice(&payload).map_err(|e| {
                        warn!("Failed to deserialize MQTT payload: {}", e);
                        e
                    })?;

                let subject = format!(
                    "/{}",
                    topic
                        .clone()
                        .trim_start_matches(mqtt_input_topic)
                        .trim_matches('/')
                );
                info!("Received event candidate for subject: {}", subject);
                db_client
                    .write_events(
                        vec![received_candidate.into_event_candidate(subject)],
                        vec![],
                    )
                    .await?;
            } else if topic.starts_with(mqtt_control_topic) {
                info!("Received control message on topic: {}", topic);
                let control_message: ControlMessage = serde_json::from_slice(&payload)?;
                control_tx.send(control_message)?;
            } else {
                warn!("Received MQTT publish on unknown topic: {}", topic);
            }
        }
    }
}

async fn sync_db_to_mqtt(
    mut control_rx: tokio::sync::mpsc::UnboundedReceiver<ControlMessage>,
    db_client: &DbClient,
    mqtt_client: &rumqttc::AsyncClient,
    db_subscription: &str,
    mqtt_output_topic: &str,
    mqtt_control_topic: &str,
) -> anyhow::Result<()> {
    let mut last_event = None;
    #[allow(unused_assignments)]
    let mut last_event_id = "".to_string();
    let mut init_done = false;
    sleep(std::time::Duration::from_secs(1)).await; // Wait a bit before sending init done
    mqtt_client
        .publish(
            mqtt_control_topic,
            rumqttc::QoS::AtLeastOnce,
            false,
            serde_json::to_string(&ControlMessage::InitDone)?,
        )
        .await?;
    while !init_done {
        if let Some(control_message) = control_rx.recv().await {
            match control_message {
                ControlMessage::LastEvent(event_id) => {
                    info!("Received last event request for event ID: {}", event_id);
                    last_event_id = event_id;
                    last_event = Some(Bound {
                        id: &last_event_id,
                        bound_type: BoundType::Exclusive,
                    });
                }
                ControlMessage::InitDone => {
                    info!("Initialization done, starting DB subscription");
                    init_done = true;
                }
            }
        }
    }

    let mut subscription = db_client
        .observe_events(
            db_subscription,
            Some(ObserveEventsRequestOptions {
                recursive: true,
                from_latest_event: None,
                lower_bound: last_event,
            }),
        )
        .await?;

    while let Some(event) = subscription.next().await {
        match event {
            Ok(event) => {
                info!(
                    "Received event from DB: {} on {}",
                    event.id(),
                    event.subject()
                );
                let topic = format!(
                    "{}/{}",
                    mqtt_output_topic,
                    event.subject().trim_start_matches('/')
                );
                mqtt_client
                    .publish(
                        topic,
                        rumqttc::QoS::AtLeastOnce,
                        false,
                        serde_json::to_string(&event)?,
                    )
                    .await?;
                mqtt_client
                    .publish(
                        mqtt_control_topic,
                        rumqttc::QoS::AtLeastOnce,
                        true,
                        serde_json::to_string(&ControlMessage::LastEvent(event.id().to_string()))?,
                    )
                    .await?;
            }
            Err(e) => {
                warn!("Error receiving event from DB: {}", e);
            }
        }
    }

    Ok(())
}
