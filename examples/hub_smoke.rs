//! Prueba de humo del hub MQTT, SIN radio.
//!
//! Usa exactamente el mismo cliente (websockets + TLS opcional) que el agente,
//! asi que si esto pasa, la mitad MQTT del bridge esta validada. Sirve tambien
//! para que un tercero verifique su hub antes de conectar el nodo.
//!
//!   CHAN_MQTT_HOST=hub.ejemplo.cl CHAN_MQTT_USER=... CHAN_MQTT_PASS=... \
//!   CHANNEL_NAME=publica ISLAND_IATA=SCL cargo run --example hub_smoke
//!
//! Publica un mensaje meshchan valido al topic y espera recibirlo de vuelta por
//! su propia suscripcion.

use std::time::Duration;

use anyhow::{bail, Result};
use bridge_msc::{bridge, config::Config, mqtt};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Config::from_env()?;
    // Con varios canales (CHANNELS) se prueba el primero.
    let canal = &cfg.channels[0];
    let topic = cfg.topic(canal);
    println!("hub   : {}", cfg.chan_mqtt.descripcion());
    println!("topic : {topic}");

    let (tx, mut rx) = mpsc::channel::<mqtt::Incoming>(16);
    let hub = mqtt::spawn(
        "smoke",
        &cfg.chan_mqtt,
        "meshchan-smoke",
        &cfg.chan_mqtt.user,
        vec![topic.clone()],
        Some(tx),
    )?;

    // Dale un momento al CONNACK y a la suscripcion antes de publicar.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let ts = bridge::now_epoch();
    let texto = format!("prueba de humo {ts}");
    let sender_id = "smoke-test";
    let msg = bridge::ChanMessage {
        v: bridge::PROTOCOL_VERSION,
        id: bridge::message_id(&canal.name, sender_id, &texto, ts),
        channel: canal.name.clone(),
        text: texto.clone(),
        sender: "smoke".into(),
        sender_id: sender_id.into(),
        // Pubkey falsa: un bridge de verdad la ignoraria por ser ajena, que es
        // lo que queremos para no inyectar nada a ninguna radio.
        origin_bridge: "0".repeat(64),
        origin_island: cfg.island_iata.clone(),
        ts,
    };
    println!("publicando id={} ...", msg.id);
    hub.publish(&topic, serde_json::to_vec(&msg)?).await?;

    match tokio::time::timeout(Duration::from_secs(15), rx.recv()).await {
        Ok(Some(inc)) => {
            let vuelta: bridge::ChanMessage = serde_json::from_slice(&inc.payload)?;
            if vuelta.id != msg.id {
                bail!("volvio otro mensaje: {} != {}", vuelta.id, msg.id);
            }
            println!("OK: ida y vuelta completa. text={:?}", vuelta.text);
            Ok(())
        }
        Ok(None) => bail!("el canal de entrada se cerro"),
        Err(_) => bail!(
            "no volvio nada en 15 s. Revisa credenciales, ACLs del topic {topic} \
             y que el broker acepte websockets en esa ruta"
        ),
    }
}
