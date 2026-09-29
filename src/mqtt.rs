//! Clientes MQTT sobre websockets (+TLS) con reconexion automatica.
//!
//! Se usan hasta dos: el hub del canal (siempre) y el broker de observabilidad
//! (solo si OBS_ENABLED). Cada uno corre su EventLoop en su propia task.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, TlsConfiguration, Transport};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::{MqttEndpoint, MqttTransport};

/// Un publish entrante ya desempaquetado.
#[derive(Debug, Clone)]
pub struct Incoming {
    pub topic: String,
    pub payload: Vec<u8>,
}

/// Handle para publicar. Clonable y barato.
#[derive(Clone)]
pub struct MqttHandle {
    client: AsyncClient,
    name: &'static str,
}

impl MqttHandle {
    pub async fn publish(&self, topic: &str, payload: Vec<u8>) -> Result<()> {
        self.client
            .publish(topic, QoS::AtLeastOnce, false, payload)
            .await
            .with_context(|| format!("publish a {topic} en el broker {}", self.name))?;
        Ok(())
    }
}

/// Instala el proveedor criptografico de rustls una sola vez por proceso.
/// Sin esto `ClientConfig::builder()` entra en panico con "no process-level
/// CryptoProvider available" (usamos rumqttc con `use-rustls-no-provider`).
pub fn init_crypto() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn tls_config(ep: &MqttEndpoint) -> Result<TlsConfiguration> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    // CA propia opcional, para un broker con certificado autofirmado.
    if let Some(path) = &ep.ca_file {
        let pem = std::fs::read(path).with_context(|| format!("leyendo la CA {path}"))?;
        let mut cursor = std::io::Cursor::new(pem);
        let mut agregados = 0usize;
        for cert in rustls_pemfile::certs(&mut cursor) {
            roots
                .add(cert.context("certificado invalido en el PEM de la CA")?)
                .context("agregando la CA al almacen de raices")?;
            agregados += 1;
        }
        if agregados == 0 {
            anyhow::bail!("{path} no contiene ningun certificado");
        }
        info!(ca = %path, certificados = agregados, "CA propia cargada");
    }

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsConfiguration::Rustls(Arc::new(config)))
}

/// Levanta un cliente MQTT y su EventLoop.
///
/// - `client_id` debe ser unico por conexion contra el mismo broker.
/// - `subscribe` (uno por canal) se re-suscribe solo en cada reconexion (en el CONNACK).
/// - `incoming_tx` recibe los publish; pasa `None` si el cliente es solo de escritura.
pub fn spawn(
    name: &'static str,
    ep: &MqttEndpoint,
    client_id: &str,
    user: &str,
    subscribe: Vec<String>,
    incoming_tx: Option<mpsc::Sender<Incoming>>,
) -> Result<MqttHandle> {
    init_crypto();

    // Con websockets rumqttc espera la URL completa en `broker_addr` e ignora el
    // puerto del constructor; con MQTT nativo espera solo el host y si usa el
    // puerto. `broker_addr()` devuelve lo que corresponde a cada caso.
    let mut opts = MqttOptions::new(client_id, ep.broker_addr(), ep.port);
    opts.set_transport(match (ep.transport, ep.tls) {
        (MqttTransport::Ws, true) => Transport::Wss(tls_config(ep)?),
        (MqttTransport::Ws, false) => Transport::Ws,
        (MqttTransport::Tcp, true) => Transport::Tls(tls_config(ep)?),
        (MqttTransport::Tcp, false) => Transport::Tcp,
    });
    opts.set_keep_alive(Duration::from_secs(45));
    opts.set_clean_session(true);
    // Techo del buffer de salida: si el broker esta caido, se descartan publishes
    // en vez de crecer sin limite.
    opts.set_max_packet_size(64 * 1024, 64 * 1024);
    if !user.is_empty() {
        opts.set_credentials(user, &ep.pass);
    }

    let (client, mut eventloop) = AsyncClient::new(opts, 64);
    let handle = MqttHandle {
        client: client.clone(),
        name,
    };

    info!(broker = name, destino = %ep.descripcion(), client_id = %client_id, "conectando al broker MQTT");

    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    backoff = Duration::from_secs(1);
                    info!(broker = name, "conectado");
                    for topic in &subscribe {
                        match client.subscribe(topic.clone(), QoS::AtLeastOnce).await {
                            Ok(()) => info!(broker = name, topic = %topic, "suscrito"),
                            Err(e) => warn!(broker = name, error = %e, "no se pudo suscribir"),
                        }
                    }
                }
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    if let Some(tx) = &incoming_tx {
                        let msg = Incoming {
                            topic: p.topic.clone(),
                            payload: p.payload.to_vec(),
                        };
                        // try_send: si el consumidor se atraso, botamos el
                        // mensaje en vez de trabar el EventLoop.
                        if tx.try_send(msg).is_err() {
                            warn!(broker = name, topic = %p.topic, "cola de entrada llena, mensaje descartado");
                        }
                    }
                }
                Ok(ev) => debug!(broker = name, ?ev, "evento mqtt"),
                Err(e) => {
                    warn!(broker = name, error = %e, reintento_en = ?backoff, "conexion MQTT caida");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                }
            }
        }
    });

    Ok(handle)
}
