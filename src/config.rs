//! Configuracion por variables de entorno. Ver `.env.example`.

use anyhow::{bail, Context, Result};
use std::time::Duration;

/// Transporte hacia el nodo MeshCore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transport {
    Serial,
    Tcp,
    Ble,
}

/// Como se habla con el broker.
///
/// La spec §4 recomienda websockets con TLS, pero permite MQTT nativo. El
/// puerto nativo es lo que espera cualquier cliente MQTT estandar
/// (`mosquitto_sub`, MQTT Explorer, otras implementaciones del bridge).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqttTransport {
    /// MQTT sobre websockets (ws / wss).
    Ws,
    /// MQTT nativo sobre TCP (1883 / 8883).
    Tcp,
}

/// Datos de conexion a un broker MQTT.
#[derive(Debug, Clone)]
pub struct MqttEndpoint {
    pub host: String,
    pub port: u16,
    pub transport: MqttTransport,
    /// Ruta del websocket. Se ignora con transporte tcp.
    pub ws_path: String,
    pub tls: bool,
    pub user: String,
    pub pass: String,
    /// PEM opcional con una CA propia (broker con certificado autofirmado).
    pub ca_file: Option<String>,
}

impl MqttEndpoint {
    /// Lo que rumqttc espera en `broker_addr`: la URL completa con websockets,
    /// y solo el host con TCP nativo.
    pub fn broker_addr(&self) -> String {
        match self.transport {
            MqttTransport::Tcp => self.host.clone(),
            MqttTransport::Ws => {
                let scheme = if self.tls { "wss" } else { "ws" };
                let path = if self.ws_path.starts_with('/') {
                    self.ws_path.clone()
                } else {
                    format!("/{}", self.ws_path)
                };
                format!("{}://{}:{}{}", scheme, self.host, self.port, path)
            }
        }
    }

    /// Como se ve la conexion en los logs.
    pub fn descripcion(&self) -> String {
        match self.transport {
            MqttTransport::Tcp => format!(
                "{}://{}:{}",
                if self.tls { "mqtts" } else { "mqtt" },
                self.host,
                self.port
            ),
            MqttTransport::Ws => self.broker_addr(),
        }
    }
}

fn env_transport(key: &str) -> Result<MqttTransport> {
    match env_or(key, "ws").to_ascii_lowercase().as_str() {
        "ws" | "websocket" | "websockets" => Ok(MqttTransport::Ws),
        "tcp" | "mqtt" => Ok(MqttTransport::Tcp),
        otro => bail!("{key} invalido: {otro} (ws|tcp)"),
    }
}

#[derive(Debug, Clone)]
pub struct ObsConfig {
    pub endpoint: MqttEndpoint,
    pub iata: String,
    pub gateway_pubkey: Option<String>,
    pub interval: Duration,
}

#[derive(Debug, Clone)]
pub struct Config {
    // --- nodo ---
    pub transport: Transport,
    pub address: String,
    pub baud: u32,

    // --- canal ---
    pub channel_name: String,
    pub channel_idx: u8,
    pub island_iata: String,
    pub topic_prefix: String,
    /// Segmento OPCIONAL de region entre el prefijo y el canal (spec §5).
    pub topic_region: String,
    pub chan_mqtt: MqttEndpoint,

    // --- anti-abuso ---
    pub dedup_ttl: Duration,
    pub dedup_max: usize,
    pub rate_per_min: u32,
    pub rate_burst: u32,
    pub rate_min_interval: Duration,
    pub max_text_len: usize,

    // --- observabilidad (opcional) ---
    pub obs: Option<ObsConfig>,
}

fn env_opt(key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(v) if !v.trim().is_empty() => Some(v.trim().to_string()),
        _ => None,
    }
}

fn env_req(key: &str) -> Result<String> {
    env_opt(key).with_context(|| format!("falta la variable de entorno {key}"))
}

fn env_or(key: &str, default: &str) -> String {
    env_opt(key).unwrap_or_else(|| default.to_string())
}

fn env_num<T: std::str::FromStr>(key: &str, default: T) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    match env_opt(key) {
        None => Ok(default),
        Some(v) => v
            .parse::<T>()
            .map_err(|e| anyhow::anyhow!("{key} no es un numero valido ({v}): {e}")),
    }
}

fn env_bool(key: &str, default: bool) -> bool {
    match env_opt(key) {
        None => default,
        Some(v) => matches!(
            v.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "y" | "on" | "si"
        ),
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let transport = match env_or("MC_TRANSPORT", "serial").to_ascii_lowercase().as_str() {
            "serial" => Transport::Serial,
            "tcp" => Transport::Tcp,
            "ble" => Transport::Ble,
            other => bail!("MC_TRANSPORT invalido: {other} (serial|tcp|ble)"),
        };

        let address = env_opt("MC_ADDRESS").unwrap_or_else(|| match transport {
            Transport::Serial => "/dev/ttyUSB0".to_string(),
            Transport::Tcp => "127.0.0.1:5000".to_string(),
            Transport::Ble => String::new(),
        });
        if transport == Transport::Ble && address.is_empty() {
            bail!("MC_ADDRESS es obligatorio con MC_TRANSPORT=ble (nombre del dispositivo)");
        }

        let chan_transport = env_transport("CHAN_MQTT_TRANSPORT")?;
        let chan_mqtt = MqttEndpoint {
            host: env_req("CHAN_MQTT_HOST")?,
            port: env_num(
                "CHAN_MQTT_PORT",
                match (chan_transport, env_bool("CHAN_MQTT_TLS", true)) {
                    (MqttTransport::Ws, _) => 443u16,
                    (MqttTransport::Tcp, true) => 8883u16,
                    (MqttTransport::Tcp, false) => 1883u16,
                },
            )?,
            transport: chan_transport,
            ws_path: env_or("CHAN_MQTT_WS_PATH", "/mqtt"),
            tls: env_bool("CHAN_MQTT_TLS", true),
            user: env_or("CHAN_MQTT_USER", ""),
            pass: env_or("CHAN_MQTT_PASS", ""),
            ca_file: env_opt("CHAN_MQTT_CA_FILE"),
        };

        let obs = if env_bool("OBS_ENABLED", false) {
            let map_transport = env_transport("MAP_MQTT_TRANSPORT")?;
            let endpoint = MqttEndpoint {
                host: env_req("MAP_MQTT_HOST")?,
                port: env_num(
                    "MAP_MQTT_PORT",
                    match (map_transport, env_bool("MAP_MQTT_TLS", true)) {
                        (MqttTransport::Ws, _) => 443u16,
                        (MqttTransport::Tcp, true) => 8883u16,
                        (MqttTransport::Tcp, false) => 1883u16,
                    },
                )?,
                transport: map_transport,
                ws_path: env_or("MAP_MQTT_WS_PATH", "/mqtt"),
                tls: env_bool("MAP_MQTT_TLS", true),
                // El usuario real se arma en runtime como v1_<pubkey>, porque la
                // pubkey puede venir autodetectada del nodo.
                user: String::new(),
                pass: env_req("GATEWAY_TOKEN")?,
                ca_file: env_opt("MAP_MQTT_CA_FILE"),
            };
            Some(ObsConfig {
                iata: env_req("MAP_IATA")?,
                gateway_pubkey: env_opt("GATEWAY_PUBLIC_KEY").map(|k| k.to_lowercase()),
                interval: Duration::from_secs(env_num("MAP_INTERVAL", 60u64)?.max(10)),
                endpoint,
            })
        } else {
            None
        };

        let cfg = Config {
            transport,
            address,
            baud: env_num("MC_BAUD", 115_200u32)?,
            channel_name: env_req("CHANNEL_NAME")?,
            channel_idx: env_num("CHANNEL_IDX", 0u8)?,
            island_iata: env_req("ISLAND_IATA")?,
            topic_prefix: env_or("CHAN_TOPIC_PREFIX", "meshchan")
                .trim_matches('/')
                .to_string(),
            topic_region: env_or("CHAN_REGION", "").trim_matches('/').to_string(),
            chan_mqtt,
            dedup_ttl: Duration::from_secs(env_num("DEDUP_TTL_S", 600u64)?),
            dedup_max: env_num("DEDUP_MAX", 4096usize)?,
            rate_per_min: env_num("RATE_PER_MIN", 10u32)?,
            rate_burst: env_num("RATE_BURST", 5u32)?,
            rate_min_interval: Duration::from_secs(env_num("RATE_MIN_INTERVAL_S", 3u64)?),
            max_text_len: env_num("MAX_TEXT_LEN", 178usize)?,
            obs,
        };

        if cfg.channel_name.contains('/') || cfg.channel_name.contains('+') || cfg.channel_name.contains('#') {
            bail!("CHANNEL_NAME no puede contener / + # (se usa como nivel de topic MQTT)");
        }
        if cfg.max_text_len == 0 {
            bail!("MAX_TEXT_LEN debe ser > 0");
        }
        Ok(cfg)
    }

    /// Topic del canal (spec §5): `<prefix>[/<region>]/<channel>`.
    ///
    /// Con CHAN_TOPIC_PREFIX=meshchan y CHAN_REGION=CL queda
    /// `meshchan/CL/publica`. Todos los bridges de un mismo canal tienen que
    /// publicar y suscribirse EXACTAMENTE al mismo topic.
    pub fn channel_topic(&self) -> String {
        if self.topic_region.is_empty() {
            format!("{}/{}", self.topic_prefix, self.channel_name)
        } else {
            format!(
                "{}/{}/{}",
                self.topic_prefix, self.topic_region, self.channel_name
            )
        }
    }
}
