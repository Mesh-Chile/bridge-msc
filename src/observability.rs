//! Modulo OPCIONAL (OBS_ENABLED). Publica los contactos con posicion del nodo
//! al broker de observabilidad como paquetes ADVERT, para alimentar el mapa.
//!
//! Si OBS_ENABLED=false nada de esto corre: no se crea el segundo cliente MQTT
//! ni se llama a get_contacts.

use std::sync::Arc;

use anyhow::Result;
use meshcore_rs::events::Contact;
use meshcore_rs::MeshCore;
use serde::Serialize;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::config::ObsConfig;
use crate::mesh::hex;
use crate::mqtt::MqttHandle;

/// Payload que espera el broker de observabilidad.
#[derive(Debug, Serialize)]
pub struct AdvertPacket {
    pub origin_id: String,
    #[serde(rename = "type")]
    pub packet_type: &'static str,
    pub node_pubkey: String,
    pub name: String,
    pub role: &'static str,
    pub latitude: f64,
    pub longitude: f64,
    pub timestamp: u32,
}

/// Vocabulario de roles que entiende el mapa.
pub fn role_de(contact_type: u8) -> &'static str {
    match contact_type & 0x0F {
        1 => "companion",
        2 => "repeater",
        3 => "room",
        _ => "unknown",
    }
}

/// Convierte microgrados a grados y descarta lo que no sirve.
///
/// `meshcore-rs` 0.2 ya entrega `adv_lat`/`adv_lon` tipados como i32 en
/// microgrados, asi que no hace falta la extraccion defensiva que si necesitan
/// las implementaciones que leen JSON crudo.
pub fn posicion(c: &Contact) -> Option<(f64, f64)> {
    if c.adv_lat == 0 && c.adv_lon == 0 {
        return None; // (0,0) es "sin posicion", no el golfo de Guinea.
    }
    let lat = c.adv_lat as f64 / 1e6;
    let lon = c.adv_lon as f64 / 1e6;
    if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
        return None;
    }
    Some((lat, lon))
}

pub fn topic(iata: &str, gateway_pubkey: &str) -> String {
    format!("meshcore/{iata}/{gateway_pubkey}/packets")
}

/// Una pasada: pide contactos y publica los que tienen posicion.
/// Devuelve (publicados, total).
pub async fn publicar_ronda(
    mc: &MeshCore,
    mqtt: &MqttHandle,
    cfg: &ObsConfig,
    gateway_pubkey: &str,
    primera_vez: &Mutex<bool>,
) -> Result<(usize, usize)> {
    // 0 = "dame todos", sin filtrar por fecha de modificacion.
    let contactos = mc.commands().lock().await.get_contacts(0).await?;
    let total = contactos.len();
    let topic = topic(&cfg.iata, gateway_pubkey);
    let mut publicados = 0usize;

    for c in &contactos {
        {
            // En la primera corrida logueamos un contacto entero para poder
            // validar los nombres de campo contra lo que llega de verdad.
            let mut p = primera_vez.lock().await;
            if *p {
                info!(
                    nombre = %c.adv_name,
                    pubkey = %c.public_key_hex(),
                    contact_type = c.contact_type,
                    adv_lat = c.adv_lat,
                    adv_lon = c.adv_lon,
                    last_advert = c.last_advert,
                    "primer contacto de la primera ronda (para validar campos)"
                );
                *p = false;
            }
        }

        let Some((lat, lon)) = posicion(c) else {
            debug!(nombre = %c.adv_name, "contacto sin posicion, se omite");
            continue;
        };

        let pkt = AdvertPacket {
            origin_id: gateway_pubkey.to_string(),
            packet_type: "ADVERT",
            node_pubkey: hex(&c.public_key),
            name: c.adv_name.clone(),
            role: role_de(c.contact_type),
            latitude: lat,
            longitude: lon,
            timestamp: c.last_advert,
        };
        match serde_json::to_vec(&pkt) {
            Ok(body) => {
                if let Err(e) = mqtt.publish(&topic, body).await {
                    warn!(error = %e, nombre = %c.adv_name, "no se pudo publicar el advert");
                } else {
                    publicados += 1;
                }
            }
            Err(e) => warn!(error = %e, "no se pudo serializar el advert"),
        }
    }
    Ok((publicados, total))
}

/// Lanza el bucle de observabilidad. La conexion al nodo se toma del handle
/// compartido en cada tick, asi la task sobrevive a las reconexiones.
pub fn spawn(
    cfg: ObsConfig,
    gateway_pubkey: String,
    mqtt: MqttHandle,
    nodo: Arc<tokio::sync::RwLock<Option<Arc<MeshCore>>>>,
) {
    tokio::spawn(async move {
        let primera_vez = Mutex::new(true);
        let mut tick = tokio::time::interval(cfg.interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let Some(mc) = nodo.read().await.clone() else {
                debug!("observabilidad: el nodo no esta conectado, salto la ronda");
                continue;
            };
            match publicar_ronda(&mc, &mqtt, &cfg, &gateway_pubkey, &primera_vez).await {
                Ok((pub_, total)) => {
                    info!(publicados = pub_, contactos = total, "ronda de observabilidad")
                }
                Err(e) => warn!(error = %e, "fallo la ronda de observabilidad"),
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contacto(lat: i32, lon: i32, t: u8) -> Contact {
        Contact {
            public_key: [0xAB; 32],
            contact_type: t,
            flags: 0,
            path_len: -1,
            out_path: vec![],
            adv_name: "n3v-test".into(),
            last_advert: 1_756_000_000,
            adv_lat: lat,
            adv_lon: lon,
            last_modification_timestamp: 0,
        }
    }

    #[test]
    fn cero_cero_se_descarta() {
        assert_eq!(posicion(&contacto(0, 0, 1)), None);
    }

    #[test]
    fn microgrados_a_grados() {
        let (lat, lon) = posicion(&contacto(-33_450_000, -70_666_000, 1)).unwrap();
        assert!((lat - -33.45).abs() < 1e-9);
        assert!((lon - -70.666).abs() < 1e-9);
    }

    #[test]
    fn coordenadas_fuera_de_rango_se_descartan() {
        assert_eq!(posicion(&contacto(999_000_000, 1_000, 1)), None);
    }

    #[test]
    fn roles_conocidos() {
        assert_eq!(role_de(1), "companion");
        assert_eq!(role_de(2), "repeater");
        assert_eq!(role_de(3), "room");
        assert_eq!(role_de(4), "unknown");
        assert_eq!(role_de(0xF2), "repeater");
    }

    #[test]
    fn el_topic_sigue_el_formato_del_broker() {
        assert_eq!(topic("SCL", "aabb"), "meshcore/SCL/aabb/packets");
    }

    #[test]
    fn el_json_del_advert_usa_type_no_packet_type() {
        let pkt = AdvertPacket {
            origin_id: "aa".into(),
            packet_type: "ADVERT",
            node_pubkey: "bb".into(),
            name: "n".into(),
            role: "repeater",
            latitude: -33.45,
            longitude: -70.66,
            timestamp: 1,
        };
        let j = serde_json::to_value(&pkt).unwrap();
        assert_eq!(j["type"], "ADVERT");
        assert!(j.get("packet_type").is_none());
    }
}
