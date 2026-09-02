//! Conexion al nodo MeshCore: apertura por transporte, autodeteccion de la
//! pubkey propia y el drenaje defensivo de la cola de mensajes.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use meshcore_rs::MeshCore;
use tracing::{debug, info, warn};

use crate::config::{Config, Transport};

/// Cuantos `get_msg` seguidos pueden fallar antes de dar la cola por vacia.
const DRAIN_MAX_ERRORES: usize = 3;
/// Techo de mensajes por ronda de drenaje, por si algo se realimenta.
const DRAIN_MAX_MENSAJES: usize = 64;

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Abre la conexion al nodo segun MC_TRANSPORT.
pub async fn connect(cfg: &Config) -> Result<Arc<MeshCore>> {
    let mc = match cfg.transport {
        Transport::Serial => {
            info!(puerto = %cfg.address, baud = cfg.baud, "abriendo serial");
            MeshCore::serial(&cfg.address, cfg.baud)
                .await
                .with_context(|| {
                    format!(
                        "abriendo el serial {}. Revisa que ningun otro proceso \
                         (publisher, clock-sync, bot) lo tenga tomado y que el \
                         usuario este en el grupo dialout",
                        cfg.address
                    )
                })?
        }
        Transport::Tcp => {
            let (host, port) = cfg
                .address
                .rsplit_once(':')
                .context("MC_ADDRESS con MC_TRANSPORT=tcp debe ser host:puerto")?;
            let port: u16 = port.parse().context("puerto TCP invalido en MC_ADDRESS")?;
            info!(host, port, "conectando por TCP");
            MeshCore::tcp(host, port)
                .await
                .with_context(|| format!("conectando por TCP a {}", cfg.address))?
        }
        #[cfg(feature = "ble")]
        Transport::Ble => {
            info!(dispositivo = %cfg.address, "conectando por BLE");
            MeshCore::ble_connect(&cfg.address)
                .await
                .with_context(|| format!("conectando por BLE a {}", cfg.address))?
        }
        #[cfg(not(feature = "ble"))]
        Transport::Ble => {
            anyhow::bail!("este binario se compilo sin la feature `ble`");
        }
    };
    Ok(Arc::new(mc))
}

/// Pubkey del propio nodo (64 hex). Es `origin_bridge` del protocolo y, si no se
/// paso GATEWAY_PUBLIC_KEY, tambien la identidad ante el broker de observabilidad.
pub async fn self_pubkey(mc: &MeshCore) -> Result<(String, String)> {
    let info = mc
        .commands()
        .lock()
        .await
        .send_appstart()
        .await
        .context("send_appstart: el nodo no respondio la identificacion inicial")?;
    Ok((hex(&info.public_key), info.name.clone()))
}

/// Drena a mano la cola de mensajes del device.
///
/// Existe porque `get_msg` solo espera CONTACT_MSG_RECV / CHANNEL_MSG_RECV /
/// NO_MORE_MSGS / ERROR: si en la cola hay un frame que la libreria no mapea
/// (por ejemplo CHANNEL_DATA_RECV = 27, que esta version no conoce), la espera
/// expira. Ese frame YA salio de la cola del device (el comando es "dame el
/// siguiente"), asi que NO hay que cortar: se sigue drenando. Cortar ahi es
/// justo lo que deja al bridge sordo hasta el proximo MESSAGES_WAITING.
///
/// Los mensajes rescatados no se devuelven: `get_msg` los despacha por el
/// dispatcher y el lector de eventos los toma solo.
pub async fn drain_queue(mc: &MeshCore) -> usize {
    let mut rescatados = 0usize;
    let mut errores = 0usize;
    for _ in 0..DRAIN_MAX_MENSAJES {
        let r = mc
            .commands()
            .lock()
            .await
            .get_msg_with_timeout(Duration::from_secs(5))
            .await;
        match r {
            Ok(Some(_)) => {
                rescatados += 1;
                errores = 0;
            }
            Ok(None) => break, // NO_MORE_MSGS: la cola quedo vacia de verdad.
            Err(e) => {
                errores += 1;
                debug!(error = %e, intento = errores, "get_msg fallo durante el drenaje");
                if errores >= DRAIN_MAX_ERRORES {
                    warn!(
                        errores,
                        "{DRAIN_MAX_ERRORES} get_msg seguidos sin respuesta; corto el drenaje"
                    );
                    break;
                }
            }
        }
    }
    rescatados
}
