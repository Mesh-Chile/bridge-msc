//! Muestra que ve el agente en el nodo, SIN publicar nada a ningun lado.
//!
//! Sirve para el paso previo a levantar el bridge: confirmar que el nodo
//! responde, en que indice esta el canal publico y que contactos ve.
//!
//!   MC_ADDRESS=/dev/ttyACM1 cargo run --example node_info
//!
//! ADVERTENCIA: abre el serial del nodo. No lo corras con el agente andando.

use std::time::Duration;

use anyhow::Result;
use bridge_msc::mesh;

#[tokio::main]
async fn main() -> Result<()> {
    let puerto = std::env::var("MC_ADDRESS").unwrap_or_else(|_| "/dev/ttyACM0".into());
    let baud: u32 = std::env::var("MC_BAUD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(115_200);

    println!("abriendo {puerto} a {baud} baud...");
    let mc = meshcore_rs::MeshCore::serial(&puerto, baud).await?;
    mc.set_default_timeout(Duration::from_secs(5)).await;

    let info = mc.commands().lock().await.send_appstart().await?;
    println!("\n--- NODO ---");
    println!("nombre      : {}", info.name);
    println!("pubkey      : {}", mesh::hex(&info.public_key));
    println!("posicion    : {}, {}", info.adv_lat as f64 / 1e6, info.adv_lon as f64 / 1e6);
    println!("tx power    : {} dBm (max {})", info.tx_power, info.max_tx_power);
    println!("radio       : {} kHz, bw {} kHz, sf {}", info.radio_freq, info.radio_bw, info.sf);

    println!("\n--- CANALES ---");
    println!("(un canal vacio o con nombre en blanco es un slot sin configurar)");
    for idx in 0..8u8 {
        match mc.commands().lock().await.get_channel(idx).await {
            Ok(ch) => {
                let nombre = ch.name.trim_end_matches('\0').trim();
                let con_clave = ch.secret.iter().any(|b| *b != 0);
                if nombre.is_empty() && !con_clave {
                    continue;
                }
                println!(
                    "  idx {idx}: {:?}{}",
                    nombre,
                    if con_clave { "  [con clave]" } else { "  [SIN CLAVE]" }
                );
            }
            Err(e) => {
                println!("  idx {idx}: (no se pudo leer: {e})");
                break;
            }
        }
    }

    println!("\n--- CONTACTOS ---");
    match mc.commands().lock().await.get_contacts(0).await {
        Ok(cs) => {
            println!("{} contactos", cs.len());
            for c in cs.iter().take(15) {
                let pos = if c.adv_lat == 0 && c.adv_lon == 0 {
                    "sin posicion".to_string()
                } else {
                    format!("{:.4}, {:.4}", c.adv_lat as f64 / 1e6, c.adv_lon as f64 / 1e6)
                };
                println!("  {:<20} tipo={} {}", c.adv_name, c.contact_type, pos);
            }
            if cs.len() > 15 {
                println!("  ... y {} mas", cs.len() - 15);
            }
        }
        Err(e) => println!("no se pudieron leer: {e}"),
    }

    mc.disconnect().await?;
    Ok(())
}
