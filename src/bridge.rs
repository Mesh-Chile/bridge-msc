//! Protocolo meshchan v0.1 y la logica pura del bridge.
//!
//! Implementa `meshchan-spec-v.01.md`. Todo lo testeable sin radio ni broker
//! vive aca; el cableado con tokio esta en `main.rs`.

use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

/// Valor del campo `v` que emitimos (spec §6, §12).
pub const PROTOCOL_VERSION: u8 = 1;

/// Ancho del bucket temporal que entra al hash del id (spec §7).
pub const ID_BUCKET_SECS: u64 = 30;

/// Techo de texto de un mensaje de canal en el firmware MeshCore:
/// `MAX_TEXT_LEN = 10 * CIPHER_BLOCK_SIZE` = 160 bytes (BaseChatMesh.h).
///
/// OJO: ese techo cuenta el prefijo `"<nombre_del_nodo>: "` que el PROPIO
/// firmware antepone al transmitir (BaseChatMesh::sendGroupMessage), no el
/// texto que le pasamos por el serial. Si nos pasamos, el firmware corta en
/// silencio.
pub const FIRMWARE_MAX_TEXT_LEN: usize = 160;

/// Cuantos bytes de texto podemos mandarle de verdad al firmware, descontando
/// el prefijo que el va a agregar solo.
pub fn presupuesto_texto(configurado: usize, nombre_nodo: &str) -> usize {
    let prefijo = nombre_nodo.len() + 2; // "nombre" + ": "
    let disponible = FIRMWARE_MAX_TEXT_LEN.saturating_sub(prefijo).max(1);
    configurado.min(disponible)
}

/// Separador de campos del preimage: ASCII Unit Separator (spec §7).
const US: u8 = 0x1F;

fn v_por_defecto() -> u8 {
    PROTOCOL_VERSION
}

/// Payload JSON que viaja por el topic del canal (spec §6).
///
/// Los campos desconocidos se ignoran al deserializar, como manda la spec
/// (compatibilidad hacia adelante): serde lo hace por defecto, NO agregar
/// `deny_unknown_fields`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChanMessage {
    /// Version del protocolo. 1 para esta spec.
    #[serde(default = "v_por_defecto")]
    pub v: u8,
    /// Id canonico (§7): 16 hex minusculas.
    pub id: String,
    /// Nombre del canal publico (coincide con el topic).
    pub channel: String,
    /// Texto original tal como se oyo en la RF, SIN el prefijo de atribucion.
    pub text: String,
    /// Nombre visible del remitente. Puede ir vacio.
    #[serde(default)]
    pub sender: String,
    /// Identidad del nodo de origen (del humano, no del bridge).
    pub sender_id: String,
    /// Pubkey del bridge que publico. Anti-loop.
    pub origin_bridge: String,
    /// Etiqueta de procedencia del bridge de origen (IATA/region).
    pub origin_island: String,
    /// Epoch en segundos en que el bridge de origen oyo el mensaje.
    pub ts: u64,
}

/// id = lowercasehex(SHA1(channel ␟ sender_id ␟ text ␟ bucket)[0..8])
pub fn message_id_bucket(channel: &str, sender_id: &str, text: &str, bucket: u64) -> String {
    let mut h = Sha1::new();
    h.update(channel.as_bytes());
    h.update([US]);
    h.update(sender_id.as_bytes());
    h.update([US]);
    h.update(text.as_bytes());
    h.update([US]);
    h.update(bucket.to_string().as_bytes());
    let digest = h.finalize();
    digest.iter().take(8).map(|b| format!("{b:02x}")).collect()
}

/// Id canonico de un mensaje observado en `ts`.
pub fn message_id(channel: &str, sender_id: &str, text: &str, ts: u64) -> String {
    message_id_bucket(channel, sender_id, text, ts / ID_BUCKET_SECS)
}

/// Id del bucket ANTERIOR. La spec (§7, "known limitation") permite probarlo
/// tambien: un mensaje oido a caballo de un limite de 30 s da dos ids, y
/// mirando los dos se cierra ese hueco.
pub fn message_id_previo(channel: &str, sender_id: &str, text: &str, ts: u64) -> String {
    let bucket = (ts / ID_BUCKET_SECS).saturating_sub(1);
    message_id_bucket(channel, sender_id, text, bucket)
}

/// Un prefijo es un nick si no esta vacio y, cuando trae espacios, es corto:
/// un ':' despues de varias palabras es mas probable que sea parte del texto.
fn es_nick_plausible(nick: &str) -> bool {
    if nick.is_empty() {
        return false;
    }
    !nick.contains(' ') || nick.len() <= 24
}

/// Los mensajes de canal viajan como `"<nombre>: <texto>"`. Parte en el PRIMER
/// ':' (el firmware antepone siempre el nombre, asi que ese ':' es el separador).
///
/// El texto sale **byte a byte** como se oyo en la RF: se saca el separador
/// —los dos puntos y UN solo espacio, que es exactamente lo que escribe
/// `sendGroupMessage()` con `"%s: "`— y nada mas. Ni `trim`, ni normalizacion.
///
/// Esto es deliberado y es contrato de cable: el `id` canonico se calcula sobre
/// el texto, asi que cualquier normalizacion que una implementacion haga y otra
/// no, rompe la deduplicacion entre bridges. Un espacio al final es un mensaje
/// distinto, y esta bien que lo sea.
pub fn split_sender(raw: &str) -> (Option<String>, String) {
    match raw.split_once(':') {
        Some((nick, rest)) if es_nick_plausible(nick) => {
            let texto = rest.strip_prefix(' ').unwrap_or(rest);
            (Some(nick.to_string()), texto.to_string())
        }
        _ => (None, raw.to_string()),
    }
}

/// Corta a `max` bytes sin partir un caracter UTF-8 por la mitad.
pub fn truncate_bytes(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

/// Texto que se inyecta a la RF (spec §8.3): `[<island>] <sender>: <text>`.
///
/// El firmware antepone SIEMPRE el nombre de nuestro propio nodo al transmitir,
/// asi que sin este prefijo el mensaje se leeria en la RF como si lo hubiera
/// dicho el nodo del bridge. En el aire queda:
/// `<nodo_del_bridge>: [<island>] <sender>: <text>`.
/// Si `sender` viene vacio se cae al `sender_id` acortado, y si tampoco hay, se
/// omite el nombre.
pub fn format_injection(
    island: &str,
    sender: &str,
    sender_id: &str,
    text: &str,
    max_len: usize,
) -> String {
    let nombre = if !sender.trim().is_empty() {
        sender.trim().to_string()
    } else {
        truncate_bytes(sender_id.trim(), 12)
    };
    let compuesto = if nombre.is_empty() {
        format!("[{island}] {text}")
    } else {
        format!("[{island}] {nombre}: {text}")
    };
    truncate_bytes(&compuesto, max_len)
}

/// Decide si un mensaje que llego por MQTT hay que inyectarlo a la RF.
#[derive(Debug, PartialEq, Eq)]
pub enum Inbound {
    /// Es nuestra propia publicacion devuelta por el hub (spec §8.2.1).
    EcoPropio,
    /// El canal del payload no es el nuestro.
    OtroCanal,
    /// Version mayor de protocolo que no sabemos interpretar (spec §12).
    VersionIncompatible,
    Inyectar,
}

pub fn classify_inbound(msg: &ChanMessage, my_pubkey: &str, my_channel: &str) -> Inbound {
    if msg.v > PROTOCOL_VERSION {
        return Inbound::VersionIncompatible;
    }
    if msg.origin_bridge.eq_ignore_ascii_case(my_pubkey) {
        return Inbound::EcoPropio;
    }
    if msg.channel != my_channel {
        return Inbound::OtroCanal;
    }
    Inbound::Inyectar
}

pub fn now_epoch() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn el_id_es_16_hex_minusculas() {
        let id = message_id("publica", "fac180abf4d2", "hola", 1_756_742_400);
        assert_eq!(id.len(), 16);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()));
    }

    /// Vector fijo calculado a mano contra la spec §7. Si esto cambia, se rompio
    /// la compatibilidad de cable con las otras implementaciones.
    #[test]
    fn vector_de_referencia_del_preimage() {
        // preimage = "publica" 0x1F "fac180abf4d2" 0x1F "hola" 0x1F "58558080"
        let esperado = {
            let mut h = Sha1::new();
            h.update(b"publica\x1ffac180abf4d2\x1fhola\x1f58558080");
            let d = h.finalize();
            d.iter().take(8).map(|b| format!("{b:02x}")).collect::<String>()
        };
        // 1_756_742_400 / 30 = 58_558_080
        assert_eq!(message_id("publica", "fac180abf4d2", "hola", 1_756_742_400), esperado);
    }

    #[test]
    fn el_separador_es_0x1f_y_no_dos_puntos() {
        // Con separador ':' estos dos casos colisionarian; con 0x1F no.
        let a = message_id("a:b", "c", "d", 0);
        let b = message_id("a", "b:c", "d", 0);
        assert_ne!(a, b, "el 0x1F tiene que hacer inambiguos los limites de campo");
    }

    #[test]
    fn el_id_es_estable_dentro_del_bucket_de_30s() {
        let a = message_id("c", "s", "t", 1_755_999_990);
        let b = message_id("c", "s", "t", 1_755_999_990 + 29);
        assert_eq!(a, b);
    }

    #[test]
    fn el_id_cambia_al_cruzar_el_bucket_y_el_previo_lo_recupera() {
        let t_antes = 1_755_999_990 + 29;
        let t_despues = 1_755_999_990 + 30;
        assert_ne!(
            message_id("c", "s", "t", t_antes),
            message_id("c", "s", "t", t_despues)
        );
        // El truco de la spec §7: el id del bucket previo del segundo
        // avistamiento es el id del primero.
        assert_eq!(
            message_id("c", "s", "t", t_antes),
            message_id_previo("c", "s", "t", t_despues)
        );
    }

    #[test]
    fn el_bucket_previo_no_se_pasa_de_cero() {
        // Sin panic ni underflow con ts chicos.
        let _ = message_id_previo("c", "s", "t", 0);
    }

    #[test]
    fn el_id_depende_de_los_tres_campos() {
        let base = message_id("c", "s", "t", 0);
        assert_ne!(base, message_id("c2", "s", "t", 0));
        assert_ne!(base, message_id("c", "s2", "t", 0));
        assert_ne!(base, message_id("c", "s", "t2", 0));
    }

    #[test]
    fn separa_nick_y_texto() {
        assert_eq!(
            split_sender("n3v1l: hola"),
            (Some("n3v1l".into()), "hola".into())
        );
    }

    #[test]
    fn un_texto_con_dos_puntos_adentro_conserva_el_resto() {
        assert_eq!(
            split_sender("n3v1l: ojo: esto es una prueba"),
            (Some("n3v1l".into()), "ojo: esto es una prueba".into())
        );
    }

    #[test]
    fn sin_prefijo_no_hay_nick() {
        assert_eq!(split_sender("hola a todos"), (None, "hola a todos".into()));
    }

    /// Caso real visto en vivo el 2026-09-02: el mismo texto con y sin espacio
    /// final son mensajes DISTINTOS, y el id lo refleja. No se normaliza.
    #[test]
    fn un_espacio_al_final_se_conserva_y_da_otro_id() {
        let (_, con) = split_sender("n3v-mscmov01: @[cl-bridge] recibido ");
        let (_, sin) = split_sender("n3v-mscmov01: @[cl-bridge] recibido");
        assert_eq!(con, "@[cl-bridge] recibido ");
        assert_eq!(sin, "@[cl-bridge] recibido");
        assert_ne!(
            message_id("Public", "n3v-mscmov01", &con, 1_788_325_073),
            message_id("Public", "n3v-mscmov01", &sin, 1_788_325_073)
        );
    }

    #[test]
    fn se_consume_exactamente_un_espacio_del_separador() {
        // El firmware escribe "%s: ", un solo espacio. Si el texto empieza con
        // mas espacios, son del texto y se conservan.
        let (_, t) = split_sender("juan:   hola");
        assert_eq!(t, "  hola", "solo se saca el espacio del separador");
    }

    #[test]
    fn sin_espacio_tras_los_dos_puntos_no_se_come_nada() {
        let (nick, t) = split_sender("juan:hola");
        assert_eq!(nick.as_deref(), Some("juan"));
        assert_eq!(t, "hola");
    }

    #[test]
    fn un_mensaje_sin_nick_se_publica_tal_cual() {
        // Sin ':' no hay nada que sacar: ni un espacio de los bordes.
        assert_eq!(split_sender("  hola  "), (None, "  hola  ".into()));
    }

    #[test]
    fn una_frase_larga_con_dos_puntos_no_se_toma_por_nick() {
        let (nick, _) = split_sender(
            "esta es una frase bastante larga que igual trae: dos puntos al medio",
        );
        assert_eq!(nick, None);
    }

    #[test]
    fn truncar_no_parte_un_caracter_utf8() {
        let t = truncate_bytes("ñññññ", 5); // 2 bytes cada una
        assert_eq!(t, "ññ");
        assert!(std::str::from_utf8(t.as_bytes()).is_ok());
    }

    #[test]
    fn la_inyeccion_sigue_el_formato_de_la_spec() {
        // Ejemplo del Apendice A.
        assert_eq!(
            format_injection("CCP", "juan", "fac180abf4d2", "alguien copia en la zona sur?", 178),
            "[CCP] juan: alguien copia en la zona sur?"
        );
    }

    #[test]
    fn la_inyeccion_respeta_el_techo_de_largo() {
        let largo = format_injection("SCL", "juan", "id", &"x".repeat(500), 178);
        assert!(largo.len() <= 178);
        assert!(largo.starts_with("[SCL] juan: "));
    }

    #[test]
    fn sin_nombre_visible_cae_al_sender_id_corto() {
        assert_eq!(
            format_injection("SCL", "", "fac180abf4d2ffff", "hola", 178),
            "[SCL] fac180abf4d2: hola"
        );
    }

    #[test]
    fn sin_nombre_ni_id_se_omite_el_nombre() {
        assert_eq!(format_injection("SCL", "", "", "hola", 178), "[SCL] hola");
    }

    #[test]
    fn el_presupuesto_descuenta_el_prefijo_del_firmware() {
        // "n3v-msc-tlora" son 13 bytes + ": " = 15 -> 160 - 15 = 145
        assert_eq!(presupuesto_texto(178, "n3v-msc-tlora"), 145);
    }

    #[test]
    fn el_presupuesto_respeta_un_maximo_configurado_menor() {
        assert_eq!(presupuesto_texto(80, "n3v-msc-tlora"), 80);
    }

    #[test]
    fn un_nombre_de_nodo_absurdo_no_deja_el_presupuesto_en_cero() {
        assert!(presupuesto_texto(178, &"x".repeat(300)) >= 1);
    }

    #[test]
    fn lo_que_inyectamos_mas_el_prefijo_del_firmware_cabe_en_el_aire() {
        let nodo = "n3v-msc-tlora";
        let max = presupuesto_texto(178, nodo);
        let texto = format_injection("CCP", "juan", "fac1", &"x".repeat(500), max);
        assert!(texto.len() <= max);
        let en_el_aire = format!("{nodo}: {texto}");
        assert!(
            en_el_aire.len() <= FIRMWARE_MAX_TEXT_LEN,
            "el firmware cortaria: {} bytes",
            en_el_aire.len()
        );
    }

    fn msg(bridge: &str, channel: &str) -> ChanMessage {
        ChanMessage {
            v: 1,
            id: "0".into(),
            channel: channel.into(),
            text: "hola".into(),
            sender: "otro".into(),
            sender_id: "otro".into(),
            origin_bridge: bridge.into(),
            origin_island: "VAP".into(),
            ts: 0,
        }
    }

    #[test]
    fn el_eco_propio_no_se_reinyecta() {
        let m = msg("AABB", "publica");
        assert_eq!(
            classify_inbound(&m, "aabb", "publica"),
            Inbound::EcoPropio,
            "la comparacion de pubkey debe ser case-insensitive"
        );
    }

    #[test]
    fn otro_canal_se_descarta() {
        let m = msg("CCDD", "otro");
        assert_eq!(classify_inbound(&m, "aabb", "publica"), Inbound::OtroCanal);
    }

    #[test]
    fn una_version_mayor_no_se_inyecta() {
        let mut m = msg("CCDD", "publica");
        m.v = 2;
        assert_eq!(
            classify_inbound(&m, "aabb", "publica"),
            Inbound::VersionIncompatible
        );
    }

    #[test]
    fn un_mensaje_remoto_del_canal_se_inyecta() {
        let m = msg("CCDD", "publica");
        assert_eq!(classify_inbound(&m, "aabb", "publica"), Inbound::Inyectar);
    }

    #[test]
    fn el_json_tiene_exactamente_los_campos_del_contrato() {
        let m = msg("CCDD", "publica");
        let j = serde_json::to_value(&m).unwrap();
        for k in [
            "v",
            "id",
            "channel",
            "text",
            "sender",
            "sender_id",
            "origin_bridge",
            "origin_island",
            "ts",
        ] {
            assert!(j.get(k).is_some(), "falta el campo {k}");
        }
        assert_eq!(j.as_object().unwrap().len(), 9, "no debe haber campos extra");
    }

    #[test]
    fn los_campos_desconocidos_se_ignoran_al_recibir() {
        // Spec §6: compatibilidad hacia adelante.
        let json = r#"{"v":1,"id":"abc","channel":"publica","text":"hola",
            "sender":"juan","sender_id":"fac1","origin_bridge":"aabb",
            "origin_island":"CCP","ts":1,"campo_del_futuro":42}"#;
        let m: ChanMessage = serde_json::from_str(json).unwrap();
        assert_eq!(m.text, "hola");
    }

    #[test]
    fn el_ejemplo_del_apendice_a_deserializa() {
        let json = r#"{
          "v": 1, "id": "9f2a7c1b4e6d8a03", "channel": "publica",
          "text": "alguien copia en la zona sur?", "sender": "juan",
          "sender_id": "fac180abf4d2", "origin_bridge": "a1b2c3d4e5f60718",
          "origin_island": "CCP", "ts": 1756742400 }"#;
        let m: ChanMessage = serde_json::from_str(json).unwrap();
        assert_eq!(m.origin_island, "CCP");
        assert_eq!(m.ts, 1_756_742_400);
    }

    #[test]
    fn un_payload_sin_v_se_toma_como_v1() {
        let json = r#"{"id":"a","channel":"c","text":"t","sender_id":"s",
            "origin_bridge":"b","origin_island":"i","ts":1}"#;
        let m: ChanMessage = serde_json::from_str(json).unwrap();
        assert_eq!(m.v, 1);
        assert_eq!(m.sender, "");
    }
}
