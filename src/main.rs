//! bridge-msc — implementacion de referencia del protocolo meshchan v0.1.
//!
//! Un solo proceso, una sola conexion al nodo MeshCore, dos funciones:
//!   1. bridge bidireccional de uno o mas canales (CHANNELS) contra un hub MQTT (siempre);
//!   2. publisher de observabilidad para el mapa (opcional, OBS_ENABLED).
//!
//! ADVERTENCIA: este agente es dueno EXCLUSIVO del puerto del nodo. Ningun otro
//! proceso puede tenerlo abierto en paralelo.

use bridge_msc::{bridge, config, dedup, mesh, mqtt, observability, ratelimit};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures::StreamExt;
use meshcore_rs::{EventPayload, EventType, MeshCore};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

use bridge::{ChanMessage, Inbound};
use config::{Canal, Config};
use dedup::Dedup;
use ratelimit::{Denial, RateLimiter};

/// Cada cuanto revisa el watchdog el estado de la cola.
const WATCHDOG_TICK: Duration = Duration::from_secs(30);
/// Sin ningun mensaje por este tiempo, drenamos la cola a mano.
const SIN_MENSAJES_DRENAR: Duration = Duration::from_secs(300);
/// Sin ningun mensaje de canal por este tiempo, avisamos (bug #1232 del firmware).
const SIN_CANAL_AVISAR: Duration = Duration::from_secs(300);

/// Handle compartido a la conexion viva. `None` mientras se reconecta.
type NodoCompartido = Arc<RwLock<Option<Arc<MeshCore>>>>;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Arc::new(Config::from_env().context("configuracion invalida")?);
    info!(
        isla = %cfg.island_iata,
        observabilidad = cfg.obs.is_some(),
        "bridge-msc v{}", env!("CARGO_PKG_VERSION")
    );
    for canal in &cfg.channels {
        info!(canal = %canal.name, idx = canal.idx, topic = %cfg.topic(canal), "canal puenteado");
    }

    // --- 1. Nodo: primera conexion, que ademas nos da la identidad ---
    let nodo: NodoCompartido = Arc::new(RwLock::new(None));
    let (mc, pubkey, nombre_nodo) = conectar_e_identificar(&cfg).await;
    *nodo.write().await = Some(mc);

    // El techo efectivo ya se calculo y se logueo al identificar el nodo.
    let max_texto = bridge::presupuesto_texto(cfg.max_text_len, &nombre_nodo);

    let nombre_nodo = Arc::new(nombre_nodo);
    let pubkey = Arc::new(pubkey);

    // --- 2. Hub del canal (siempre) ---
    let (rx_tx, rx_mqtt) = mpsc::channel::<mqtt::Incoming>(64);
    let hub = mqtt::spawn(
        "canal",
        &cfg.chan_mqtt,
        &format!("meshchan-{}", &pubkey[..12.min(pubkey.len())]),
        &cfg.chan_mqtt.user.clone(),
        cfg.channels.iter().map(|c| cfg.topic(c)).collect(),
        Some(rx_tx),
    )
    .context("levantando el cliente MQTT del canal")?;

    // Cache de dedup compartida por las dos direcciones: es lo que corta el lazo.
    let dedup = Arc::new(Mutex::new(Dedup::new(cfg.dedup_ttl, cfg.dedup_max)));

    // --- 3. Observabilidad (opcional) ---
    if let Some(obs) = cfg.obs.clone() {
        let gw = obs
            .gateway_pubkey
            .clone()
            .unwrap_or_else(|| pubkey.to_string());
        let usuario = format!("v1_{gw}");
        info!(gateway = %gw, iata = %obs.iata, intervalo = ?obs.interval, "observabilidad ACTIVA");
        let mapa = mqtt::spawn(
            "mapa",
            &obs.endpoint,
            &format!("meshchan-obs-{}", &gw[..12.min(gw.len())]),
            &usuario,
            Vec::new(),
            None,
        )
        .context("levantando el cliente MQTT de observabilidad")?;
        observability::spawn(obs, gw, mapa, nodo.clone());
    } else {
        info!("observabilidad APAGADA (OBS_ENABLED=false): bridge puro");
    }

    // --- 4. Inyector MQTT -> RF (task larga, sobrevive reconexiones del nodo) ---
    spawn_inyector(
        cfg.clone(),
        pubkey.clone(),
        nodo.clone(),
        dedup.clone(),
        rx_mqtt,
        max_texto,
    );

    // --- 5. Supervisor de la sesion RF -> MQTT ---
    let supervisor = supervisar_sesiones(
        cfg.clone(),
        pubkey.clone(),
        nombre_nodo.clone(),
        nodo.clone(),
        dedup.clone(),
        hub,
    );

    tokio::select! {
        _ = supervisor => {}
        _ = esperar_senal() => {
            info!("senal recibida, cerrando");
        }
    }

    if let Some(mc) = nodo.write().await.take() {
        let _ = mc.disconnect().await;
    }
    info!("chao");
    Ok(())
}

async fn esperar_senal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Conecta al nodo reintentando con backoff exponencial. No se rinde: sin nodo
/// no hay agente, y el servicio tiene que sobrevivir a un USB desenchufado.
async fn conectar_con_reintento(cfg: &Config) -> Arc<MeshCore> {
    let mut backoff = Duration::from_secs(2);
    loop {
        match mesh::connect(cfg).await {
            Ok(mc) => return mc,
            Err(e) => {
                error!(error = %format!("{e:#}"), reintento_en = ?backoff, "no se pudo conectar al nodo");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        }
    }
}

/// Conecta y ademas se asegura de que el nodo se identifique, reintentando.
///
/// Se separo de `conectar_con_reintento` porque abrir el puerto no basta: visto
/// en vivo el 2026-09-02, justo despues de que otro proceso suelta el serial el
/// primer `appstart` puede expirar aunque el nodo este perfecto. Eso NO es
/// motivo para que el agente se muera: se cierra y se vuelve a intentar.
async fn conectar_e_identificar(cfg: &Config) -> (Arc<MeshCore>, String, String) {
    let mut backoff = Duration::from_secs(2);
    loop {
        let mc = conectar_con_reintento(cfg).await;
        match mesh::self_pubkey(&mc).await {
            Ok((pubkey, nombre)) => {
                info!(pubkey = %pubkey, nombre = %nombre, "nodo identificado");
                let max = bridge::presupuesto_texto(cfg.max_text_len, &nombre);
                if max < cfg.max_text_len {
                    info!(
                        configurado = cfg.max_text_len,
                        efectivo = max,
                        "MAX_TEXT_LEN recortado: el firmware antepone el nombre del nodo y corta en {} bytes",
                        bridge::FIRMWARE_MAX_TEXT_LEN
                    );
                }
                return (mc, pubkey, nombre);
            }
            Err(e) => {
                error!(
                    error = %format!("{e:#}"),
                    reintento_en = ?backoff,
                    "el nodo abrio el puerto pero no se identifico"
                );
                let _ = mc.disconnect().await;
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        }
    }
}

/// MQTT -> RF: clasifica, deduplica, limita y solo entonces inyecta.
///
/// El rate-limit va POR CANAL: una rafaga en un canal (p.ej. respuestas de bots)
/// no deja mudo al canal publico.
fn spawn_inyector(
    cfg: Arc<Config>,
    pubkey: Arc<String>,
    nodo: NodoCompartido,
    dedup: Arc<Mutex<Dedup>>,
    mut rx: mpsc::Receiver<mqtt::Incoming>,
    max_texto: usize,
) {
    tokio::spawn(async move {
        let mut limites: HashMap<u8, RateLimiter> = cfg
            .channels
            .iter()
            .map(|c| (c.idx, RateLimiter::new(cfg.rate_per_min, cfg.rate_burst, cfg.rate_min_interval)))
            .collect();
        while let Some(inc) = rx.recv().await {
            let msg: ChanMessage = match serde_json::from_slice(&inc.payload) {
                Ok(m) => m,
                Err(e) => {
                    warn!(error = %e, topic = %inc.topic, "payload MQTT ilegible, lo ignoro");
                    continue;
                }
            };

            match bridge::classify_inbound(&msg, &pubkey, &cfg.channels) {
                Inbound::EcoPropio => {
                    debug!(id = %msg.id, "eco propio de vuelta del hub, ignorado");
                    continue;
                }
                Inbound::OtroCanal => {
                    debug!(id = %msg.id, canal = %msg.channel, "payload de otro canal, ignorado");
                    continue;
                }
                Inbound::VersionIncompatible => {
                    warn!(
                        id = %msg.id, v = msg.v,
                        "payload de una version mayor del protocolo; no se inyecta"
                    );
                    continue;
                }
                Inbound::Inyectar => {}
            }
            // classify_inbound ya garantizo que el canal es nuestro.
            let Some(canal) = cfg.canal_por_nombre(&msg.channel) else {
                continue;
            };

            // El id del hub manda: si ya lo vimos (lo publicamos nosotros, o ya
            // lo inyectamos), no se repite.
            if !dedup.lock().await.insert_if_absent(&msg.id) {
                debug!(id = %msg.id, "duplicado, no se inyecta");
                continue;
            }

            let rl = limites.get_mut(&canal.idx).expect("un limitador por canal");
            if let Err(d) = rl.try_acquire() {
                let motivo = match d {
                    Denial::SinTokens => "sin tokens (RATE_PER_MIN)",
                    Denial::MuySeguido => "muy seguido (RATE_MIN_INTERVAL_S)",
                };
                warn!(id = %msg.id, canal = %canal.name, de = %msg.sender, motivo, "DESCARTADO por rate-limit");
                continue;
            }

            let texto = bridge::format_injection(
                &msg.origin_island,
                &msg.sender,
                &msg.sender_id,
                &msg.text,
                max_texto,
            );
            // No hace falta marcar el id del eco: el eco RF de esta inyeccion lo
            // transmite NUESTRO nodo, y la regla §8.1.1 (nunca republicamos lo
            // propio) ya corta el lazo.

            let Some(mc) = nodo.read().await.clone() else {
                warn!(id = %msg.id, "el nodo no esta conectado, mensaje perdido");
                continue;
            };
            let r = mc
                .commands()
                .lock()
                .await
                .send_channel_msg(canal.idx, &texto, None)
                .await;
            match r {
                Ok(()) => info!(id = %msg.id, canal = %canal.name, de = %msg.sender, isla = %msg.origin_island, "inyectado a la RF: {texto}"),
                Err(e) => warn!(id = %msg.id, error = %e, "fallo la inyeccion a la RF"),
            }
        }
        warn!("el inyector se quedo sin canal de entrada");
    });
}

/// Reconecta el nodo y vuelve a levantar la sesion RF -> MQTT cada vez que cae.
async fn supervisar_sesiones(
    cfg: Arc<Config>,
    pubkey: Arc<String>,
    nombre_nodo: Arc<String>,
    nodo: NodoCompartido,
    dedup: Arc<Mutex<Dedup>>,
    hub: mqtt::MqttHandle,
) {
    loop {
        let mc = match nodo.read().await.clone() {
            Some(mc) => mc,
            None => {
                let (mc, _, _) = conectar_e_identificar(&cfg).await;
                info!("nodo reconectado");
                *nodo.write().await = Some(mc.clone());
                mc
            }
        };

        let r = correr_sesion(&cfg, &pubkey, &nombre_nodo, &mc, &dedup, &hub).await;
        match r {
            Ok(()) => warn!("la sesion con el nodo termino sola"),
            Err(e) => warn!(error = %format!("{e:#}"), "la sesion con el nodo fallo"),
        }
        let _ = mc.disconnect().await;
        *nodo.write().await = None;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

/// RF -> MQTT + watchdog de la cola de mensajes.
async fn correr_sesion(
    cfg: &Config,
    pubkey: &str,
    nombre_nodo: &str,
    mc: &Arc<MeshCore>,
    dedup: &Arc<Mutex<Dedup>>,
    hub: &mqtt::MqttHandle,
) -> Result<()> {
    let mut stream = mc.event_stream();
    let mut tick = tokio::time::interval(WATCHDOG_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let drenando = Arc::new(AtomicBool::new(false));
    let mut ultimo_mensaje = Instant::now();
    let mut ultimo_canal = Instant::now();
    let mut aviso_canal = false;
    let mut aviso_pendiente = false;

    // Al arrancar, la cola del device puede traer cosas de antes.
    let rescatados = mesh::drain_queue(mc).await;
    if rescatados > 0 {
        info!(rescatados, "cola del device drenada al arranque");
    }

    loop {
        tokio::select! {
            ev = stream.next() => {
                let Some(ev) = ev else {
                    anyhow::bail!("el stream de eventos del nodo se cerro");
                };
                match ev.event_type {
                    EventType::MessagesWaiting => {
                        debug!("el device avisa mensajes en cola");
                        aviso_pendiente = true;
                        lanzar_drenaje(mc.clone(), drenando.clone());
                    }
                    EventType::ChannelMsgRecv => {
                        ultimo_mensaje = Instant::now();
                        aviso_pendiente = false;
                        if let EventPayload::ChannelMessage(m) = ev.payload {
                            let Some(canal) = cfg.canal_por_idx(m.channel_idx) else {
                                debug!(idx = m.channel_idx, "mensaje de un canal no puenteado, ignorado");
                                continue;
                            };
                            ultimo_canal = Instant::now();
                            aviso_canal = false;
                            procesar_rf(cfg, canal, pubkey, nombre_nodo, &m.text, dedup, hub).await;
                        }
                    }
                    EventType::ContactMsgRecv => {
                        // ALCANCE: el agente no toca DMs. Solo cuenta como senal
                        // de vida para el watchdog.
                        ultimo_mensaje = Instant::now();
                        aviso_pendiente = false;
                        debug!("DM recibido, fuera de alcance (ignorado)");
                    }
                    EventType::Disconnected => {
                        anyhow::bail!("el nodo reporto desconexion");
                    }
                    otro => debug!(?otro, "evento del nodo"),
                }
            }
            _ = tick.tick() => {
                // (a) el device aviso mensajes y no se entrego ninguno.
                if aviso_pendiente {
                    warn!("hubo aviso de mensajes en cola sin entrega; drenando a mano");
                    aviso_pendiente = false;
                    lanzar_drenaje(mc.clone(), drenando.clone());
                }
                // (b) silencio total prolongado.
                if ultimo_mensaje.elapsed() >= SIN_MENSAJES_DRENAR {
                    warn!(segundos = ultimo_mensaje.elapsed().as_secs(), "sin mensajes hace rato; drenando a mano");
                    ultimo_mensaje = Instant::now();
                    lanzar_drenaje(mc.clone(), drenando.clone());
                }
                // (c) el aviso del bug de firmware.
                if !aviso_canal && ultimo_canal.elapsed() >= SIN_CANAL_AVISAR {
                    aviso_canal = true;
                    warn!(
                        minutos = ultimo_canal.elapsed().as_secs() / 60,
                        "sin eventos de mensaje de canal. Puede ser malla tranquila, \
                         o el bug conocido del firmware Companion (issue #1232) en que \
                         el evento no dispara. El agente sigue vivo."
                    );
                }
            }
        }
    }
}

/// Un drenaje a la vez; si ya hay uno corriendo, no encolamos otro.
fn lanzar_drenaje(mc: Arc<MeshCore>, drenando: Arc<AtomicBool>) {
    if drenando
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }
    tokio::spawn(async move {
        let n = mesh::drain_queue(&mc).await;
        if n > 0 {
            warn!(rescatados = n, "watchdog: mensajes rescatados de la cola del device");
        }
        drenando.store(false, Ordering::SeqCst);
    });
}

/// RF -> MQTT: un mensaje de un canal puenteado oido por la radio.
async fn procesar_rf(
    cfg: &Config,
    canal: &Canal,
    pubkey: &str,
    nombre_nodo: &str,
    raw: &str,
    dedup: &Arc<Mutex<Dedup>>,
    hub: &mqtt::MqttHandle,
) {
    let (nick, texto) = bridge::split_sender(raw);
    let sender = nick.unwrap_or_default();

    // Spec §8.1.1: nunca republicamos lo que transmitio nuestro propio nodo (es
    // el eco de una inyeccion volviendo por el reflood del repetidor). La spec
    // compara pubkeys; el frame de canal del Companion no trae ninguna, asi que
    // comparamos el nick contra el nombre de nuestro nodo.
    if !sender.is_empty() && sender == nombre_nodo {
        debug!(texto = %texto, "eco de nuestra propia inyeccion, no se publica");
        return;
    }
    if texto.is_empty() {
        debug!("mensaje de canal vacio, ignorado");
        return;
    }

    // DESVIACION DOCUMENTADA de la spec §6: `sender_id` deberia ser la pubkey
    // del nodo de origen, pero CHANNEL_MSG_RECV no trae identidad del remitente
    // (ver README). En un observer Companion lo mas estable que hay es el nick.
    let sender_id = sender.clone();

    let ts = bridge::now_epoch();
    let id = bridge::message_id(&canal.name, &sender_id, &texto, ts);
    let id_previo = bridge::message_id_previo(&canal.name, &sender_id, &texto, ts);
    {
        let mut cache = dedup.lock().await;
        // Spec §7: probamos tambien el bucket anterior, para no republicar un
        // mensaje que otro avistamiento vio a caballo del limite de 30 s.
        if cache.contains(&id_previo) {
            debug!(id = %id, "ya visto en el bucket anterior, no se republica");
            return;
        }
        if !cache.insert_if_absent(&id) {
            debug!(id = %id, "ya visto, no se republica");
            return;
        }
    }

    let msg = ChanMessage {
        v: bridge::PROTOCOL_VERSION,
        id: id.clone(),
        channel: canal.name.clone(),
        text: texto.clone(),
        sender: sender.clone(),
        sender_id,
        origin_bridge: pubkey.to_string(),
        origin_island: cfg.island_iata.clone(),
        ts,
    };
    match serde_json::to_vec(&msg) {
        Ok(body) => match hub.publish(&cfg.topic(canal), body).await {
            Ok(()) => info!(id = %id, canal = %canal.name, de = %sender, "publicado al hub: {texto}"),
            Err(e) => warn!(id = %id, error = %format!("{e:#}"), "no se pudo publicar al hub"),
        },
        Err(e) => warn!(error = %e, "no se pudo serializar el mensaje"),
    }
}
