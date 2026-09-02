//! bridge-msc como biblioteca, para que los ejemplos y los tests puedan
//! usar las mismas piezas que el binario.
//!
//! El protocolo esta en [`bridge`]; ver `meshchan-spec-v0.1.md`.

pub mod bridge;
pub mod config;
pub mod dedup;
pub mod mesh;
pub mod mqtt;
pub mod observability;
pub mod ratelimit;
