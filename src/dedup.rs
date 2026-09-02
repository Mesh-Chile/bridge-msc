//! Cache de deduplicacion ACOTADO: TTL + techo duro de entradas.
//!
//! Es lo que corta el lazo RF <-> MQTT: un mensaje que ya vimos (lo publicamos
//! nosotros, o lo inyectamos y lo vamos a oir de vuelta por RF) no se reenvia.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

pub struct Dedup {
    ttl: Duration,
    max: usize,
    seen: HashMap<String, Instant>,
    /// Orden de insercion, para poder podar el mas viejo sin recorrer el mapa.
    order: VecDeque<String>,
}

impl Dedup {
    pub fn new(ttl: Duration, max: usize) -> Self {
        let max = max.max(16);
        Self {
            ttl,
            max,
            seen: HashMap::with_capacity(max.min(1024)),
            order: VecDeque::with_capacity(max.min(1024)),
        }
    }

    /// Inserta el id si no estaba. Devuelve `true` si es nuevo (hay que
    /// procesarlo), `false` si es un duplicado.
    pub fn insert_if_absent(&mut self, id: &str) -> bool {
        let now = Instant::now();
        self.prune(now);
        if let Some(t) = self.seen.get(id) {
            if now.duration_since(*t) < self.ttl {
                return false;
            }
        }
        if self.seen.insert(id.to_string(), now).is_none() {
            self.order.push_back(id.to_string());
        }
        self.enforce_cap();
        true
    }

    /// Marca un id como visto sin preguntar. El bridge no lo necesita (§8.1.1
    /// ya corta el eco), pero es parte natural de la cache y esta testeado.
    #[allow(dead_code)]
    pub fn mark(&mut self, id: &str) {
        let now = Instant::now();
        if self.seen.insert(id.to_string(), now).is_none() {
            self.order.push_back(id.to_string());
        }
        self.enforce_cap();
    }

    /// Solo lo usan los tests y quien quiera inspeccionar la cache.
    #[allow(dead_code)]
    pub fn contains(&self, id: &str) -> bool {
        match self.seen.get(id) {
            Some(t) => Instant::now().duration_since(*t) < self.ttl,
            None => false,
        }
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }

    /// Poda por TTL desde el frente (el orden de insercion es orden de tiempo).
    fn prune(&mut self, now: Instant) {
        while let Some(front) = self.order.front() {
            match self.seen.get(front) {
                None => {
                    self.order.pop_front();
                }
                Some(t) if now.duration_since(*t) >= self.ttl => {
                    let k = self.order.pop_front().unwrap();
                    self.seen.remove(&k);
                }
                Some(_) => break,
            }
        }
    }

    /// Techo duro: si la poda por TTL no alcanzo, botamos los mas viejos.
    fn enforce_cap(&mut self) {
        while self.seen.len() > self.max {
            match self.order.pop_front() {
                Some(k) => {
                    self.seen.remove(&k);
                }
                None => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primera_vez_pasa_y_la_segunda_no() {
        let mut d = Dedup::new(Duration::from_secs(60), 100);
        assert!(d.insert_if_absent("a"));
        assert!(!d.insert_if_absent("a"));
        assert!(d.insert_if_absent("b"));
    }

    #[test]
    fn mark_bloquea_el_eco() {
        let mut d = Dedup::new(Duration::from_secs(60), 100);
        d.mark("eco");
        assert!(!d.insert_if_absent("eco"));
    }

    #[test]
    fn ttl_vencido_vuelve_a_pasar() {
        let mut d = Dedup::new(Duration::from_millis(1), 100);
        assert!(d.insert_if_absent("a"));
        std::thread::sleep(Duration::from_millis(5));
        assert!(d.insert_if_absent("a"));
    }

    #[test]
    fn nunca_crece_mas_alla_del_techo() {
        let mut d = Dedup::new(Duration::from_secs(3600), 32);
        for i in 0..10_000 {
            d.insert_if_absent(&format!("id-{i}"));
        }
        assert!(d.len() <= 32, "len={}", d.len());
        // Y el ultimo insertado sigue ahi.
        assert!(d.contains("id-9999"));
    }

    #[test]
    fn el_techo_minimo_no_es_cero() {
        let mut d = Dedup::new(Duration::from_secs(60), 0);
        for i in 0..100 {
            d.insert_if_absent(&format!("x{i}"));
        }
        assert!(!d.is_empty() && d.len() <= 16);
    }
}
