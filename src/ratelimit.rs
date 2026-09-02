//! Token bucket para la inyeccion MQTT -> RF.
//!
//! La RF es el cuello de botella: solo se limita esta direccion. RF -> MQTT no
//! se limita nunca (lo que se oyo por radio ya gasto airtime, publicarlo es gratis).

use std::time::{Duration, Instant};

pub struct RateLimiter {
    capacity: f64,
    /// Tokens que se recargan por segundo.
    refill_per_sec: f64,
    min_interval: Duration,
    tokens: f64,
    last_refill: Instant,
    last_grant: Option<Instant>,
}

/// Por que se rechazo una inyeccion (para loguear distinto).
#[derive(Debug, PartialEq, Eq)]
pub enum Denial {
    /// No quedan tokens: se supero RATE_PER_MIN sostenido.
    SinTokens,
    /// Hay tokens pero paso muy poco desde el envio anterior.
    MuySeguido,
}

impl RateLimiter {
    pub fn new(per_min: u32, burst: u32, min_interval: Duration) -> Self {
        let capacity = burst.max(1) as f64;
        Self {
            capacity,
            refill_per_sec: per_min as f64 / 60.0,
            min_interval,
            // Arranca lleno: un bridge recien levantado puede pasar la rafaga inicial.
            tokens: capacity,
            last_refill: Instant::now(),
            last_grant: None,
        }
    }

    pub fn try_acquire(&mut self) -> Result<(), Denial> {
        self.try_acquire_at(Instant::now())
    }

    /// Version testeable con reloj inyectado.
    pub fn try_acquire_at(&mut self, now: Instant) -> Result<(), Denial> {
        let elapsed = now.saturating_duration_since(self.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
            self.last_refill = now;
        }
        if let Some(last) = self.last_grant {
            if now.saturating_duration_since(last) < self.min_interval {
                return Err(Denial::MuySeguido);
            }
        }
        if self.tokens < 1.0 {
            return Err(Denial::SinTokens);
        }
        self.tokens -= 1.0;
        self.last_grant = Some(now);
        Ok(())
    }

    #[allow(dead_code)]
    pub fn tokens(&self) -> f64 {
        self.tokens
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deja_pasar_la_rafaga_y_despues_corta() {
        let mut rl = RateLimiter::new(10, 3, Duration::ZERO);
        let t = Instant::now();
        assert!(rl.try_acquire_at(t).is_ok());
        assert!(rl.try_acquire_at(t).is_ok());
        assert!(rl.try_acquire_at(t).is_ok());
        assert_eq!(rl.try_acquire_at(t), Err(Denial::SinTokens));
    }

    #[test]
    fn recarga_con_el_tiempo() {
        let mut rl = RateLimiter::new(60, 1, Duration::ZERO); // 1 token/seg
        let t0 = Instant::now();
        assert!(rl.try_acquire_at(t0).is_ok());
        assert_eq!(rl.try_acquire_at(t0), Err(Denial::SinTokens));
        assert!(rl.try_acquire_at(t0 + Duration::from_secs(2)).is_ok());
    }

    #[test]
    fn respeta_el_intervalo_minimo() {
        let mut rl = RateLimiter::new(600, 10, Duration::from_secs(3));
        let t0 = Instant::now();
        assert!(rl.try_acquire_at(t0).is_ok());
        assert_eq!(
            rl.try_acquire_at(t0 + Duration::from_secs(1)),
            Err(Denial::MuySeguido)
        );
        assert!(rl.try_acquire_at(t0 + Duration::from_secs(3)).is_ok());
    }

    #[test]
    fn nunca_acumula_mas_que_la_capacidad() {
        let mut rl = RateLimiter::new(600, 2, Duration::ZERO);
        let t0 = Instant::now();
        let _ = rl.try_acquire_at(t0 + Duration::from_secs(3600));
        assert!(rl.tokens() <= 2.0);
    }
}
