# bridge-msc

Puente del **canal público de MeshCore** entre la radio local y un hub MQTT, para
que islas de malla que no se alcanzan por RF compartan igual el mismo canal.

Un mensaje enviado al canal público en Santiago aparece en el canal público de
Concepción, con su procedencia marcada, sin extender la malla ni saturar el aire.

Es la **implementación de referencia del protocolo meshchan v0.1**, cuya
especificación va incluida acá al lado, en
[`meshchan-spec-v0.1.md`](meshchan-spec-v0.1.md), pensada para que se pueda
escribir otra implementación sin leer este código.

Un solo binario de ~2,6 MB, configuración por variables de entorno, ~1 MB de RAM
en marcha. Corre en cualquier host Linux (una Raspberry Pi, un mini-PC) con un
nodo MeshCore Companion conectado por USB.

> **Nota.** meshchan es una propuesta de la comunidad MeshChile. No está
> afiliada al proyecto MeshCore ni ratificada por él.

---

## Índice

1. [Qué hace y qué no](#1-qué-hace-y-qué-no)
2. [Cómo se ve en la práctica](#2-cómo-se-ve-en-la-práctica)
3. [Requisitos](#3-requisitos)
4. [Instalación](#4-instalación)
5. [Configuración](#5-configuración)
6. [Cómo comprobar que funciona](#6-cómo-comprobar-que-funciona)
7. [Operación diaria](#7-operación-diaria)
8. [Cuando algo falla](#8-cuando-algo-falla)
9. [Detalles del protocolo que conviene saber](#9-detalles-del-protocolo-que-conviene-saber)
10. [Observabilidad (opcional)](#10-observabilidad-opcional)
11. [Desarrollo](#11-desarrollo)

---

## 1. Qué hace y qué no

Relaya **el texto de un solo canal público acordado**, con procedencia, límites
de airtime y protección de lazos. Es un puente **curado a nivel de canal**, no
una extensión transparente de la malla.

Un bridge conforme **nunca** relaya (spec §2):

- mensajes directos (DM) ni nada que no sea el canal público configurado;
- adverts, contactos, telemetría, posición ni metadatos de nodos;
- otros canales;
- tramas RF crudas.

La observabilidad de nodos (posiciones para un mapa) es un **plano aparte**, con
su propio árbol de topics y su propio broker. Este agente la trae como módulo
opcional, apagado por defecto, y aun encendido no la mezcla con meshchan.

## 2. Cómo se ve en la práctica

Alguien en Concepción escribe en el canal público:

```
alguien copia en la zona sur?
```

Su bridge local lo publica al hub. En Santiago, tu bridge lo recibe y lo inyecta
a tu radio, y en los nodos de tu isla aparece:

```
tu-nodo: [CCP] juan: alguien copia en la zona sur?
```

Tres cosas que vale la pena notar en esa línea:

- `tu-nodo:` lo antepone **el firmware**, no este programa. Todo mensaje de canal
  en MeshCore lleva el nombre del nodo que lo transmitió.
- `[CCP]` es la procedencia, que agrega el bridge. Sin ella, el mensaje se leería
  como si lo hubiera dicho tu nodo.
- `juan` es el nombre que usó el remitente original. **No está autenticado**: en
  un canal público de MeshCore cualquiera con la clave del canal puede firmar con
  el nombre que quiera. Trátalo como informativo, nunca como prueba de origen.

## 3. Requisitos

1. **Un nodo MeshCore con firmware Companion**, conectado al host por USB serial
   (también soporta TCP y BLE). Un T-LoRa, un Heltec, un Xiao: cualquiera que
   corra Companion sirve.
2. **El canal público ya configurado en ese nodo**: nombre y clave compartida.
   El agente *usa* el canal, no lo configura. Los datos del canal público de tu
   comunidad se piden a la comunidad y se cargan en el nodo **antes** de levantar
   el agente.
3. **Credenciales propias del hub MQTT**. Cada bridge debería tener las suyas,
   para que se puedan revocar por separado (spec §4).
4. El usuario que corre el agente en el grupo `dialout`, si usas serial.
5. Rust 1.75 o superior para compilar.

> ### ⚠️ El agente es dueño EXCLUSIVO del puerto del nodo
>
> Ningún otro proceso puede tener abierto el mismo serial en paralelo: ni un
> publisher de mapa, ni un bot, ni un clock-sync. Dos procesos peleando el mismo
> `/dev/ttyACM0` terminan en que ninguno de los dos anda.
>
> Si ya tienes algo hablándole a ese nodo, dedica **otro** nodo al bridge. Es la
> forma recomendada de montarlo.

## 4. Instalación

### Compilar

```bash
git clone https://github.com/Mesh-Chile/bridge-msc.git
cd bridge-msc
cargo build --release
```

En un host sin bluetooth, un binario más chico y sin dependencias de D-Bus:

```bash
cargo build --release --no-default-features   # ~2,6 MB en vez de ~3,1 MB
```

### Instalar como servicio

```bash
sudo cp target/release/bridge-msc /usr/local/bin/

sudo cp .env.example /etc/bridge-msc.env
sudo $EDITOR /etc/bridge-msc.env
sudo chmod 600 /etc/bridge-msc.env          # trae credenciales

sudo useradd -r -s /usr/sbin/nologin bridge-msc     # o usa un usuario existente
sudo cp bridge-msc.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bridge-msc
journalctl -u bridge-msc -f
```

La unit trae `Restart=always`, `MemoryMax=128M` y `SupplementaryGroups=dialout`.
Si usas TCP o BLE en vez de serial, puedes sacar ese `SupplementaryGroups`.

**Un detalle que ahorra dolores de cabeza:** apunta `MC_ADDRESS` a
`/dev/serial/by-id/...` y no a `/dev/ttyACM0`. El número cambia al reenchufar o
al reiniciar; el `by-id` no. Para ver el tuyo:

```bash
ls -l /dev/serial/by-id/
```

### Compilar para otra arquitectura

El TLS es 100 % Rust (`rustls` + `ring`), así que **no hace falta cmake ni
toolchain C** para la parte de red: solo el enlazador cruzado.

```bash
# arm64 (Raspberry Pi 64 bits, Oracle Ampere)
rustup target add aarch64-unknown-linux-gnu
sudo apt install gcc-aarch64-linux-gnu
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
  cargo build --release --no-default-features --target aarch64-unknown-linux-gnu
```

Ojo con la glibc: un binario compilado contra una glibc nueva no corre en un host
con una más vieja. Compila en un host de glibc igual o menor que la del destino,
o usa `--target x86_64-unknown-linux-musl` para un estático.

Si compilas **con** la feature `ble` (viene por defecto), el target necesita
además las cabeceras de D-Bus.

## 5. Configuración

Todo va por variables de entorno. [`.env.example`](.env.example) está comentado
entero; lo mínimo es:

```ini
MC_ADDRESS=/dev/serial/by-id/usb-XXXX-if00
CHANNEL_NAME=Public
CHANNEL_IDX=0
ISLAND_IATA=SCL
CHAN_MQTT_TRANSPORT=tcp
CHAN_MQTT_HOST=hub.ejemplo.cl
CHAN_MQTT_PORT=8883
CHAN_MQTT_USER=bridge-scl
CHAN_MQTT_PASS=...
```

Las que más se prestan a confusión:

| Variable | Qué es |
|---|---|
| `CHANNEL_NAME` | El nombre del canal **tal como está en el nodo**, y también el último nivel del topic. Distingue mayúsculas. |
| `CHANNEL_IDX` | El índice del canal en el nodo, casi siempre `0`. Si no calza, el agente no oye nada. |
| `ISLAND_IATA` | La etiqueta de tu isla. Es lo que las otras islas ven como `[SCL]`. |
| `CHAN_MQTT_TRANSPORT` | `tcp` para MQTT nativo (1883 / 8883), `ws` para websockets (443). |
| `CHAN_MQTT_TLS` | `true` por defecto. Ponlo en `false` solo en una red de confianza. |

El topic queda `<CHAN_TOPIC_PREFIX>[/<CHAN_REGION>]/<CHANNEL_NAME>`; con los
valores por defecto y `CHAN_REGION=CL` da `meshchan/CL/Public`.

> **Todos los bridges de un mismo canal tienen que usar exactamente el mismo
> topic** (spec §5). Si el tuyo no calza con el de la comunidad, vas a estar solo
> en el hub sin darte cuenta.

Para ver qué canales tiene cargados tu nodo, sin adivinar:

```bash
MC_ADDRESS=/dev/serial/by-id/usb-XXXX-if00 cargo run --example node_info
```

Imprime nombre y pubkey del nodo, los canales con sus índices y los contactos.
**No lo corras con el servicio andando**: pelearían por el serial.

## 6. Cómo comprobar que funciona

### Primero el hub, sin radio

```bash
CHANNEL_NAME=Public ISLAND_IATA=SCL CHAN_REGION=CL \
CHAN_MQTT_TRANSPORT=tcp CHAN_MQTT_HOST=hub.ejemplo.cl CHAN_MQTT_PORT=8883 \
CHAN_MQTT_USER=bridge-scl CHAN_MQTT_PASS=... \
MC_ADDRESS=/dev/null \
cargo run --example hub_smoke
```

Publica un mensaje meshchan válido y espera recibirlo de vuelta por su propia
suscripción. Tiene que terminar en `OK: ida y vuelta completa`. Si no vuelve
nada, el problema son las credenciales, la ACL del topic o el puerto — todavía no
has tocado la radio.

### RF → MQTT

1. Suscríbete al topic desde otra máquina.
2. Manda un mensaje al canal público desde cualquier nodo de tu isla.
3. Tiene que aparecer el JSON, y en el journal `publicado al hub: <texto>`.

### MQTT → RF

Publica un payload al topic con un `origin_bridge` **distinto** del de tu nodo
(si pones el tuyo, el agente lo ignora, y hace bien):

```json
{"v":1,"id":"0123456789abcdef","channel":"Public","text":"prueba desde el hub",
 "sender":"pepe","sender_id":"pepe","origin_bridge":"0000",
 "origin_island":"CCP","ts":1756742400}
```

En la RF tiene que aparecer `[CCP] pepe: prueba desde el hub`, y en el journal
`inyectado a la RF: ...`.

Después vale la pena probar los límites:

- Repite el mismo payload: la segunda vez tiene que decir
  `duplicado, no se inyecta`.
- Manda varios seguidos: a partir del quinto,
  `DESCARTADO por rate-limit`.

### Que no haya lazo

Con las dos direcciones andando, un mensaje inyectado se oye de vuelta por RF
cuando el repetidor lo re-emite. **No** debe republicarse al hub. Con
`RUST_LOG=bridge_msc=debug` vas a ver `eco de nuestra propia inyeccion, no se
publica`. Si en vez de eso ves el mismo texto rebotando, revisa que el nombre de
tu nodo sea el que efectivamente aparece como prefijo en la RF.

## 7. Operación diaria

```bash
systemctl status bridge-msc
journalctl -u bridge-msc -f
journalctl -u bridge-msc | grep -E 'DESCARTADO|watchdog|rate-limit'
```

Reconecta solo: al nodo con backoff exponencial (2 s → 60 s) y a cada broker MQTT
por su cuenta. Ninguna tarea que falle tumba el proceso. `SIGTERM` y `Ctrl-C`
cierran el nodo y las conexiones MQTT ordenadamente.

Para actualizar: recompila, reemplaza el binario y `systemctl restart bridge-msc`.

## 8. Cuando algo falla

**`el nodo no respondio la identificacion inicial`**
El puerto abrió pero el nodo no contesta. Suele ser transitorio, justo después de
que otro proceso soltó el serial; el agente reintenta solo con backoff. Si
persiste: revisa que el firmware sea **Companion** (un repeater no habla ese
protocolo) y que nadie más tenga el puerto tomado.

**No aparece ningún mensaje de canal**
Revisa `CHANNEL_IDX` con `--example node_info`. Si pasan 5 minutos sin ningún
mensaje de canal, el agente deja un WARNING y **sigue vivo**: en una malla
tranquila eso es normal de madrugada. Pero en algunas versiones del firmware
Companion el evento de mensaje de canal directamente no dispara aunque el nodo sí
esté oyendo (issue #1232 aguas arriba); ese WARNING es la pista.

**`watchdog: mensajes rescatados de la cola del device`**
Es el agente haciendo su trabajo, no un error. La librería solo espera cuatro
tipos de respuesta al pedir el siguiente mensaje: si en la cola hay una trama que
no conoce (por ejemplo `CHANNEL_DATA_RECV` = 27, de firmwares recientes), la
espera expira. Esa trama **ya salió** de la cola del device, así que cortar el
drenaje ahí es justo lo que deja al bridge sordo hasta el próximo aviso. El
agente no corta: sigue drenando, y avisa cuando rescató algo.

**El texto llega cortado**
El firmware corta el mensaje de canal en **160 bytes contando el prefijo
`"<nombre_del_nodo>: "` que agrega solo**. El agente calcula el techo real al
arrancar y lo deja en el log. Con un nodo llamado `cl-bridge` quedan 149 bytes
útiles. **Ponle nombre corto a tu nodo**: cada carácter se le resta al mensaje.

**Nada llega al hub y el log dice `conexion MQTT caida`**
Verifica primero con `--example hub_smoke`, que no toca la radio. Si ahí falla,
es credenciales, ACL o puerto.

## 9. Detalles del protocolo que conviene saber

- **`sender_id` es el nombre del remitente, no su pubkey.** No es una limitación
  de este programa ni del API Companion: un mensaje de canal viaja como
  `PAYLOAD_TYPE_GRP_TXT`, que el propio firmware documenta como *"an
  (unverified) group text message (prefixed with channel hash, MAC) (enc data:
  timestamp, `"name: msg"`)"*. No hay pubkey, ni hash del emisor, ni firma. El
  único rastro del remitente es el nombre dentro del texto cifrado. Ni siquiera
  una implementación en firmware, leyendo el paquete crudo, tiene algo mejor.
- **El texto va byte a byte, sin normalizar.** Se saca del texto RF solo el
  separador —los dos puntos y **un** espacio, que es exactamente lo que escribe
  `sendGroupMessage()` con `"%s: "`— y nada más. Ni `trim`, ni normalización de
  espacios. Es deliberado y es contrato de cable: el `id` canónico se calcula
  sobre el texto, así que si una implementación normaliza y otra no, dejan de
  deduplicar entre sí. Un espacio al final hace un mensaje distinto, y está bien
  que lo sea.
- **Anti-loop.** Nunca se republica lo que transmitió el propio nodo (spec
  §8.1.1). Como la trama no trae pubkey, la comparación es contra el nombre del
  nodo, que es justamente lo que el firmware antepone.
- **El límite de los 30 s.** Los buckets del `id` son una grilla fija, no una
  ventana móvil. Al publicar se prueba también el id del bucket anterior, como
  permite la spec §7, para cerrar el hueco del mensaje oído a caballo de un
  límite.
- **La cache de dedup es acotada de verdad**: TTL más techo duro de entradas, sin
  crecimiento ilimitado (spec §8.5).
- **El rate-limit solo aplica a MQTT → RF.** Lo que se oyó por radio ya gastó
  airtime; publicarlo es gratis. Los límites del §8.4 son un mecanismo de
  seguridad, no una perilla de tuning: subirlos mucho satura la RF de tu isla.

## 10. Observabilidad (opcional)

Con `OBS_ENABLED=false` (el valor por defecto) el agente es un bridge puro: no se
crea el segundo cliente MQTT y no se toca la lista de contactos del nodo. El
módulo no cuesta nada.

Encendido, cada `MAP_INTERVAL` segundos publica los contactos con posición a un
broker de observabilidad, en su propio árbol de topics. Enciéndelo solo si
acordaste con el operador de ese mapa publicar lo que ve tu observer, y solo con
las credenciales que él te dé.

## 11. Desarrollo

```bash
cargo test                        # 47 tests, ninguno necesita radio ni broker
cargo clippy --all-targets
RUST_LOG=bridge_msc=debug cargo run
```

El crate es biblioteca + binario: los módulos viven en `src/lib.rs`, y el binario
y los ejemplos los consumen.

| Módulo | Qué hace |
|---|---|
| `config` | Lee y valida el entorno |
| `mesh` | Conexión al nodo y drenaje defensivo de la cola |
| `bridge` | Protocolo meshchan y toda la lógica pura |
| `mqtt` | Clientes MQTT (tcp/ws, con y sin TLS) y reconexión |
| `dedup` | Cache acotada por TTL y por tamaño |
| `ratelimit` | Token bucket de la inyección |
| `observability` | El módulo opcional del mapa |

Todo lo que se puede testear sin hardware está en funciones puras, y los tests
cubren el vector de referencia del `id`, los casos raros del parseo de nicks, el
recorte UTF-8, el techo de texto del firmware y el comportamiento del rate-limit.

Ejemplos: `hub_smoke` valida un hub MQTT de punta a punta sin radio; `node_info`
muestra qué ve el agente en el nodo.

## Licencia

MIT.
