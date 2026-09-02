# meshchan — Public Channel Bridge Protocol for MeshCore

**Version:** 0.1 (Draft)
**Status:** Proposal — originated by the MeshChile community. Not affiliated with,
nor ratified by, the MeshCore project.
**Reference implementation:** `meshchan-agent` (Rust).

The key words MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY in this document are to be
interpreted as described in RFC 2119.

---

## 1. Motivation

MeshCore is off-grid-first and has no built-in message backhaul. When a group of
MeshCore nodes cannot reach the wider network over RF — separated by distance or
terrain — they form an isolated island. meshchan lets those islands participate in a
shared **public channel** by relaying that channel's messages over an MQTT hub, so a
message sent on the public channel in one island reaches the public channel in every
other island.

meshchan is a **curated, channel-level** bridge: it relays the text of one agreed
public channel, with provenance, rate limits, and loop protection. It is deliberately
**not** a transparent mesh extension.

## 2. Scope

**In scope:** relaying text messages of a single, pre-agreed public channel between
RF and an MQTT hub, in both directions, across multiple bridges.

**Out of scope (a conformant bridge MUST NOT relay):**
- Direct messages (DMs) or any non-public-channel traffic.
- Adverts, contact records, telemetry, position, or any node metadata.
- Any channel other than the configured public channel.
- Raw RF frames or packet-level data.

Node observability (positions, adverts for a map) is a **separate plane** with its own
topic tree and brokers; meshchan does not address it.

## 3. Terminology

- **Public channel:** a MeshCore channel (name + shared key) agreed by a community as
  its public channel. Its key distribution is out of band.
- **Bridge:** a node + host (or firmware) that relays the public channel between RF and
  the hub. Identified by its MeshCore node public key.
- **Island:** the set of nodes reachable over RF from a given bridge.
- **Hub:** the MQTT broker all bridges connect to.
- **Origin bridge:** the bridge that published a given message to the hub.

## 4. Transport and hub

- The hub MUST be an MQTT broker. Bridges SHOULD connect over MQTT-over-WebSocket with
  TLS (`wss`). Plain TCP MAY be used on trusted networks.
- meshchan is **broker-agnostic**: any compliant MQTT broker (e.g. Mosquitto) works.
  It does NOT require any specialized MeshCore-aware broker.
- Bridge authentication to the hub (username/password, ACLs) is out of band and is the
  hub operator's responsibility. Each bridge SHOULD have its own credentials so access
  can be revoked per bridge.

## 5. Topic structure

```
<prefix>/<region>/<channel>
```

- `<prefix>` — namespace root. Default: `meshchan`. Deployments MAY add a country
  segment, e.g. `meshchan/CL`.
- `<region>` — OPTIONAL grouping segment (a country or area code). When a country
  segment is used as the prefix, `<region>` MAY be omitted.
- `<channel>` — the public channel name.

Example: `meshchan/CL/publica`

All bridges relaying the same public channel MUST publish to, and subscribe to, the
exact same topic. A bridge subscribes to the topic to receive messages from other
islands, and publishes to it messages heard on RF.

## 6. Message format

Every meshchan message is a UTF-8 JSON object. Unknown fields MUST be ignored (for
forward compatibility). Fields:

| Field | Type | Req | Description |
|---|---|---|---|
| `v` | integer | MUST | Protocol version. `1` for this spec. |
| `id` | string | MUST | Canonical message id (§7). 16 lowercase hex chars. |
| `channel` | string | MUST | Public channel name (matches the topic). |
| `text` | string | MUST | Original message text, UTF-8. No attribution prefix. |
| `sender` | string | SHOULD | Human-readable sender display name. MAY be empty. |
| `sender_id` | string | MUST | Originating node identity (hex public key or prefix). |
| `origin_bridge` | string | MUST | Public key of the bridge that published this message. |
| `origin_island` | string | MUST | Provenance tag of the origin bridge (e.g. region/IATA code). |
| `ts` | integer | MUST | Unix time (seconds, UTC) when the origin bridge observed the message. |

Notes:
- `text` MUST be the original text as heard on RF, WITHOUT any attribution prefix.
  Attribution is applied only at RF injection (§8.3).
- `sender_id` identifies the human's originating node, not the bridge. It is distinct
  from `origin_bridge`.

## 7. Canonical message id

The `id` deduplicates a logical message across multiple hearings, multiple observing
bridges, and the RF echo of an injection. It MUST be computed as:

```
bucket   = floor(ts / 30)                       # integer, as base-10 string
preimage = channel ⟨0x1F⟩ sender_id ⟨0x1F⟩ text ⟨0x1F⟩ bucket   # UTF-8 bytes
id       = lowercasehex( SHA1(preimage)[0:8] )  # first 8 bytes -> 16 hex chars
```

- `⟨0x1F⟩` is a single ASCII Unit Separator byte (0x1F) between fields, so field
  boundaries are unambiguous.
- The 30-second `bucket` lets a sender repeat identical text after ~30 s and have it
  treated as a new message, while collapsing rapid RF floods of one message.
- **Known limitation:** a message observed on opposite sides of a 30 s boundary yields
  two ids. Implementations MAY also test the previous bucket's id to close this gap.

## 8. Bridge behavior

A bridge maintains a bounded, time-limited **dedup cache** of recently seen `id`
values (§8.5), shared across both directions.

### 8.1 RF to MQTT

On receiving a message on the configured public channel from RF, a bridge:

1. MUST ignore it if the RF sender is the bridge's own node (it is the bridge's own
   transmission or an injection echo — see §8.2).
2. MUST compute `id` (§7). If `id` is in the dedup cache, the message MUST be dropped.
3. Otherwise MUST insert `id` into the cache and publish the message to the topic, with
   `origin_bridge` set to the bridge's own public key and `origin_island` to its region
   tag.

### 8.2 MQTT to RF (loop protection)

On receiving a message from the topic, a bridge:

1. MUST ignore it if `origin_bridge` equals the bridge's own public key (its own
   publication returned by the hub).
2. MUST ignore it if `id` is already in the dedup cache.
3. Otherwise MUST insert `id` into the cache and inject the message onto the RF public
   channel (subject to §8.3 and §8.4).

Because a bridge never re-publishes messages sent by its own node (§8.1 rule 1), the RF
echo of an injection does not create a loop, even though the echo's RF sender and text
differ from the original.

### 8.3 Attribution

MeshCore messages are signed; a bridge cannot re-inject a message under the original
sender's identity. Therefore, on RF injection, the bridge MUST prepend provenance:

```
[<origin_island>] <sender>: <text>
```

If `sender` is empty, the bridge SHOULD use `[<origin_island>] <sender_id short>: <text>`
or omit the name. The composed string MUST be truncated to the channel's maximum text
length (§8.4). The attribution prefix MUST NOT be included in the `text` field when
publishing (§6).

### 8.4 Rate limiting and airtime

RF airtime is the system's bottleneck; the internet side is effectively unbounded. To
protect islands:

- Rate limiting MUST apply to the MQTT-to-RF direction (injection) only. The RF-to-MQTT
  direction MUST NOT be rate limited.
- A bridge MUST enforce a token-bucket (or equivalent) limit on injections: a burst
  capacity, a steady refill rate, and a minimum interval between injections. When no
  token is available the message MUST be dropped (not queued unbounded) and SHOULD be
  logged.
- A bridge MUST relay text only, and MUST truncate injected text to a configured
  maximum length.

Recommended defaults: burst 5, refill 10/min, minimum interval 3 s, max text 178 bytes.

### 8.5 Deduplication cache

- The cache MUST be bounded in size and time (entries expire after a TTL; recommended
  600 s). It MUST NOT grow without limit.
- A fixed-capacity structure (e.g. ring buffer or size-capped map with periodic
  pruning) SHOULD be used so memory is constant on constrained hosts.

## 9. Security and privacy considerations

- **Attribution is advisory, not authenticated.** Injected messages are transmitted by
  the bridge's node; the `[island] sender:` prefix is informational and MAY be forged by
  a malicious hub client. Consumers MUST NOT treat it as proof of origin.
- **The public channel is public.** Anything relayed is visible to every island and to
  any hub subscriber. Bridges MUST NOT relay DMs or any non-public traffic (§2).
- **No PII beyond the channel content.** meshchan payloads carry only what the public
  channel already exposes (sender name, node id, text). Bridges MUST NOT add position,
  contact lists, or telemetry.
- **Hub trust.** A malicious or compromised hub can inject arbitrary public-channel
  text into every island (subject to rate limits). Operators SHOULD treat hub access as
  sensitive and scope per-bridge credentials.
- **Airtime as a safety property.** The §8.4 limits are a safety mechanism, not a tuning
  knob; disabling them can saturate an island's RF and is non-conformant.

## 10. Conformance

A conformant meshchan bridge:
- MUST implement §5 topics, §6 message format, §7 id, and §8.1–§8.5.
- MUST restrict relaying to the single configured public channel (§2, §9).
- MUST apply injection rate limiting and text truncation (§8.4).
- MUST set `v` to the protocol version it emits and ignore unknown fields on receipt.
- MAY additionally run an unrelated observability function, provided it uses a separate
  topic tree and does not violate §2.

## 11. Relationship to other MeshCore MQTT work

meshchan is a **curated channel-message** bridge. It is distinct from, and can coexist
with:
- **Raw-frame / transparent bridges** (e.g. firmware that XOR-bridges mesh frames):
  those extend the mesh at the packet level and relay all traffic; meshchan relays one
  channel's text with provenance and airtime limits.
- **Observability publishing** (e.g. node/position feeds to a map broker): a separate
  plane with its own topics and, typically, a specialized broker; meshchan does not
  carry observability data.

An implementation MAY do both meshchan and observability, but they remain separate
concerns on separate topic trees.

## 12. Versioning and change process

- This document is `v0.1`, a draft. The wire `v` field is `1`.
- Backward-compatible additions (new OPTIONAL fields) do not bump `v`; receivers ignore
  unknown fields.
- Changes that alter the id algorithm, topic structure, or required fields MUST bump the
  major protocol version and SHOULD define a migration path.
- The canonical source of this spec is the MeshChile repository. Proposals to evolve it
  SHOULD be raised there and, where relevant, discussed with the wider MeshCore
  community before adoption.

---

## Appendix A — Example message

Published to `meshchan/CL/publica`:

```json
{
  "v": 1,
  "id": "9f2a7c1b4e6d8a03",
  "channel": "publica",
  "text": "alguien copia en la zona sur?",
  "sender": "juan",
  "sender_id": "fac180abf4d2",
  "origin_bridge": "a1b2c3d4e5f60718",
  "origin_island": "CCP",
  "ts": 1756742400
}
```

On another island tagged `SCL`, this is injected onto RF as:

```
[CCP] juan: alguien copia en la zona sur?
```

## Appendix B — Reference pseudocode

```
on_rf_channel_message(msg):
    if msg.sender_node == self.node_pubkey: return        # never relay our own tx
    id = canonical_id(channel, msg.sender_id, msg.text, now())
    if cache.contains(id): return
    cache.insert(id)
    publish(topic, {
        v:1, id, channel, text: msg.text, sender: msg.sender,
        sender_id: msg.sender_id, origin_bridge: self.node_pubkey,
        origin_island: self.island, ts: now()
    })

on_mqtt_message(m):
    if m.origin_bridge == self.node_pubkey: return         # our own publication
    if cache.contains(m.id): return
    cache.insert(m.id)
    if not ratelimit.try_acquire(): log_drop(m); return
    text = truncate("[" + m.origin_island + "] " + m.sender + ": " + m.text, MAX_TEXT_LEN)
    rf_send_channel(channel, text)

canonical_id(channel, sender_id, text, ts):
    bucket = floor(ts / 30)
    pre = utf8(channel) + 0x1F + utf8(sender_id) + 0x1F + utf8(text) + 0x1F + utf8(str(bucket))
    return hex(sha1(pre)[0:8])
```

