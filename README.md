# `soot_telemetry`

Telemetry stream definitions, an HTTP/2 ingest endpoint, schema
fingerprint + sequence-replay protection, and a ClickHouse DDL
generator.

Depends on [`ash_pki`](../ash_pki) (mTLS plug + actor) and
[`soot_core`](../soot_core) (tenant + device resources). The actual
ClickHouse client is **not** a dependency: writes go through a
`SootTelemetry.Writer` behavior whose default implementation discards;
production deployments swap in a writer over the `:ch` driver.

## DSL

A telemetry stream is declared on a plain Spark module:

```elixir
defmodule MyApp.Telemetry.Vibration do
  use SootTelemetry.Stream.Definition

  telemetry_stream do
    name :vibration
    tenant_scope :per_tenant
    retention months: 12

    fields do
      field :ts, :timestamp_us, required: true
      field :ingest_ts, :timestamp_us, server_set: true
      field :device_id, :string, dictionary: true
      field :tenant_id, :string, dictionary: true, server_set: true
      field :axis_x, :float32
      field :axis_y, :float32
      field :axis_z, :float32
      field :sequence, :uint64, monotonic: true
    end

    clickhouse do
      engine "MergeTree"
      order_by [:tenant_id, :device_id, :ts]
      partition_by "toYYYYMM(ts)"
    end
  end
end
```

`SootTelemetry.Schema.Fingerprint.compute/1` produces a stable
hex-SHA-256 fingerprint of the schema; key ordering inside the
descriptor doesn't change the hash. Two streams with the same fields,
flags, and order produce the same fingerprint regardless of module name.

## Resources

* `SootTelemetry.Schema` — immutable, versioned schema rows. One row per
  unique fingerprint per stream.
* `SootTelemetry.StreamRow` — registered streams. Points at
  `current_schema_id`. Lifecycle: `:active` ⇄ `:paused`, terminal
  `:retired`.
* `SootTelemetry.IngestSession` — per-(device, stream) telemetry
  high-water + counters. Used for replay protection and observability.

## Registry

`SootTelemetry.Registry.register/1` upserts the Schema and StreamRow
rows for a module. Re-running with an unchanged module is idempotent;
a fingerprint change creates a new schema row, advances the version,
and updates the stream's `current_schema_id`.

```elixir
SootTelemetry.Registry.register_all([
  MyApp.Telemetry.Vibration,
  MyApp.Telemetry.Power
])
```

## Ingest endpoint

`SootTelemetry.Plug.Ingest` handles `POST /ingest/:stream_name`. Mount
behind `AshPki.Plug.MTLS`:

```elixir
forward "/ingest/:stream_name", to: Plug.Builder.compile([
  {AshPki.Plug.MTLS, [require_known_certificate: true]},
  SootTelemetry.Plug.Ingest
])
```

Required headers:

| header                  | meaning                                                         |
|-------------------------|-----------------------------------------------------------------|
| `x-stream`              | must match the URL path                                         |
| `x-schema-fingerprint`  | must match the active schema's fingerprint (mismatch → 409 + hint URL) |
| `x-sequence-start`      | first sequence number in the batch                              |
| `x-sequence-end`        | last sequence number in the batch                               |

Response codes:

| status | when                                                      |
|--------|-----------------------------------------------------------|
| 204    | accepted                                                  |
| 400    | malformed/missing headers, invalid body, sequence inversion |
| 401    | no mTLS actor                                             |
| 404    | unknown stream                                            |
| 405    | non-POST                                                  |
| 409    | fingerprint mismatch *or* sequence regression past the grace window |
| 413    | body exceeds the per-batch limit                          |
| 423    | stream is paused or retired                               |
| 429    | per-device or per-tenant rate-limit exhausted (with `retry-after`) |
| 500    | writer error / no active schema                           |

## Rate limiting

Token-bucket, ETS-backed. Two bucket kinds:

* `{:device_stream, device_id, stream_id}` — per-device cap.
* `{:tenant_stream, tenant_id, stream_id}` — per-tenant aggregate cap.

Both are checked per batch. Either rejection short-circuits the
request. Configure in `config/runtime.exs`:

```elixir
config :soot_telemetry, :rate_limits,
  device_stream: [capacity: 60, refill_per_second: 10],
  tenant_stream: [capacity: 6_000, refill_per_second: 1_000]
```

## ClickHouse DDL

`SootTelemetry.ClickHouse.DDL.create_table/2` renders a
`CREATE TABLE IF NOT EXISTS` statement matching the DSL declaration.
Type mapping covers every Arrow logical type the DSL accepts (see the
table in the moduledoc); `dictionary: true` columns become
`LowCardinality(<type>)`, `required: false` columns are wrapped in
`Nullable(_)`, and `retention` / `clickhouse :ttl` are surfaced as the
table TTL.

`alter_for_descriptor_change/3` diffs two stored schema descriptors and
returns the additive `ALTER TABLE ADD COLUMN` statements; non-additive
changes (removed column, type change) return `{:error, :non_additive}`
so the operator is forced to handle them explicitly.

## Mix tasks

```sh
mix soot_telemetry.gen_migrations \
      --out priv/migrations/V0001__telemetry_streams.sql \
      --stream MyApp.Telemetry.Vibration \
      --stream MyApp.Telemetry.Power \
      [--database iot]
```

## Out of scope (v0.1)

* The `:ch` ClickHouse driver dependency. Production writers plug in via
  the `SootTelemetry.Writer` behavior; the `Noop` default is what the
  in-repo tests run against.
* Real Arrow IPC decoding. The plug treats the request body as opaque
  bytes; framing/columnar validation is the consumer writer's
  responsibility for now.
* Automatic backfill on schema change.
* Multi-region replication coordination.

## Tests

```sh
mix test
```

The suite covers: DSL parsing + parse-time rejections (unknown type,
missing name), Info accessors and effective lookups, fingerprint
determinism + key-order independence, the registry's idempotence and
multi-module registration, the rate limiter (exhaustion, refill,
independent buckets, `:infinity` for zero-refill, app-env config),
the ingest plug across happy path + every named rejection branch
(non-POST, no actor, unknown stream, missing/invalid headers,
fingerprint mismatch with hint URL, sequence regression past grace
window, sequence inversion, rate-limited 429 with `retry-after`,
paused stream 423), DDL output for every type/flag combination plus
ALTER diffing, and the migration-generation mix task.
