# Changelog

All notable changes to `soot_telemetry` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
the project adheres to semantic versioning.

## [Unreleased]

### Added
- Configurable `max_body_bytes` plug option on `SootTelemetry.Plug.Ingest`.
- `SootTelemetry.Plug.Ingest.TenantSan` — extracted SAN-tenant resolver
  with its own test surface.
- `SootTelemetry.IngestSession.Changes.ClampSequenceHighWater` — keeps
  the per-session high-water monotone across in-grace late batches.

### Changed
- `Schema` identity is now `(stream_name, fingerprint)`; the lookup
  action is `get_for_stream_fingerprint/2`.
- `Plug.Ingest` returns a structured `500 writer_error` instead of
  crashing when a writer returns `{:error, _}`.
- `tenant_id_from_actor/1` guards against `nil` SANs.
- `record_session/4` logs IngestSession write failures via
  `Logger.warning` instead of silently dropping them.

## [0.1.0] - 2026-04-26

### Added
- Initial Phase 4 release: telemetry stream DSL, schema fingerprinting,
  Ash resources for `Schema`, `StreamRow`, and `IngestSession`,
  `/ingest/:stream_name` Plug, ETS-backed token-bucket rate limiter,
  ClickHouse `CREATE TABLE`/`ALTER TABLE` generator, and the
  `mix soot_telemetry.gen_migrations` task.
