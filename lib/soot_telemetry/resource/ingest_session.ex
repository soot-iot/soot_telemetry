defmodule SootTelemetry.Resource.IngestSession do
  @moduledoc """
  `Ash.Resource` extension that injects the `SootTelemetry` ingest-session
  schema into a consumer-owned resource module.

  Tracks the per-(device, stream) sequence high water for replay
  protection plus byte/batch counters for observability.

  Usage and override semantics mirror `SootCore.Resource.Tenant`. Register
  via `config :soot_telemetry, ingest_session: MyApp.IngestSession`.
  """

  use Spark.Dsl.Extension,
    transformers: [SootTelemetry.Resource.IngestSession.Transformers.Inject]
end
