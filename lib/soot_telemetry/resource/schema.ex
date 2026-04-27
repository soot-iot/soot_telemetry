defmodule SootTelemetry.Resource.Schema do
  @moduledoc """
  `Ash.Resource` extension that injects the `SootTelemetry` schema
  resource (immutable, versioned snapshots of a stream's Arrow schema)
  into a consumer-owned resource module.

  Usage and override semantics mirror `SootCore.Resource.Tenant`. Register
  via `config :soot_telemetry, schema: MyApp.TelemetrySchema`.
  """

  use Spark.Dsl.Extension,
    transformers: [SootTelemetry.Resource.Schema.Transformers.Inject]
end
