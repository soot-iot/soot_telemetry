defmodule SootTelemetry.Resource.StreamRow do
  @moduledoc """
  `Ash.Resource` extension that injects the `SootTelemetry` stream-row
  schema into a consumer-owned resource module.

  Usage and override semantics mirror `SootCore.Resource.Tenant`. Register
  via `config :soot_telemetry, stream_row: MyApp.StreamRow`.
  """

  use Spark.Dsl.Extension,
    transformers: [SootTelemetry.Resource.StreamRow.Transformers.Inject]
end
