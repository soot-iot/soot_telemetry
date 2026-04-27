defmodule SootTelemetry.StreamRow do
  @moduledoc """
  Default `StreamRow` resource shipped with `soot_telemetry`.

  A registered telemetry stream. One row per unique stream `name`.
  `current_schema_id` points at the `SootTelemetry.Schema` row whose
  fingerprint the ingest endpoint expects right now.

  The Ash resource is named `StreamRow` to avoid colliding with the
  Spark DSL extension `SootTelemetry.Stream` that lives on user modules.

  The schema is provided by the `SootTelemetry.Resource.StreamRow`
  extension. This default uses `Ash.DataLayer.Ets`; production
  deployments override with their own resource module backed by
  `AshPostgres.DataLayer` and register it via
  `config :soot_telemetry, stream_row: MyApp.StreamRow`.
  """

  use Ash.Resource,
    otp_app: :soot_telemetry,
    domain: SootTelemetry.Domain,
    data_layer: Ash.DataLayer.Ets,
    extensions: [SootTelemetry.Resource.StreamRow]

  ets do
    private? false
  end
end
