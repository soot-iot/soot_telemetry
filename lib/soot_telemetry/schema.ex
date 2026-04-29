defmodule SootTelemetry.Schema do
  @moduledoc """
  Default `Schema` resource shipped with `soot_telemetry`.

  An immutable, versioned snapshot of a stream's Arrow schema.

  One row per `(stream_name, fingerprint)` pair. The current row for a
  given stream is referenced by
  `SootTelemetry.StreamRow.current_schema_id`. Field values are never
  rewritten — new schemas land as new rows. The only mutations are
  status transitions: `:active → :deprecated → :retired`.

  The schema is provided by the `SootTelemetry.Resource.Schema` extension.
  This default uses `Ash.DataLayer.Ets`; production deployments override
  with their own resource module backed by `AshPostgres.DataLayer` and
  register it via `config :soot_telemetry, schema: MyApp.TelemetrySchema`.
  """

  use Ash.Resource,
    otp_app: :soot_telemetry,
    domain: SootTelemetry.Domain,
    data_layer: Ash.DataLayer.Ets,
    authorizers: [Ash.Policy.Authorizer],
    extensions: [SootTelemetry.Resource.Schema]

  ets do
    private? false
  end

  policies do
    policy always() do
      access_type :strict
      authorize_if actor_attribute_equals(:part, :registry_sync)
    end
  end
end
