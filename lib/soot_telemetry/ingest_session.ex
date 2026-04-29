defmodule SootTelemetry.IngestSession do
  @moduledoc """
  Default `IngestSession` resource shipped with `soot_telemetry`.

  An open ingest connection (or the most recent batch from a device, in
  the lean topology). Tracks the per-(device, stream) sequence high water
  for replay protection plus byte/batch counters for observability.

  Exists in the OLTP store alongside the rest of `soot_core` so operators
  can correlate it with device state without crossing into the OLAP
  system.

  The schema is provided by the `SootTelemetry.Resource.IngestSession`
  extension. This default uses `Ash.DataLayer.Ets`; production
  deployments override with their own resource module backed by
  `AshPostgres.DataLayer` and register it via
  `config :soot_telemetry, ingest_session: MyApp.IngestSession`.
  """

  use Ash.Resource,
    otp_app: :soot_telemetry,
    domain: SootTelemetry.Domain,
    data_layer: Ash.DataLayer.Ets,
    authorizers: [Ash.Policy.Authorizer],
    extensions: [SootTelemetry.Resource.IngestSession]

  ets do
    private? false
  end

  policies do
    policy always() do
      access_type :strict
      authorize_if actor_attribute_equals(:part, :ingest_session_writer)
    end
  end
end
