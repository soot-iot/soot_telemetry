defmodule SootTelemetry do
  @moduledoc """
  Telemetry streams: a Spark DSL for declaring Arrow-shaped streams,
  Ash resources for tracking schema versions and live ingest sessions,
  a `Plug` that terminates the `/ingest/:stream_name` endpoint, and a
  ClickHouse DDL generator.

  See `SootTelemetry.Stream` for the DSL surface and
  `SootTelemetry.Plug.Ingest` for the endpoint.

  ## Resource overrides

  Each resource ships as an `Ash.Resource` extension under
  `SootTelemetry.Resource.*` plus a thin `Ash.DataLayer.Ets` default
  under `SootTelemetry.*`. Production deployments declare their own
  resource modules backed by `AshPostgres.DataLayer` and register them:

      config :soot_telemetry,
        stream_row: MyApp.StreamRow,
        schema: MyApp.TelemetrySchema,
        ingest_session: MyApp.IngestSession

  Internal callers resolve the active module through the helpers below.
  """

  @doc "Configured `StreamRow` resource module."
  @spec stream_row() :: module()
  def stream_row, do: Application.get_env(:soot_telemetry, :stream_row, SootTelemetry.StreamRow)

  @doc "Configured `Schema` resource module."
  @spec schema() :: module()
  def schema, do: Application.get_env(:soot_telemetry, :schema, SootTelemetry.Schema)

  @doc "Configured `IngestSession` resource module."
  @spec ingest_session() :: module()
  def ingest_session,
    do: Application.get_env(:soot_telemetry, :ingest_session, SootTelemetry.IngestSession)
end
