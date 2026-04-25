defmodule SootTelemetry do
  @moduledoc """
  Telemetry streams: a Spark DSL for declaring Arrow-shaped streams,
  Ash resources for tracking schema versions and live ingest sessions,
  a `Plug` that terminates the `/ingest/:stream_name` endpoint, and a
  ClickHouse DDL generator.

  See `SootTelemetry.Stream` for the DSL surface and
  `SootTelemetry.Plug.Ingest` for the endpoint.
  """
end
