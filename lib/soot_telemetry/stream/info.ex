defmodule SootTelemetry.Stream.Info do
  @moduledoc """
  Introspection helpers for `telemetry_stream do … end`.

      SootTelemetry.Stream.Info.name(MyApp.Telemetry.Vibration)
      SootTelemetry.Stream.Info.fields(MyApp.Telemetry.Vibration)
      SootTelemetry.Stream.Info.clickhouse(MyApp.Telemetry.Vibration)

  `Spark.InfoGenerator` produces `telemetry_stream_<option>/1` accessors
  for the section options; `fields/1` and `clickhouse/1` are written by
  hand because Spark's generator only walks one level of nesting and we
  want a single named struct out of the `clickhouse` subsection.
  """

  use Spark.InfoGenerator,
    extension: SootTelemetry.Stream,
    sections: [:telemetry_stream]

  alias SootTelemetry.Stream.{ClickHouseConfig, Field}

  @doc "All `field` entities, in declaration order."
  @spec fields(module()) :: [Field.t()]
  def fields(module) do
    Spark.Dsl.Extension.get_entities(module, [:telemetry_stream, :fields])
  end

  @doc "Find a field by name; returns nil when absent."
  @spec field(module(), atom()) :: Field.t() | nil
  def field(module, name) when is_atom(name) do
    fields(module) |> Enum.find(&(&1.name == name))
  end

  @doc "Fields the device is allowed to populate (i.e. not server_set)."
  @spec client_fields(module()) :: [Field.t()]
  def client_fields(module), do: fields(module) |> Enum.reject(& &1.server_set)

  @doc "Fields the ingest endpoint will project in-memory before insert."
  @spec server_fields(module()) :: [Field.t()]
  def server_fields(module), do: fields(module) |> Enum.filter(& &1.server_set)

  @doc "The (zero or one) field marked `monotonic: true`."
  @spec monotonic_field(module()) :: Field.t() | nil
  def monotonic_field(module), do: fields(module) |> Enum.find(& &1.monotonic)

  @doc "Compile the `clickhouse` subsection into a struct."
  @spec clickhouse(module()) :: ClickHouseConfig.t()
  def clickhouse(module) do
    %ClickHouseConfig{
      engine: get_clickhouse(module, :engine),
      order_by: get_clickhouse(module, :order_by) || [],
      partition_by: get_clickhouse(module, :partition_by),
      ttl: get_clickhouse(module, :ttl),
      settings: get_clickhouse(module, :settings) || []
    }
  end

  @doc "The stream name from `telemetry_stream do name :vibration end`."
  @spec name(module()) :: atom()
  def name(module), do: telemetry_stream_name!(module)

  @doc "Whether the stream is scoped to a single tenant."
  @spec per_tenant?(module()) :: boolean()
  def per_tenant?(module), do: telemetry_stream_tenant_scope!(module) == :per_tenant

  @doc "Retention keyword list, e.g. `[months: 12]`."
  @spec retention(module()) :: keyword()
  def retention(module), do: telemetry_stream_retention!(module)

  @doc "Whether the stream rejects new batches."
  @spec paused?(module()) :: boolean()
  def paused?(module), do: telemetry_stream_paused!(module)

  defp get_clickhouse(module, key) do
    Spark.Dsl.Extension.get_opt(module, [:telemetry_stream, :clickhouse], key, nil)
  end
end
