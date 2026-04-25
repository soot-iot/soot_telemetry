defmodule SootTelemetry.Registry do
  @moduledoc """
  Walks `SootTelemetry.Stream`-using modules and upserts the matching
  `SootTelemetry.Schema` and `SootTelemetry.StreamRow` rows.

  Call `register/1` per module at boot, or `register_all/1` with a list.
  Idempotent: a module whose declaration matches an existing schema
  fingerprint reuses that schema row; a new fingerprint creates a new
  schema row, advances the version counter, and updates the stream's
  `current_schema_id`.
  """

  alias SootTelemetry.Schema
  alias SootTelemetry.Schema.Fingerprint
  alias SootTelemetry.Stream.Info
  alias SootTelemetry.StreamRow

  @doc """
  Register or update a single stream module.

  Returns `{:ok, %{schema: schema, stream: stream}}`.
  """
  @spec register(module(), keyword()) ::
          {:ok, %{schema: Schema.t(), stream: StreamRow.t()}} | {:error, term()}
  def register(module, opts \\ []) when is_atom(module) do
    name = Info.name(module)
    fingerprint = Fingerprint.compute(module)
    descriptor = Fingerprint.descriptor(module)

    with {:ok, schema} <- ensure_schema(name, fingerprint, descriptor),
         {:ok, stream} <- upsert_stream(module, name, schema, opts) do
      {:ok, %{schema: schema, stream: stream}}
    end
  end

  @doc "Register every module in `modules`. Returns `{:ok, [%{...}, ...]}` or the first error."
  @spec register_all([module()], keyword()) ::
          {:ok, [%{schema: Schema.t(), stream: StreamRow.t()}]} | {:error, term()}
  def register_all(modules, opts \\ []) do
    Enum.reduce_while(modules, {:ok, []}, fn module, {:ok, acc} ->
      case register(module, opts) do
        {:ok, result} -> {:cont, {:ok, [result | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      err -> err
    end
  end

  defp ensure_schema(name, fingerprint, descriptor) do
    case Schema.get_for_stream_fingerprint(name, fingerprint, authorize?: false) do
      {:ok, %Schema{} = schema} ->
        {:ok, schema}

      {:error, _} ->
        version = next_version(name)
        Schema.create(name, version, fingerprint, descriptor, authorize?: false)
    end
  end

  defp next_version(name) do
    case Schema.for_stream(name, authorize?: false) do
      {:ok, []} -> 1
      {:ok, schemas} -> (Enum.map(schemas, & &1.version) |> Enum.max()) + 1
      _ -> 1
    end
  end

  defp upsert_stream(module, name, %Schema{} = schema, opts) do
    case StreamRow.get_by_name(name, authorize?: false) do
      {:ok, %StreamRow{} = stream} ->
        if stream.current_schema_id == schema.id do
          {:ok, stream}
        else
          Ash.update(stream, %{current_schema_id: schema.id},
            action: :update,
            authorize?: false
          )
        end

      {:error, _} ->
        StreamRow.create(
          name,
          module,
          tenant_scope_for(module),
          schema.id,
          %{
            clickhouse_table: Keyword.get(opts, :clickhouse_table, default_table_name(name)),
            retention: retention_map(module),
            partitioning: partition_for(module)
          },
          authorize?: false
        )
    end
  end

  defp tenant_scope_for(module) do
    if Info.per_tenant?(module), do: :per_tenant, else: :shared
  end

  defp default_table_name(name), do: "telemetry_" <> Atom.to_string(name)

  defp retention_map(module) do
    Info.retention(module) |> Map.new()
  end

  defp partition_for(module) do
    Info.clickhouse(module).partition_by
  end
end
