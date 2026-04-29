defmodule SootTelemetry.Writer.ClickHouse do
  @moduledoc """
  Pass-through ClickHouse writer for telemetry batches.

  The body bytes the ingest plug received from the device are forwarded
  verbatim to ClickHouse with `INSERT INTO <table> FORMAT <format>`.
  No decoding, no row projection, no batching beyond what the device
  already sent in the request body — one ingest request produces one
  INSERT.

  This is the v0.2 default writer described in `SPEC-2.md` §5.1, opt-in
  via `config :soot_telemetry, :writer, SootTelemetry.Writer.ClickHouse`.

  ## Configuration

      config :soot_telemetry, SootTelemetry.Writer.ClickHouse,
        scheme: "http",
        hostname: "localhost",
        port: 8123,
        database: "default",
        username: nil,
        password: nil,
        pool_size: 4,
        format: "ArrowStream",
        timeout: 30_000

  All keys except `:format` (and the internal pool name) are forwarded
  to `Ch.start_link/1`. `:format` is the ClickHouse input format used
  in the INSERT statement; it defaults to `"ArrowStream"` because the
  ingest endpoint expects Arrow IPC stream framing.

  ## Failure surface

  ClickHouse rejections (HTTP errors, schema mismatch, table missing,
  malformed body) are NOT silently swallowed. Every failed insert is
  logged at error level with stream / table / device / tenant /
  fingerprint / byte-size context, and the writer returns
  `{:error, {:clickhouse_insert_failed, reason}}` so the ingest plug
  surfaces a 500 to the device.
  """

  @behaviour SootTelemetry.Writer

  require Logger

  @default_format "ArrowStream"
  @default_pool_size 4

  @doc """
  Child spec for the underlying `Ch` connection pool.

  Add this to your supervision tree (or let `SootTelemetry.Application`
  start it for you when this module is the configured writer):

      children = [SootTelemetry.Writer.ClickHouse]
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    runtime_opts = Keyword.merge(runtime_config(), opts)
    {ch_opts, _writer_opts} = split_opts(runtime_opts)
    ch_opts = Keyword.put_new(ch_opts, :name, pool_name())
    ch_opts = Keyword.put_new(ch_opts, :pool_size, @default_pool_size)

    %{
      id: __MODULE__,
      start: {ch_module(), :start_link, [ch_opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5_000
    }
  end

  @impl true
  def write(%{body: body, stream: stream_name} = batch) do
    case resolve_table(stream_name) do
      {:ok, table} ->
        format = configured_format()
        statement = "INSERT INTO #{quote_table(table)} FORMAT #{format}"

        case ch_module().query(pool_name(), statement, body, encode: false, command: :insert) do
          {:ok, _result} ->
            :ok

          {:error, reason} ->
            log_failure(stream_name, table, format, body, batch, reason)
            {:error, {:clickhouse_insert_failed, reason}}
        end

      {:error, reason} ->
        log_failure(
          stream_name,
          default_table_or_unknown(stream_name),
          configured_format(),
          body,
          batch,
          reason
        )

        {:error, reason}
    end
  end

  @doc "The registered name of the connection pool."
  @spec pool_name() :: atom()
  def pool_name do
    Keyword.get(runtime_config(), :name, __MODULE__.Pool)
  end

  @doc "The configured ClickHouse input format (e.g. `\"ArrowStream\"`)."
  @spec configured_format() :: String.t()
  def configured_format do
    Keyword.get(runtime_config(), :format, @default_format)
  end

  # ─── helpers ───────────────────────────────────────────────────────────

  defp runtime_config do
    Application.get_env(:soot_telemetry, __MODULE__, [])
  end

  defp ch_module do
    Application.get_env(:soot_telemetry, :ch_module, Ch)
  end

  defp split_opts(opts) do
    {writer, ch} = Keyword.split(opts, [:format])
    {ch, writer}
  end

  defp resolve_table(nil), do: {:error, :missing_stream}

  defp resolve_table(stream_name) when is_atom(stream_name) do
    case SootTelemetry.stream_row().get_by_name(stream_name,
           actor: SootTelemetry.Actors.system(:registry_sync)
         ) do
      {:ok, %{clickhouse_table: table}} when is_binary(table) and table != "" ->
        {:ok, table}

      {:ok, %_{}} ->
        {:ok, default_table(stream_name)}

      _ ->
        {:error, {:unknown_stream, stream_name}}
    end
  end

  defp default_table_or_unknown(nil), do: "<unknown>"
  defp default_table_or_unknown(stream_name), do: default_table(stream_name)

  defp default_table(stream_name), do: "telemetry_" <> Atom.to_string(stream_name)

  defp quote_table(table) do
    if String.contains?(table, ".") do
      table
      |> String.split(".", parts: 2)
      |> Enum.map_join(".", &("\"" <> escape_ident(&1) <> "\""))
    else
      "\"" <> escape_ident(table) <> "\""
    end
  end

  defp escape_ident(ident), do: String.replace(ident, "\"", "\"\"")

  defp log_failure(stream_name, table, format, body, batch, reason) do
    Logger.error(fn ->
      "soot_telemetry: ClickHouse insert failed " <>
        "stream=#{inspect(stream_name)} table=#{inspect(table)} format=#{format} " <>
        "device=#{inspect(Map.get(batch, :device_id))} " <>
        "tenant=#{inspect(Map.get(batch, :tenant_id))} " <>
        "fingerprint=#{inspect(Map.get(batch, :fingerprint))} " <>
        "sequence=#{inspect(Map.get(batch, :sequence_start))}.." <>
        "#{inspect(Map.get(batch, :sequence_end))} " <>
        "bytes=#{byte_size(body)} reason=" <> format_reason(reason)
    end)
  end

  defp format_reason(%{__exception__: true} = e), do: Exception.message(e)
  defp format_reason(other), do: inspect(other)
end
