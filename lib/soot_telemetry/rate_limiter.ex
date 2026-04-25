defmodule SootTelemetry.RateLimiter do
  @moduledoc """
  Token-bucket rate limiter, ETS-backed.

  Each bucket is keyed by an opaque term and configured with a `capacity`
  (max burst) and a `refill` rate (tokens per second). Tokens accumulate
  continuously over time up to `capacity`. `take/3` deducts one token if
  any are available; otherwise it returns the milliseconds the caller
  should wait.

  The ingest plug calls this twice per request: once with
  `{:device_stream, device_id, stream_id}` and once with
  `{:tenant_stream, tenant_id, stream_id}`. Either rejection short-circuits
  the request with `429`.

  Bucket configuration is sourced from `Application.get_env/2` with a
  per-key override path; defaults are conservative.

      config :soot_telemetry, :rate_limits,
        device_stream: [capacity: 60, refill_per_second: 10],
        tenant_stream: [capacity: 6_000, refill_per_second: 1_000]
  """

  use GenServer

  @table __MODULE__
  @default_device_limit %{capacity: 60.0, refill_per_second: 10.0}
  @default_tenant_limit %{capacity: 6_000.0, refill_per_second: 1_000.0}

  # ─── Client API ──────────────────────────────────────────────────────

  @doc "Start the limiter. Owns an `:ets` table for bucket state."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc """
  Take one token from the bucket identified by `key`, charging the
  configured cost (default 1).

  Returns:
    * `{:ok, %{remaining: float, retry_after_ms: 0}}`
    * `{:rate_limited, %{retry_after_ms: pos_integer()}}`

  `key` is an opaque term, but the convention used by the ingest plug is
  `{:device_stream, device_id, stream_id}` /
  `{:tenant_stream, tenant_id, stream_id}` so the limit shape can be
  configured per kind.
  """
  @spec take(term(), pos_integer(), keyword()) ::
          {:ok, map()} | {:rate_limited, map()}
  def take(key, cost \\ 1, opts \\ []) do
    config = config_for(key, opts)
    now_ms = System.monotonic_time(:millisecond)

    {prev_tokens, prev_ts} =
      case :ets.lookup(@table, key) do
        [{^key, t, ts}] -> {t, ts}
        [] -> {config.capacity, now_ms}
      end

    elapsed_s = (now_ms - prev_ts) / 1_000.0
    refilled = min(config.capacity, prev_tokens + elapsed_s * config.refill_per_second)

    if refilled >= cost do
      remaining = refilled - cost
      :ets.insert(@table, {key, remaining, now_ms})
      {:ok, %{remaining: remaining, retry_after_ms: 0}}
    else
      missing = cost - refilled

      retry_after_ms =
        if config.refill_per_second <= 0 do
          :infinity
        else
          max(1, ceil(missing / config.refill_per_second * 1_000))
        end

      :ets.insert(@table, {key, refilled, now_ms})
      {:rate_limited, %{retry_after_ms: retry_after_ms, remaining: refilled}}
    end
  end

  @doc "Reset (or pre-fill) a bucket. Mostly useful in tests."
  @spec reset(term()) :: :ok
  def reset(key) do
    :ets.delete(@table, key)
    :ok
  end

  @doc "Drop every bucket. Tests only."
  @spec reset_all() :: :ok
  def reset_all do
    :ets.delete_all_objects(@table)
    :ok
  end

  # ─── GenServer ───────────────────────────────────────────────────────

  @impl true
  def init(_opts) do
    table =
      :ets.new(@table, [
        :named_table,
        :public,
        :set,
        read_concurrency: true,
        write_concurrency: true
      ])

    {:ok, %{table: table}}
  end

  # ─── Internals ───────────────────────────────────────────────────────

  defp config_for({:device_stream, _, _}, opts) do
    user = Keyword.get(opts, :device_stream)

    user || env_config(:device_stream) || @default_device_limit
  end

  defp config_for({:tenant_stream, _, _}, opts) do
    user = Keyword.get(opts, :tenant_stream)
    user || env_config(:tenant_stream) || @default_tenant_limit
  end

  defp config_for(_other, opts) do
    Keyword.get(opts, :default) || env_config(:default) || @default_device_limit
  end

  defp env_config(kind) do
    case Application.get_env(:soot_telemetry, :rate_limits) do
      nil ->
        nil

      kw when is_list(kw) ->
        case Keyword.get(kw, kind) do
          nil ->
            nil

          settings ->
            %{
              capacity: settings |> Keyword.fetch!(:capacity) |> to_float(),
              refill_per_second: settings |> Keyword.fetch!(:refill_per_second) |> to_float()
            }
        end
    end
  end

  defp to_float(n) when is_integer(n), do: n / 1
  defp to_float(n) when is_float(n), do: n
end
