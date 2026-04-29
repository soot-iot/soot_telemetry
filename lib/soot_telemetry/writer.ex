defmodule SootTelemetry.Writer do
  @moduledoc """
  Behavior for handing a validated batch downstream.

  The library's application-env default is `SootTelemetry.Writer.Noop`,
  which records no rows. That default exists *only* so soot_telemetry's
  own test suite can run without ClickHouse — every consumer project
  has `mix soot_telemetry.install` write
  `config :soot_telemetry, :writer, SootTelemetry.Writer.ClickHouse`
  into `config/config.exs`, which forwards Arrow batches to ClickHouse
  over the `ch` driver. Postgres + ClickHouse are mandatory for any
  soot deployment, including dev.

  The writer is invoked *after* the ingest plug has validated headers,
  fingerprint, sequence, rate limits, and authorization. It is not
  responsible for any of those concerns.
  """

  @type batch :: %{
          required(:body) => binary(),
          required(:stream) => atom(),
          required(:fingerprint) => String.t(),
          required(:sequence_start) => non_neg_integer(),
          required(:sequence_end) => non_neg_integer(),
          required(:device_id) => String.t() | nil,
          required(:tenant_id) => String.t() | nil,
          required(:received_at) => DateTime.t()
        }

  @callback write(batch()) :: :ok | {:error, term()}

  @doc "Configured writer module; defaults to `SootTelemetry.Writer.Noop`."
  @spec configured() :: module()
  def configured do
    Application.get_env(:soot_telemetry, :writer, SootTelemetry.Writer.Noop)
  end

  @doc "Write a batch through the configured writer."
  @spec write(batch()) :: :ok | {:error, term()}
  def write(batch), do: configured().write(batch)
end

defmodule SootTelemetry.Writer.Noop do
  @moduledoc "Default writer: validates the batch shape and discards."
  @behaviour SootTelemetry.Writer

  @impl true
  def write(%{body: _, stream: _, fingerprint: _} = _batch), do: :ok
end
