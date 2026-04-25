defmodule SootTelemetry.Stream.Field do
  @moduledoc """
  One column in a telemetry stream.

  `type` is the Arrow logical type. `required: true` becomes a
  `NOT NULL` column in ClickHouse. `dictionary: true` hints
  `LowCardinality(<base type>)` and tells the wire encoder to dictionary-
  encode the column. `server_set: true` columns are rejected if the device
  attempts to fill them; the ingest plug projects them in-memory before
  insert. `monotonic: true` flags a column the sequence-replay protector
  treats as a per-(device, stream) high-water value (typically `:sequence`).
  """

  defstruct [
    :name,
    :type,
    required: false,
    dictionary: false,
    server_set: false,
    monotonic: false,
    __spark_metadata__: nil
  ]

  @types [
    :int8,
    :int16,
    :int32,
    :int64,
    :uint8,
    :uint16,
    :uint32,
    :uint64,
    :float32,
    :float64,
    :bool,
    :string,
    :binary,
    :timestamp_us,
    :timestamp_ms,
    :timestamp_s,
    :date32
  ]

  @type type :: unquote(Enum.reduce(@types, &{:|, [], [&1, &2]}))

  @type t :: %__MODULE__{
          name: atom(),
          type: type(),
          required: boolean(),
          dictionary: boolean(),
          server_set: boolean(),
          monotonic: boolean()
        }

  @doc "All valid Arrow logical types accepted by the DSL."
  @spec types() :: [atom()]
  def types, do: @types
end
