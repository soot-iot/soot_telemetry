defmodule SootTelemetry.Stream.ClickHouseConfig do
  @moduledoc """
  Per-stream ClickHouse table-engine configuration.

  `engine` is the table engine string (`"MergeTree"`, `"ReplicatedMergeTree(...)"`,
  etc). `order_by` is the list of columns that form the sorting key.
  `partition_by` is a string expression (e.g. `"toYYYYMM(ts)"`) — passed
  through verbatim so operators can use ClickHouse functions.
  """

  defstruct [
    :engine,
    :order_by,
    :partition_by,
    :ttl,
    settings: [],
    __spark_metadata__: nil
  ]

  @type t :: %__MODULE__{
          engine: String.t() | nil,
          order_by: [atom()] | nil,
          partition_by: String.t() | nil,
          ttl: String.t() | nil,
          settings: keyword()
        }
end
