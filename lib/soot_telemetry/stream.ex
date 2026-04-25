defmodule SootTelemetry.Stream do
  @moduledoc """
  Spark DSL extension declaring a telemetry stream on a host module.

  ## Surface

      defmodule MyApp.Telemetry.Vibration do
        use SootTelemetry.Stream

        telemetry_stream do
          name :vibration
          tenant_scope :per_tenant
          retention months: 12

          fields do
            field :ts, :timestamp_us, required: true
            field :ingest_ts, :timestamp_us, server_set: true
            field :device_id, :string, dictionary: true
            field :tenant_id, :string, dictionary: true, server_set: true
            field :axis_x, :float32
            field :axis_y, :float32
            field :axis_z, :float32
            field :sequence, :uint64, monotonic: true
          end

          clickhouse do
            engine "MergeTree"
            order_by [:tenant_id, :device_id, :ts]
            partition_by "toYYYYMM(ts)"
          end
        end
      end

  Use `SootTelemetry.Stream.Info` to introspect declarations and
  `SootTelemetry.Schema.Fingerprint.compute/1` to derive the canonical
  fingerprint that backs a `SootTelemetry.Schema` row.
  """

  @field %Spark.Dsl.Entity{
    name: :field,
    target: SootTelemetry.Stream.Field,
    args: [:name, :type],
    schema: [
      name: [type: :atom, required: true],
      type: [type: {:one_of, SootTelemetry.Stream.Field.types()}, required: true],
      required: [type: :boolean, default: false],
      dictionary: [
        type: :boolean,
        default: false,
        doc: "Hints `LowCardinality` in ClickHouse and dictionary encoding on the wire."
      ],
      server_set: [
        type: :boolean,
        default: false,
        doc: "Filled by the ingest endpoint; rejected if the device sends it."
      ],
      monotonic: [
        type: :boolean,
        default: false,
        doc: "Per-(device, stream) high-water value used for replay protection."
      ]
    ]
  }

  @fields %Spark.Dsl.Section{
    name: :fields,
    describe: "Schema fields, in declaration order.",
    entities: [@field]
  }

  @clickhouse %Spark.Dsl.Section{
    name: :clickhouse,
    describe: "ClickHouse table-engine configuration.",
    schema: [
      engine: [type: :string, default: "MergeTree"],
      order_by: [type: {:list, :atom}, default: []],
      partition_by: [type: :string],
      ttl: [type: :string, doc: "Optional TTL clause; e.g. `\"ts + INTERVAL 12 MONTH\"`."],
      settings: [type: :keyword_list, default: []]
    ]
  }

  @telemetry_stream %Spark.Dsl.Section{
    name: :telemetry_stream,
    describe: "Top-level telemetry stream declaration.",
    sections: [@fields, @clickhouse],
    schema: [
      name: [type: :atom, required: true, doc: "Stream identifier; appears in URLs and topics."],
      tenant_scope: [
        type: {:one_of, [:per_tenant, :shared]},
        default: :per_tenant,
        doc: "Whether the stream is scoped to a single tenant or shared across tenants."
      ],
      retention: [
        type: :keyword_list,
        default: [],
        doc:
          "Retention hint, e.g. `[months: 12]` or `[days: 90]`. Compiled to a TTL clause unless `clickhouse :ttl` is set explicitly."
      ],
      paused: [
        type: :boolean,
        default: false,
        doc: "When true, the ingest endpoint refuses new batches."
      ]
    ]
  }

  use Spark.Dsl.Extension, sections: [@telemetry_stream]
end
