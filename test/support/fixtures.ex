defmodule SootTelemetry.Test.Fixtures.Vibration do
  @moduledoc false
  use SootTelemetry.Stream.Definition

  telemetry_stream do
    name :vibration
    tenant_scope(:per_tenant)
    retention(months: 12)

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
      engine("MergeTree")
      order_by([:tenant_id, :device_id, :ts])
      partition_by("toYYYYMM(ts)")
    end
  end
end

defmodule SootTelemetry.Test.Fixtures.Power do
  @moduledoc false
  use SootTelemetry.Stream.Definition

  telemetry_stream do
    name :power
    tenant_scope(:per_tenant)

    fields do
      field :ts, :timestamp_us, required: true
      field :device_id, :string, dictionary: true
      field :tenant_id, :string, dictionary: true, server_set: true
      field :watts, :float32
      field :sequence, :uint64, monotonic: true
    end

    clickhouse do
      engine("MergeTree")
      order_by([:tenant_id, :device_id, :ts])
    end
  end
end
