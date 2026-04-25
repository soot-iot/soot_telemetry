defmodule SootTelemetry.StreamDslTest do
  use ExUnit.Case, async: true

  alias SootTelemetry.Stream.Info
  alias SootTelemetry.Test.Fixtures.{Power, Vibration}

  describe "section options" do
    test "name and tenant_scope are reachable through Info" do
      assert Info.name(Vibration) == :vibration
      assert Info.per_tenant?(Vibration)
      assert Info.retention(Vibration) == [months: 12]
    end

    test "paused? defaults to false" do
      refute Info.paused?(Vibration)
    end
  end

  describe "fields/1" do
    test "preserves declaration order" do
      names = Vibration |> Info.fields() |> Enum.map(& &1.name)
      assert names == [:ts, :ingest_ts, :device_id, :tenant_id, :axis_x, :axis_y, :axis_z, :sequence]
    end

    test "carries flags accurately" do
      ingest_ts = Info.field(Vibration, :ingest_ts)
      assert ingest_ts.server_set
      refute ingest_ts.required

      device_id = Info.field(Vibration, :device_id)
      assert device_id.dictionary

      sequence = Info.field(Vibration, :sequence)
      assert sequence.monotonic
    end

    test "client_fields strips server_set columns" do
      names = Vibration |> Info.client_fields() |> Enum.map(& &1.name)
      refute :ingest_ts in names
      refute :tenant_id in names
      assert :ts in names
    end

    test "server_fields includes only the server-set columns" do
      names = Vibration |> Info.server_fields() |> Enum.map(& &1.name)
      assert names == [:ingest_ts, :tenant_id]
    end

    test "monotonic_field returns the unique flag-bearing field" do
      assert Info.monotonic_field(Vibration).name == :sequence
      assert Info.monotonic_field(Power).name == :sequence
    end
  end

  describe "clickhouse/1" do
    test "exposes engine, order_by, partition_by" do
      ch = Info.clickhouse(Vibration)
      assert ch.engine == "MergeTree"
      assert ch.order_by == [:tenant_id, :device_id, :ts]
      assert ch.partition_by == "toYYYYMM(ts)"
    end

    test "missing partition_by returns nil" do
      assert Info.clickhouse(Power).partition_by == nil
    end
  end

  describe "DSL parse-time validation" do
    test "rejects an unknown field type" do
      assert_raise Spark.Error.DslError, fn ->
        defmodule BadType do
          use SootTelemetry.Stream.Definition

          telemetry_stream do
            name :bad_type

            fields do
              field :x, :not_a_real_type
            end

            clickhouse do
              order_by [:x]
            end
          end
        end
      end
    end

    test "rejects missing stream name" do
      assert_raise Spark.Error.DslError, fn ->
        defmodule NoName do
          use SootTelemetry.Stream.Definition

          telemetry_stream do
            fields do
              field :ts, :timestamp_us
            end
          end
        end
      end
    end
  end
end
