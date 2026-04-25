defmodule SootTelemetry.ClickHouse.DDLTest do
  use ExUnit.Case, async: true

  alias SootTelemetry.ClickHouse.DDL
  alias SootTelemetry.Test.Fixtures.{Power, Vibration}

  describe "create_table/2" do
    test "renders engine, ORDER BY, PARTITION BY, TTL" do
      sql = DDL.create_table(Vibration)

      assert sql =~ "CREATE TABLE IF NOT EXISTS telemetry_vibration"
      assert sql =~ "ENGINE = MergeTree"
      assert sql =~ "ORDER BY (tenant_id, device_id, ts)"
      assert sql =~ "PARTITION BY toYYYYMM(ts)"
      assert sql =~ "TTL ts + INTERVAL 12 MONTH"
    end

    test "type mapping covers Arrow logical types correctly" do
      sql = DDL.create_table(Vibration)

      assert sql =~ "ts DateTime64(6, 'UTC')"
      assert sql =~ "axis_x Nullable(Float32)"
      assert sql =~ "device_id Nullable(LowCardinality(String))"
      assert sql =~ "sequence Nullable(UInt64)"
    end

    test "required columns are not Nullable" do
      sql = DDL.create_table(Vibration)
      assert sql =~ "    ts DateTime64(6, 'UTC'),"
      refute sql =~ ~r/^    ts Nullable/m
    end

    test "no PARTITION BY when not configured" do
      sql = DDL.create_table(Power)
      refute sql =~ "PARTITION BY"
    end

    test "no TTL when retention is empty and clickhouse :ttl is unset" do
      sql = DDL.create_table(Power)
      refute sql =~ "TTL "
    end

    test "database option produces a fully-qualified table name" do
      sql = DDL.create_table(Vibration, database: "iot")
      assert sql =~ "CREATE TABLE IF NOT EXISTS iot.telemetry_vibration"
    end

    test "table option overrides the default name" do
      sql = DDL.create_table(Vibration, table: "vibrations_v2")
      assert sql =~ "CREATE TABLE IF NOT EXISTS vibrations_v2"
    end
  end

  describe "create_tables/2" do
    test "joins multiple statements with blank lines" do
      sql = DDL.create_tables([Vibration, Power])
      [first, second] = String.split(sql, "\n\n", trim: true)
      assert first =~ "telemetry_vibration"
      assert second =~ "telemetry_power"
    end
  end

  describe "alter_for_descriptor_change/3" do
    test "additive change emits ADD COLUMN" do
      from = %{
        "fields" => [
          %{"name" => "ts", "type" => "timestamp_us", "required" => true, "dictionary" => false}
        ]
      }

      to = %{
        "fields" => [
          %{"name" => "ts", "type" => "timestamp_us", "required" => true, "dictionary" => false},
          %{"name" => "extra", "type" => "float32", "required" => false, "dictionary" => false}
        ]
      }

      assert {:ok, [stmt]} = DDL.alter_for_descriptor_change("telemetry_x", from, to)
      assert stmt =~ "ALTER TABLE telemetry_x ADD COLUMN extra Nullable(Float32)"
    end

    test "removed column → :non_additive" do
      from = %{"fields" => [%{"name" => "a", "type" => "int32", "required" => true, "dictionary" => false}]}
      to = %{"fields" => []}
      assert {:error, :non_additive} = DDL.alter_for_descriptor_change("t", from, to)
    end

    test "changed type → :non_additive" do
      from = %{"fields" => [%{"name" => "x", "type" => "int32", "required" => true, "dictionary" => false}]}
      to = %{"fields" => [%{"name" => "x", "type" => "int64", "required" => true, "dictionary" => false}]}
      assert {:error, :non_additive} = DDL.alter_for_descriptor_change("t", from, to)
    end
  end
end
