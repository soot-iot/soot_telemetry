defmodule SootTelemetry.RegistryTest do
  use ExUnit.Case, async: false

  alias SootTelemetry.{Registry, Schema, StreamRow}
  alias SootTelemetry.Test.Factories
  alias SootTelemetry.Test.Fixtures.{Power, Vibration}

  setup do
    Factories.reset!()
    :ok
  end

  test "register/1 creates schema and stream rows" do
    {:ok, %{schema: schema, stream: stream}} = Registry.register(Vibration)

    assert schema.stream_name == :vibration
    assert schema.version == 1
    assert is_binary(schema.fingerprint)
    assert schema.descriptor["name"] == "vibration" or schema.descriptor[:name] == :vibration

    assert stream.name == :vibration
    assert stream.module == Vibration
    assert stream.tenant_scope == :per_tenant
    assert stream.current_schema_id == schema.id
    assert stream.clickhouse_table == "telemetry_vibration"
  end

  test "register/1 is idempotent for an unchanged module" do
    {:ok, %{schema: s1, stream: st1}} = Registry.register(Vibration)
    {:ok, %{schema: s2, stream: st2}} = Registry.register(Vibration)

    assert s1.id == s2.id
    assert st1.id == st2.id
  end

  test "registering a different module produces a different schema row" do
    {:ok, %{schema: vs}} = Registry.register(Vibration)
    {:ok, %{schema: ps}} = Registry.register(Power)

    refute vs.id == ps.id
    refute vs.fingerprint == ps.fingerprint
  end

  test "register_all/1 returns one result per module" do
    {:ok, results} = Registry.register_all([Vibration, Power])
    assert length(results) == 2

    {:ok, schemas} = Ash.read(Schema)
    assert length(schemas) == 2

    {:ok, streams} = Ash.read(StreamRow)
    assert length(streams) == 2
  end

  test "register honours custom clickhouse_table override" do
    {:ok, %{stream: stream}} = Registry.register(Vibration, clickhouse_table: "vibrations_v2")
    assert stream.clickhouse_table == "vibrations_v2"
  end
end
