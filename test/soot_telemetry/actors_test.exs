defmodule SootTelemetry.ActorsTest do
  use ExUnit.Case, async: true

  alias SootTelemetry.Actors
  alias SootTelemetry.Actors.System

  describe "system/1" do
    test "builds a System actor" do
      assert %System{part: :registry_sync, tenant_id: nil} = Actors.system(:registry_sync)
    end

    for part <- [:registry_sync, :ingest_session_writer] do
      test "accepts :#{part}" do
        part = unquote(part)
        assert %System{part: ^part} = Actors.system(part)
      end
    end
  end

  describe "system/2" do
    test "tenant_id binary" do
      assert %System{tenant_id: "t-1"} = Actors.system(:registry_sync, "t-1")
    end

    test "nil tenant" do
      assert %System{tenant_id: nil} = Actors.system(:registry_sync, nil)
    end

    test "keyword opts" do
      assert %System{tenant_id: "t-x"} =
               Actors.system(:ingest_session_writer, tenant_id: "t-x")
    end
  end

  describe "%System{}" do
    test "enforces :part" do
      assert_raise ArgumentError, fn -> struct!(System, []) end
    end
  end
end
