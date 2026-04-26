defmodule Mix.Tasks.SootTelemetry.InstallTest do
  use ExUnit.Case, async: false

  import Igniter.Test

  describe "info/2" do
    test "exposes the documented option schema" do
      info = Mix.Tasks.SootTelemetry.Install.info([], nil)
      assert info.group == :soot
      assert info.schema == [example: :boolean, yes: :boolean]
      assert info.aliases == [y: :yes, e: :example]
    end
  end

  describe "generated modules" do
    test "creates the Telemetry domain module" do
      test_project(files: %{})
      |> Igniter.compose_task("soot_telemetry.install", [])
      |> assert_creates("lib/test/telemetry.ex")
    end

    test "creates the Schema resource stub" do
      test_project(files: %{})
      |> Igniter.compose_task("soot_telemetry.install", [])
      |> assert_creates("lib/test/telemetry/schema.ex")
    end

    test "creates the Stream resource stub" do
      test_project(files: %{})
      |> Igniter.compose_task("soot_telemetry.install", [])
      |> assert_creates("lib/test/telemetry/stream.ex")
    end

    test "creates the IngestSession resource stub" do
      test_project(files: %{})
      |> Igniter.compose_task("soot_telemetry.install", [])
      |> assert_creates("lib/test/telemetry/ingest_session.ex")
    end

    test "domain references all three resources" do
      result =
        test_project(files: %{})
        |> Igniter.compose_task("soot_telemetry.install", [])

      diff = diff(result, only: "lib/test/telemetry.ex")

      assert diff =~ "Test.Telemetry.Schema"
      assert diff =~ "Test.Telemetry.Stream"
      assert diff =~ "Test.Telemetry.IngestSession"
    end

    test "resource stubs declare the Telemetry domain" do
      result =
        test_project(files: %{})
        |> Igniter.compose_task("soot_telemetry.install", [])

      schema_diff = diff(result, only: "lib/test/telemetry/schema.ex")
      stream_diff = diff(result, only: "lib/test/telemetry/stream.ex")
      session_diff = diff(result, only: "lib/test/telemetry/ingest_session.ex")

      assert schema_diff =~ "use Ash.Resource"
      assert schema_diff =~ "domain: Test.Telemetry"
      assert stream_diff =~ "use Ash.Resource"
      assert stream_diff =~ "domain: Test.Telemetry"
      assert session_diff =~ "use Ash.Resource"
      assert session_diff =~ "domain: Test.Telemetry"
    end
  end

  describe "ClickHouse migrations directory" do
    test "creates priv/migrations/clickhouse/.gitkeep" do
      test_project(files: %{})
      |> Igniter.compose_task("soot_telemetry.install", [])
      |> assert_creates("priv/migrations/clickhouse/.gitkeep")
    end
  end

  describe "formatter import" do
    test "imports :soot_telemetry into .formatter.exs" do
      test_project(files: %{})
      |> Igniter.compose_task("soot_telemetry.install", [])
      |> assert_has_patch(".formatter.exs", """
      + |  import_deps: [:soot_telemetry]
      """)
    end
  end

  describe "idempotency" do
    test "running twice is a no-op on .formatter.exs" do
      test_project(files: %{})
      |> Igniter.compose_task("soot_telemetry.install", [])
      |> apply_igniter!()
      |> Igniter.compose_task("soot_telemetry.install", [])
      |> assert_unchanged(".formatter.exs")
    end
  end

  describe "next-steps notice" do
    test "emits a soot_telemetry installed notice" do
      igniter =
        test_project(files: %{})
        |> Igniter.compose_task("soot_telemetry.install", [])

      assert Enum.any?(igniter.notices, &(&1 =~ "soot_telemetry installed"))
    end

    test "notice mentions the gen_migrations followup" do
      igniter =
        test_project(files: %{})
        |> Igniter.compose_task("soot_telemetry.install", [])

      assert Enum.any?(
               igniter.notices,
               &(&1 =~ "soot_telemetry.gen_migrations")
             )
    end
  end
end
