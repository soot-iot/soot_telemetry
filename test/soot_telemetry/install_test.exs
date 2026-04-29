defmodule Mix.Tasks.SootTelemetry.InstallTest do
  use ExUnit.Case, async: false

  import Igniter.Test

  # Igniter.compose_task evaluates the generated config.exs into the
  # test VM's Application env, which leaves keys set for the rest of
  # the suite. Snapshot the relevant `:soot_telemetry` keys before each
  # test and restore them on exit so unrelated tests (e.g. Plug.Ingest
  # ones that should run with the Writer.Noop default) aren't poisoned.
  @leaked_keys [:writer, :clickhouse_url]

  setup do
    snapshot =
      for key <- @leaked_keys,
          {:ok, value} <- [Application.fetch_env(:soot_telemetry, key)],
          do: {key, value}

    on_exit(fn ->
      for key <- @leaked_keys do
        Application.delete_env(:soot_telemetry, key)
      end

      for {key, value} <- snapshot do
        Application.put_env(:soot_telemetry, key, value)
      end
    end)

    :ok
  end

  defp project_with_router do
    test_project(
      files: %{
        "lib/test_web/router.ex" => """
        defmodule TestWeb.Router do
          use Phoenix.Router

          pipeline :device_mtls do
            plug AshPki.Plug.MTLS, require_known_certificate: true
          end

          scope "/" do
            pipe_through :device_mtls

            forward "/enroll", SootCore.Plug.Enroll
          end
        end
        """,
        "lib/test_web.ex" => """
        defmodule TestWeb do
          def router do
            quote do
              use Phoenix.Router
            end
          end
        end
        """
      }
    )
  end

  describe "info/2" do
    test "exposes the documented option schema" do
      info = Mix.Tasks.SootTelemetry.Install.info([], nil)
      assert info.group == :soot
      assert info.schema == [example: :boolean, yes: :boolean]
      assert info.aliases == [y: :yes, e: :example]
    end
  end

  describe "default streams (always generated)" do
    test "creates cpu / memory / disk stream modules" do
      result =
        project_with_router()
        |> Igniter.compose_task("soot_telemetry.install", [])

      assert_creates(result, "lib/test/telemetry/cpu.ex")
      assert_creates(result, "lib/test/telemetry/memory.ex")
      assert_creates(result, "lib/test/telemetry/disk.ex")
    end

    test "cpu stream uses SootTelemetry.Stream.Definition and declares load fields" do
      result =
        project_with_router()
        |> Igniter.compose_task("soot_telemetry.install", [])

      diff = diff(result, only: "lib/test/telemetry/cpu.ex")
      assert diff =~ "use SootTelemetry.Stream.Definition"
      assert diff =~ "name(:cpu)"
      assert diff =~ ":load_1m"
      assert diff =~ ":user_pct"
    end

    test "memory stream declares total/used/swap byte fields" do
      result =
        project_with_router()
        |> Igniter.compose_task("soot_telemetry.install", [])

      diff = diff(result, only: "lib/test/telemetry/memory.ex")
      assert diff =~ "name(:memory)"
      assert diff =~ ":total_bytes"
      assert diff =~ ":swap_used_bytes"
    end

    test "disk stream includes mount_point dictionary field" do
      result =
        project_with_router()
        |> Igniter.compose_task("soot_telemetry.install", [])

      diff = diff(result, only: "lib/test/telemetry/disk.ex")
      assert diff =~ "name(:disk)"
      assert diff =~ ":mount_point"
      assert diff =~ "dictionary: true"
      assert diff =~ ":inode_total"
    end
  end

  describe "--example outdoor_temperature stream" do
    test "creates outdoor_temperature.ex when --example is passed" do
      result =
        project_with_router()
        |> Igniter.compose_task("soot_telemetry.install", ["--example"])

      assert_creates(result, "lib/test/telemetry/outdoor_temperature.ex")

      diff = diff(result, only: "lib/test/telemetry/outdoor_temperature.ex")
      assert diff =~ "name(:outdoor_temperature)"
      assert diff =~ ":celsius"
      assert diff =~ ":humidity_pct"
      assert diff =~ ":sensor_id"
    end

    test "does not create outdoor_temperature.ex without --example" do
      result =
        project_with_router()
        |> Igniter.compose_task("soot_telemetry.install", [])

      refute_creates(result, "lib/test/telemetry/outdoor_temperature.ex")
    end
  end

  describe "domain registration" do
    test "registers SootTelemetry.Domain in operator's :ash_domains" do
      result =
        project_with_router()
        |> Igniter.compose_task("soot_telemetry.install", [])

      diff = diff(result, only: "config/config.exs")
      assert diff =~ "SootTelemetry.Domain"
      assert diff =~ "ash_domains:"
    end
  end

  describe "router mount" do
    test "adds /ingest forward to the :device_mtls scope" do
      result =
        project_with_router()
        |> Igniter.compose_task("soot_telemetry.install", [])

      diff = diff(result, only: "lib/test_web/router.ex")
      assert diff =~ "/ingest"
      assert diff =~ "SootTelemetry.Plug.Ingest"
    end

    test "warns when no router exists" do
      igniter =
        test_project(files: %{})
        |> Igniter.compose_task("soot_telemetry.install", [])

      assert Enum.any?(igniter.warnings, &(&1 =~ "No Phoenix router")) or
               Enum.any?(igniter.notices, &(&1 =~ "soot_telemetry installed"))
    end
  end

  describe "ClickHouse migrations directory" do
    test "creates priv/migrations/clickhouse/.gitkeep" do
      project_with_router()
      |> Igniter.compose_task("soot_telemetry.install", [])
      |> assert_creates("priv/migrations/clickhouse/.gitkeep")
    end
  end

  describe "writer wiring" do
    test "sets :writer to SootTelemetry.Writer.ClickHouse in config.exs" do
      result =
        project_with_router()
        |> Igniter.compose_task("soot_telemetry.install", [])

      diff = diff(result, only: "config/config.exs")
      assert diff =~ "writer:"
      assert diff =~ "SootTelemetry.Writer.ClickHouse"
    end

    test "running twice does not duplicate the writer config" do
      project_with_router()
      |> Igniter.compose_task("soot_telemetry.install", [])
      |> apply_igniter!()
      |> Igniter.compose_task("soot_telemetry.install", [])
      |> assert_unchanged("config/config.exs")
    end
  end

  describe "formatter import" do
    test "imports :soot_telemetry into .formatter.exs" do
      project_with_router()
      |> Igniter.compose_task("soot_telemetry.install", [])
      |> assert_has_patch(".formatter.exs", """
      + |  import_deps: [:soot_telemetry]
      """)
    end
  end

  describe "idempotency" do
    test "running twice is a no-op on .formatter.exs" do
      project_with_router()
      |> Igniter.compose_task("soot_telemetry.install", [])
      |> apply_igniter!()
      |> Igniter.compose_task("soot_telemetry.install", [])
      |> assert_unchanged(".formatter.exs")
    end

    test "running twice does not duplicate the cpu stream module" do
      project_with_router()
      |> Igniter.compose_task("soot_telemetry.install", [])
      |> apply_igniter!()
      |> Igniter.compose_task("soot_telemetry.install", [])
      |> assert_unchanged("lib/test/telemetry/cpu.ex")
    end
  end

  describe "next-steps notice" do
    test "emits a soot_telemetry installed notice" do
      igniter =
        project_with_router()
        |> Igniter.compose_task("soot_telemetry.install", [])

      assert Enum.any?(igniter.notices, &(&1 =~ "soot_telemetry installed"))
    end

    test "notice mentions the gen_migrations followup" do
      igniter =
        project_with_router()
        |> Igniter.compose_task("soot_telemetry.install", [])

      assert Enum.any?(
               igniter.notices,
               &(&1 =~ "soot_telemetry.gen_migrations")
             )
    end

    test "notice mentions the ClickHouse writer is wired" do
      igniter =
        project_with_router()
        |> Igniter.compose_task("soot_telemetry.install", [])

      assert Enum.any?(
               igniter.notices,
               &(&1 =~ "SootTelemetry.Writer.ClickHouse")
             )
    end

    test "notice mentions the example stream when --example" do
      igniter =
        project_with_router()
        |> Igniter.compose_task("soot_telemetry.install", ["--example"])

      assert Enum.any?(
               igniter.notices,
               &(&1 =~ "outdoor_temperature")
             )
    end
  end
end
