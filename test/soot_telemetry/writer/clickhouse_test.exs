defmodule SootTelemetry.Writer.ClickHouseTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias SootTelemetry.Registry
  alias SootTelemetry.Test.Factories
  alias SootTelemetry.Test.Fixtures.Vibration
  alias SootTelemetry.Writer.ClickHouse

  defmodule StubCh do
    @moduledoc false

    def start_link(opts) do
      Agent.start_link(fn -> %{opts: opts, calls: []} end, name: server(opts))
    end

    def child_spec(opts) do
      %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
    end

    def query(server, statement, params, opts) do
      response = Process.get(:stub_ch_response, {:ok, %{num_rows: 1}})

      Agent.update(server, fn state ->
        Map.update(state, :calls, [], fn calls ->
          [%{statement: IO.iodata_to_binary(statement), params: params, opts: opts} | calls]
        end)
      end)

      response
    end

    def calls(server), do: Agent.get(server, & &1.calls) |> Enum.reverse()
    def opts(server), do: Agent.get(server, & &1.opts)

    defp server(opts), do: Keyword.fetch!(opts, :name)
  end

  setup do
    Factories.reset!()

    prev_writer_env = Application.get_env(:soot_telemetry, ClickHouse)
    prev_ch_module = Application.get_env(:soot_telemetry, :ch_module)

    Application.put_env(:soot_telemetry, ClickHouse,
      name: :stub_ch_pool,
      format: "ArrowStream"
    )

    Application.put_env(:soot_telemetry, :ch_module, StubCh)

    on_exit(fn ->
      restore(:soot_telemetry, ClickHouse, prev_writer_env)
      restore(:soot_telemetry, :ch_module, prev_ch_module)
    end)

    :ok
  end

  defp restore(app, key, nil), do: Application.delete_env(app, key)
  defp restore(app, key, value), do: Application.put_env(app, key, value)

  defp start_stub! do
    pid = start_supervised!(ClickHouse)
    pid
  end

  defp batch(overrides \\ []) do
    base = %{
      body: <<1, 2, 3, 4>>,
      stream: :vibration,
      fingerprint: "abc123",
      sequence_start: 0,
      sequence_end: 9,
      device_id: "device-fp",
      tenant_id: "acme",
      received_at: DateTime.utc_now()
    }

    Map.merge(base, Map.new(overrides))
  end

  describe "child_spec/1 + start_link" do
    test "starts the underlying Ch pool with merged options" do
      pid = start_stub!()
      assert is_pid(pid)
      opts = StubCh.opts(:stub_ch_pool)
      assert opts[:name] == :stub_ch_pool
      assert opts[:pool_size] == 4
    end

    test "honours per-call overrides on child_spec opts" do
      spec = ClickHouse.child_spec(name: :overridden_pool, pool_size: 2)
      pid = start_supervised!(%{spec | id: :overridden})
      assert is_pid(pid)
      opts = StubCh.opts(:overridden_pool)
      assert opts[:pool_size] == 2
    end
  end

  describe "child_spec/1 — :clickhouse_url" do
    setup do
      prev_url = Application.get_env(:soot_telemetry, :clickhouse_url)
      on_exit(fn -> restore(:soot_telemetry, :clickhouse_url, prev_url) end)
      :ok
    end

    test "parses URL into scheme/hostname/port forwarded to Ch" do
      Application.put_env(:soot_telemetry, :clickhouse_url, "https://ch.internal:9000")
      spec = ClickHouse.child_spec(name: :url_pool)
      pid = start_supervised!(%{spec | id: :url_pool_id})
      assert is_pid(pid)

      opts = StubCh.opts(:url_pool)
      assert opts[:scheme] == "https"
      assert opts[:hostname] == "ch.internal"
      assert opts[:port] == 9000
    end

    test "absent URL leaves Ch opts to driver defaults" do
      Application.delete_env(:soot_telemetry, :clickhouse_url)
      spec = ClickHouse.child_spec(name: :no_url_pool)
      pid = start_supervised!(%{spec | id: :no_url_pool_id})
      assert is_pid(pid)

      opts = StubCh.opts(:no_url_pool)
      refute Keyword.has_key?(opts, :scheme)
      refute Keyword.has_key?(opts, :hostname)
      refute Keyword.has_key?(opts, :port)
    end

    test "explicit per-module hostname wins over URL" do
      Application.put_env(:soot_telemetry, :clickhouse_url, "http://from-url:1234")

      Application.put_env(:soot_telemetry, ClickHouse,
        name: :wins_pool,
        format: "ArrowStream",
        hostname: "explicit.host"
      )

      spec = ClickHouse.child_spec([])
      pid = start_supervised!(%{spec | id: :wins_pool_id})
      assert is_pid(pid)

      opts = StubCh.opts(:wins_pool)
      assert opts[:hostname] == "explicit.host"
      # scheme/port were not set explicitly — URL still fills them
      assert opts[:scheme] == "http"
      assert opts[:port] == 1234
    end
  end

  describe "configured/0 helpers" do
    test "pool_name/0 picks up :name from app env" do
      assert ClickHouse.pool_name() == :stub_ch_pool
    end

    test "configured_format/0 falls back to ArrowStream" do
      Application.put_env(:soot_telemetry, ClickHouse, name: :stub_ch_pool)
      assert ClickHouse.configured_format() == "ArrowStream"
    end

    test "configured_format/0 honours overrides" do
      Application.put_env(:soot_telemetry, ClickHouse, name: :stub_ch_pool, format: "Arrow")
      assert ClickHouse.configured_format() == "Arrow"
    end
  end

  describe "write/1 — pass-through" do
    setup do
      start_stub!()
      {:ok, _} = Registry.register(Vibration)
      :ok
    end

    test "forwards body bytes verbatim with INSERT … FORMAT statement" do
      assert :ok = ClickHouse.write(batch(body: <<0, 1, 2, 3, 0xFF>>))

      [call] = StubCh.calls(:stub_ch_pool)

      assert call.statement ==
               ~s|INSERT INTO "telemetry_vibration" FORMAT ArrowStream|

      assert call.params == <<0, 1, 2, 3, 0xFF>>
      assert call.opts[:encode] == false
      assert call.opts[:command] == :insert
    end

    test "uses the StreamRow's clickhouse_table" do
      {:ok, _} =
        Registry.register(SootTelemetry.Test.Fixtures.Power, clickhouse_table: "power_v2")

      assert :ok = ClickHouse.write(batch(stream: :power))
      [call] = StubCh.calls(:stub_ch_pool)
      assert call.statement =~ ~s|INSERT INTO "power_v2" FORMAT|
    end

    test "respects configured format override" do
      Application.put_env(:soot_telemetry, ClickHouse, name: :stub_ch_pool, format: "Arrow")
      assert :ok = ClickHouse.write(batch())
      [call] = StubCh.calls(:stub_ch_pool)
      assert call.statement =~ ~r/FORMAT Arrow$/
    end

    test "returns {:error, {:unknown_stream, _}} and logs when stream is unregistered" do
      log =
        capture_log(fn ->
          assert {:error, {:unknown_stream, :ghost}} = ClickHouse.write(batch(stream: :ghost))
        end)

      assert log =~ "ClickHouse insert failed"
      assert log =~ "stream=:ghost"
      assert StubCh.calls(:stub_ch_pool) == []
    end
  end

  describe "write/1 — failure surface" do
    setup do
      start_stub!()
      {:ok, _} = Registry.register(Vibration)
      :ok
    end

    test "wraps Ch errors in {:error, {:clickhouse_insert_failed, _}}" do
      Process.put(:stub_ch_response, {:error, %RuntimeError{message: "schema mismatch"}})

      log =
        capture_log(fn ->
          assert {:error, {:clickhouse_insert_failed, %RuntimeError{}}} =
                   ClickHouse.write(batch())
        end)

      assert log =~ "ClickHouse insert failed"
      assert log =~ "stream=:vibration"
      assert log =~ ~s|table="telemetry_vibration"|
      assert log =~ "format=ArrowStream"
      assert log =~ "device=\"device-fp\""
      assert log =~ "tenant=\"acme\""
      assert log =~ "fingerprint=\"abc123\""
      assert log =~ "bytes=4"
      assert log =~ "schema mismatch"
    end

    test "does not log on success" do
      log = capture_log(fn -> assert :ok = ClickHouse.write(batch()) end)
      refute log =~ "ClickHouse insert failed"
    end
  end
end
