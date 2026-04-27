defmodule SootTelemetry.Writer.ClickHouseLocalTest do
  @moduledoc """
  End-to-end tests against a local ClickHouse instance.

  Conventions match Postgres+Ecto tests in this repo: ClickHouse is
  assumed to be running on `localhost:8123` with the `default_test`
  database present. Each test creates and drops a uniquely-named
  table so the suite is rerun-safe and parallel-friendly.
  """

  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias SootTelemetry.Registry
  alias SootTelemetry.StreamRow
  alias SootTelemetry.Test.Factories
  alias SootTelemetry.Test.Fixtures.Vibration
  alias SootTelemetry.Writer.ClickHouse

  @database "default_test"
  @pool_name :soot_telemetry_writer_local_test_pool

  setup_all do
    {:ok, ch_pid} =
      Ch.start_link(
        scheme: "http",
        hostname: "localhost",
        port: 8123,
        database: @database,
        name: :soot_telemetry_writer_local_test_admin,
        pool_size: 1
      )

    on_exit(fn ->
      if Process.alive?(ch_pid), do: Process.exit(ch_pid, :normal)
    end)

    %{admin: :soot_telemetry_writer_local_test_admin}
  end

  setup ctx do
    Factories.reset!()

    table = "soot_telemetry_writer_test_" <> random_suffix()
    create_table!(ctx.admin, table)

    prev_writer_env = Application.get_env(:soot_telemetry, ClickHouse)
    prev_ch_module = Application.get_env(:soot_telemetry, :ch_module)

    Application.put_env(:soot_telemetry, ClickHouse,
      scheme: "http",
      hostname: "localhost",
      port: 8123,
      database: @database,
      name: @pool_name,
      pool_size: 1,
      format: "JSONEachRow"
    )

    Application.delete_env(:soot_telemetry, :ch_module)

    pid = start_supervised!(ClickHouse)
    {:ok, _} = Registry.register(Vibration, clickhouse_table: table)

    on_exit(fn ->
      if Process.alive?(pid), do: :ok
      drop_table!(ctx.admin, table)
      restore(:soot_telemetry, ClickHouse, prev_writer_env)
      restore(:soot_telemetry, :ch_module, prev_ch_module)
    end)

    {:ok, table: table}
  end

  defp restore(app, key, nil), do: Application.delete_env(app, key)
  defp restore(app, key, value), do: Application.put_env(app, key, value)

  defp random_suffix do
    :crypto.strong_rand_bytes(6) |> Base.encode16(case: :lower)
  end

  defp create_table!(admin, table) do
    sql = """
    CREATE TABLE #{@database}.#{table} (
      ts DateTime64(6, 'UTC'),
      device_id LowCardinality(String),
      tenant_id LowCardinality(String),
      axis_x Float32,
      axis_y Float32,
      axis_z Float32,
      sequence UInt64
    ) ENGINE = MergeTree
    ORDER BY (tenant_id, device_id, ts)
    """

    {:ok, _} = Ch.query(admin, sql)
  end

  defp drop_table!(admin, table) do
    {:ok, _} = Ch.query(admin, "DROP TABLE IF EXISTS #{@database}.#{table}")
  end

  defp count_rows(admin, table) do
    {:ok, %{rows: [[count]]}} =
      Ch.query(admin, "SELECT count() FROM #{@database}.#{table}", [], types: ["UInt64"])

    count
  end

  defp fetch_rows(admin, table) do
    {:ok, %{rows: rows}} =
      Ch.query(
        admin,
        "SELECT device_id, tenant_id, axis_x, sequence FROM #{@database}.#{table} ORDER BY sequence",
        [],
        types: ["String", "String", "Float32", "UInt64"]
      )

    rows
  end

  defp jsonl(rows) do
    rows
    |> Enum.map_join("", fn row -> Jason.encode!(row) <> "\n" end)
  end

  defp batch(body, overrides \\ []) do
    base = %{
      body: body,
      stream: :vibration,
      fingerprint: "abc123",
      sequence_start: 0,
      sequence_end: 0,
      device_id: "device-1",
      tenant_id: "acme",
      received_at: DateTime.utc_now()
    }

    Map.merge(base, Map.new(overrides))
  end

  describe "real ClickHouse — pass-through insert" do
    test "single-row JSONEachRow body lands as one row", %{admin: admin, table: table} do
      body =
        jsonl([
          %{
            ts: "2026-04-27 12:00:00.000000",
            device_id: "dev-aaa",
            tenant_id: "acme",
            axis_x: 1.5,
            axis_y: 2.5,
            axis_z: 3.5,
            sequence: 1
          }
        ])

      assert :ok = ClickHouse.write(batch(body))
      assert count_rows(admin, table) == 1

      assert [["dev-aaa", "acme", x, 1]] = fetch_rows(admin, table)
      assert_in_delta x, 1.5, 0.0001
    end

    test "multi-row body lands as multiple rows in order", %{admin: admin, table: table} do
      rows =
        for seq <- 1..5 do
          %{
            ts: "2026-04-27 12:00:0#{seq}.000000",
            device_id: "dev-bbb",
            tenant_id: "acme",
            axis_x: seq * 1.0,
            axis_y: 0.0,
            axis_z: 0.0,
            sequence: seq
          }
        end

      assert :ok = ClickHouse.write(batch(jsonl(rows), sequence_start: 1, sequence_end: 5))
      assert count_rows(admin, table) == 5

      sequences = fetch_rows(admin, table) |> Enum.map(fn [_, _, _, s] -> s end)
      assert sequences == [1, 2, 3, 4, 5]
    end

    test "empty body inserts no rows and still returns :ok", %{admin: admin, table: table} do
      assert :ok = ClickHouse.write(batch(""))
      assert count_rows(admin, table) == 0
    end
  end

  describe "real ClickHouse — failure surface" do
    test "rejects malformed JSONEachRow with a logged error", %{admin: admin, table: table} do
      body = "not valid json at all\n"

      log =
        capture_log(fn ->
          assert {:error, {:clickhouse_insert_failed, %Ch.Error{} = err}} =
                   ClickHouse.write(batch(body))

          assert Exception.message(err) =~ "Cannot parse" or
                   Exception.message(err) =~ "JSON" or
                   Exception.message(err) =~ "parse"
        end)

      assert log =~ "ClickHouse insert failed"
      assert log =~ "stream=:vibration"
      assert log =~ ~s|table="#{table}"|
      assert log =~ "format=JSONEachRow"
      assert log =~ "device=\"device-1\""
      assert count_rows(admin, table) == 0
    end

    test "rejects insert into a missing table with a logged error", %{admin: _admin, table: table} do
      drop_table!(:soot_telemetry_writer_local_test_admin, table)

      body =
        jsonl([
          %{
            ts: "2026-04-27 12:00:00.000000",
            device_id: "dev-x",
            tenant_id: "acme",
            axis_x: 0.0,
            axis_y: 0.0,
            axis_z: 0.0,
            sequence: 1
          }
        ])

      log =
        capture_log(fn ->
          assert {:error, {:clickhouse_insert_failed, %Ch.Error{} = err}} =
                   ClickHouse.write(batch(body))

          assert Exception.message(err) =~ "doesn't exist" or
                   Exception.message(err) =~ "UNKNOWN_TABLE" or
                   Exception.message(err) =~ "Unknown table"
        end)

      assert log =~ "ClickHouse insert failed"
      assert log =~ ~s|table="#{table}"|
    end
  end

  describe "real ClickHouse — table resolution" do
    test "uses the StreamRow.clickhouse_table configured at registration", %{
      admin: admin,
      table: table
    } do
      {:ok, %StreamRow{clickhouse_table: ^table}} =
        StreamRow.get_by_name(:vibration, authorize?: false)

      body =
        jsonl([
          %{
            ts: "2026-04-27 12:00:00.000000",
            device_id: "dev-z",
            tenant_id: "acme",
            axis_x: 9.0,
            axis_y: 0.0,
            axis_z: 0.0,
            sequence: 99
          }
        ])

      assert :ok = ClickHouse.write(batch(body, sequence_start: 99, sequence_end: 99))
      assert count_rows(admin, table) == 1
    end
  end
end
