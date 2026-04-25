defmodule SootTelemetry.WriterTest do
  use ExUnit.Case, async: false

  alias SootTelemetry.Writer

  defmodule Stub do
    @behaviour SootTelemetry.Writer

    def write(batch) do
      send(self(), {:wrote, batch})
      Process.get(:stub_writer_response, :ok)
    end
  end

  setup do
    on_exit(fn -> Application.delete_env(:soot_telemetry, :writer) end)
    :ok
  end

  describe "Writer.Noop" do
    test "accepts the documented batch shape and returns :ok" do
      assert :ok =
               Writer.Noop.write(%{
                 body: <<>>,
                 stream: :vibration,
                 fingerprint: "00",
                 sequence_start: 0,
                 sequence_end: 0,
                 device_id: nil,
                 tenant_id: nil,
                 received_at: DateTime.utc_now()
               })
    end
  end

  describe "configured/0" do
    test "defaults to Noop when no env override" do
      assert Writer.configured() == SootTelemetry.Writer.Noop
    end

    test "honours :writer application env" do
      Application.put_env(:soot_telemetry, :writer, Stub)
      assert Writer.configured() == Stub
    end
  end

  describe "write/1" do
    test "delegates to the configured writer" do
      Application.put_env(:soot_telemetry, :writer, Stub)
      Process.put(:stub_writer_response, :ok)

      batch = %{body: <<1, 2, 3>>, stream: :vibration, fingerprint: "abc"}
      assert :ok = Writer.write(batch)
      assert_received {:wrote, ^batch}
    end

    test "surfaces writer-side errors verbatim" do
      Application.put_env(:soot_telemetry, :writer, Stub)
      Process.put(:stub_writer_response, {:error, :boom})

      assert {:error, :boom} = Writer.write(%{body: "", stream: :x, fingerprint: ""})
    end
  end
end
