defmodule SootTelemetry.StreamRowTest do
  use ExUnit.Case, async: false

  alias SootTelemetry.{Schema, StreamRow}
  alias SootTelemetry.Test.Factories

  setup do
    Factories.reset!()

    {:ok, schema} =
      Schema.create(:vibration, 1, "fp-aaa", %{"name" => "vibration"}, authorize?: false)

    {:ok, stream} =
      StreamRow.create(:vibration, SootTelemetry.Test.Fixtures.Vibration, :per_tenant, schema.id,
        authorize?: false
      )

    {:ok, %{stream: stream, schema: schema}}
  end

  describe "create" do
    test "rejects a duplicate name", ctx do
      assert {:error, %Ash.Error.Invalid{}} =
               StreamRow.create(
                 :vibration,
                 SootTelemetry.Test.Fixtures.Vibration,
                 :per_tenant,
                 ctx.schema.id,
                 authorize?: false
               )
    end
  end

  describe "lifecycle" do
    test "pause → :paused, resume → :active, retire → :retired", ctx do
      {:ok, paused} = StreamRow.pause(ctx.stream)
      assert paused.status == :paused

      {:ok, resumed} = StreamRow.resume(paused)
      assert resumed.status == :active

      {:ok, retired} = StreamRow.retire(resumed)
      assert retired.status == :retired
    end
  end

  describe "get_by_name" do
    test "returns the row", ctx do
      assert {:ok, %StreamRow{id: id}} = StreamRow.get_by_name(:vibration)
      assert id == ctx.stream.id
    end

    test "returns {:error, _} when missing" do
      assert {:error, _} = StreamRow.get_by_name(:nope)
    end
  end
end
