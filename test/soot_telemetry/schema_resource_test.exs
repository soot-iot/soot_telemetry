defmodule SootTelemetry.SchemaResourceTest do
  use ExUnit.Case, async: false

  alias SootTelemetry.Schema
  alias SootTelemetry.Test.Factories

  setup do
    Factories.reset!()

    {:ok, schema} =
      Schema.create(:vibration, 1, "fp-aaa", %{"name" => "vibration"}, authorize?: false)

    {:ok, %{schema: schema}}
  end

  describe "create" do
    test "rejects duplicate (stream_name, fingerprint)", _ctx do
      assert {:error, %Ash.Error.Invalid{}} =
               Schema.create(:vibration, 2, "fp-aaa", %{}, authorize?: false)
    end

    test "rejects duplicate (stream_name, version)", _ctx do
      assert {:error, %Ash.Error.Invalid{}} =
               Schema.create(:vibration, 1, "fp-bbb", %{}, authorize?: false)
    end

    test "the same fingerprint is allowed for a different stream", _ctx do
      assert {:ok, %Schema{} = other} =
               Schema.create(:power, 1, "fp-aaa", %{}, authorize?: false)

      assert other.stream_name == :power
      assert other.fingerprint == "fp-aaa"
    end
  end

  describe "lifecycle transitions" do
    test "deprecate flips :active → :deprecated", ctx do
      {:ok, deprecated} = Schema.deprecate(ctx.schema, authorize?: false)
      assert deprecated.status == :deprecated
    end

    test "retire flips into :retired", ctx do
      {:ok, retired} = Schema.retire(ctx.schema, authorize?: false)
      assert retired.status == :retired
    end
  end

  describe "lookup actions" do
    test "get_for_stream_fingerprint returns the matching row", ctx do
      assert {:ok, %Schema{id: id}} =
               Schema.get_for_stream_fingerprint(:vibration, "fp-aaa", authorize?: false)

      assert id == ctx.schema.id
    end

    test "get_for_stream_fingerprint returns {:error, _} on miss", _ctx do
      assert {:error, _} =
               Schema.get_for_stream_fingerprint(:vibration, "fp-zzz", authorize?: false)
    end

    test "for_stream returns all schemas for a stream, descending by version" do
      {:ok, _} = Schema.create(:vibration, 2, "fp-bbb", %{}, authorize?: false)
      {:ok, _} = Schema.create(:vibration, 3, "fp-ccc", %{}, authorize?: false)

      {:ok, schemas} = Schema.for_stream(:vibration, authorize?: false)
      versions = Enum.map(schemas, & &1.version)
      assert versions == [3, 2, 1]
    end
  end
end
