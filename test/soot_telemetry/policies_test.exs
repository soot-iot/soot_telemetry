defmodule SootTelemetry.PoliciesTest do
  @moduledoc """
  Boundary tests for the default `policies` blocks shipped with
  `SootTelemetry.StreamRow`, `SootTelemetry.Schema`, and
  `SootTelemetry.IngestSession`.
  """

  use ExUnit.Case, async: false

  alias SootTelemetry.{Actors, IngestSession, Registry, Schema, StreamRow}
  alias SootTelemetry.Test.Factories
  alias SootTelemetry.Test.Fixtures.Vibration

  setup do
    Factories.reset!()
    {:ok, %{schema: schema, stream: stream}} = Registry.register(Vibration)

    tenant_id = Ecto.UUID.generate()

    {:ok, session} =
      IngestSession.create(
        Ecto.UUID.generate(),
        tenant_id,
        stream.id,
        stream.name,
        DateTime.utc_now(),
        actor: Actors.system(:ingest_session_writer)
      )

    {:ok, schema: schema, stream: stream, session: session, tenant_id: tenant_id}
  end

  describe "SootTelemetry.StreamRow" do
    test ":registry_sync can read", %{stream: stream} do
      assert {:ok, ^stream} =
               Ash.get(StreamRow, stream.id, actor: Actors.system(:registry_sync))
    end

    test "no actor is forbidden", %{stream: stream} do
      assert {:error, %Ash.Error.Forbidden{}} = Ash.get(StreamRow, stream.id)
    end

    test ":ingest_session_writer is forbidden on StreamRow", %{stream: stream} do
      assert {:error, %Ash.Error.Forbidden{}} =
               Ash.get(StreamRow, stream.id, actor: Actors.system(:ingest_session_writer))
    end

    test "admin can read", %{stream: stream} do
      assert {:ok, ^stream} = Ash.get(StreamRow, stream.id, actor: Actors.admin())
    end
  end

  describe "SootTelemetry.Schema" do
    test ":registry_sync can read", %{schema: schema} do
      assert {:ok, ^schema} = Ash.get(Schema, schema.id, actor: Actors.system(:registry_sync))
    end

    test "no actor is forbidden", %{schema: schema} do
      assert {:error, %Ash.Error.Forbidden{}} = Ash.get(Schema, schema.id)
    end

    test ":ingest_session_writer is forbidden on Schema", %{schema: schema} do
      assert {:error, %Ash.Error.Forbidden{}} =
               Ash.get(Schema, schema.id, actor: Actors.system(:ingest_session_writer))
    end

    test "admin can read", %{schema: schema} do
      assert {:ok, ^schema} = Ash.get(Schema, schema.id, actor: Actors.admin())
    end
  end

  describe "SootTelemetry.IngestSession" do
    test ":ingest_session_writer can read", %{session: session} do
      assert {:ok, ^session} =
               Ash.get(IngestSession, session.id, actor: Actors.system(:ingest_session_writer))
    end

    test ":registry_sync is forbidden on IngestSession", %{session: session} do
      assert {:error, %Ash.Error.Forbidden{}} =
               Ash.get(IngestSession, session.id, actor: Actors.system(:registry_sync))
    end

    test "no actor is forbidden", %{session: session} do
      assert {:error, %Ash.Error.Forbidden{}} = Ash.get(IngestSession, session.id)
    end

    test "admin in same tenant can read", %{session: session, tenant_id: tenant_id} do
      assert {:ok, ^session} =
               Ash.get(IngestSession, session.id, actor: Actors.admin(tenant_id))
    end

    test "admin in another tenant cannot see the row", %{session: session} do
      assert {:error, %Ash.Error.Invalid{errors: [%Ash.Error.Query.NotFound{}]}} =
               Ash.get(IngestSession, session.id, actor: Actors.admin(Ecto.UUID.generate()))
    end
  end
end
