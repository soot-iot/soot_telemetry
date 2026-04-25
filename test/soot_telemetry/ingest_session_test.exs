defmodule SootTelemetry.IngestSessionTest do
  use ExUnit.Case, async: false

  alias SootTelemetry.IngestSession
  alias SootTelemetry.Test.Factories

  setup do
    Factories.reset!()

    device_id = Ecto.UUID.generate()
    tenant_id = Ecto.UUID.generate()
    stream_id = Ecto.UUID.generate()

    {:ok, session} =
      IngestSession.create(
        device_id,
        tenant_id,
        stream_id,
        :vibration,
        DateTime.utc_now(),
        authorize?: false
      )

    {:ok, %{session: session, device_id: device_id, stream_id: stream_id, tenant_id: tenant_id}}
  end

  describe "create" do
    test "fills the audited fields and zeroes counters", ctx do
      assert ctx.session.device_id == ctx.device_id
      assert ctx.session.tenant_id == ctx.tenant_id
      assert ctx.session.stream_id == ctx.stream_id
      assert ctx.session.stream_name == :vibration
      assert ctx.session.batch_count == 0
      assert ctx.session.byte_count == 0
      assert ctx.session.sequence_high_water == 0
    end

    test "rejects a duplicate (device_id, stream_id) pair", ctx do
      assert {:error, %Ash.Error.Invalid{}} =
               IngestSession.create(
                 ctx.device_id,
                 ctx.tenant_id,
                 ctx.stream_id,
                 :vibration,
                 DateTime.utc_now(),
                 authorize?: false
               )
    end
  end

  describe "record_batch" do
    test "advances batch_count, byte_count, sequence_high_water, last_batch_at", ctx do
      {:ok, after_first} = IngestSession.record_batch(ctx.session, 64, 9, authorize?: false)

      assert after_first.batch_count == 1
      assert after_first.byte_count == 64
      assert after_first.sequence_high_water == 9
      assert after_first.last_batch_at != nil

      {:ok, after_second} = IngestSession.record_batch(after_first, 32, 19, authorize?: false)
      assert after_second.batch_count == 2
      assert after_second.byte_count == 96
      assert after_second.sequence_high_water == 19
    end

    test "clamps high-water to its monotone maximum", ctx do
      {:ok, advanced} = IngestSession.record_batch(ctx.session, 1, 100, authorize?: false)
      assert advanced.sequence_high_water == 100

      # An in-grace late batch reports an earlier sequence_end. The
      # high-water must not regress.
      {:ok, late} = IngestSession.record_batch(advanced, 1, 95, authorize?: false)
      assert late.sequence_high_water == 100
      assert late.batch_count == 2
    end
  end

  describe "for_device_stream" do
    test "returns the matching session", ctx do
      assert {:ok, %IngestSession{id: id}} =
               IngestSession.for_device_stream(ctx.device_id, ctx.stream_id, authorize?: false)

      assert id == ctx.session.id
    end

    test "returns {:error, _} when no match", _ctx do
      assert {:error, _} =
               IngestSession.for_device_stream(
                 Ecto.UUID.generate(),
                 Ecto.UUID.generate(),
                 authorize?: false
               )
    end
  end

  describe "for_stream" do
    test "returns every session for a stream", ctx do
      other_device = Ecto.UUID.generate()

      {:ok, _} =
        IngestSession.create(
          other_device,
          ctx.tenant_id,
          ctx.stream_id,
          :vibration,
          DateTime.utc_now(),
          authorize?: false
        )

      {:ok, sessions} = IngestSession.for_stream(ctx.stream_id, authorize?: false)
      assert length(sessions) == 2
    end
  end
end
