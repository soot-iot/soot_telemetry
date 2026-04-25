defmodule SootTelemetry.IngestSession do
  @moduledoc """
  An open ingest connection (or the most recent batch from a device, in
  the lean topology).

  Tracks the per-(device, stream) sequence high water for replay
  protection and counters for observability. Exists in the OLTP store
  alongside the rest of `soot_core` so operators can correlate it with
  device state without crossing into the OLAP system.
  """

  use Ash.Resource,
    otp_app: :soot_telemetry,
    domain: SootTelemetry.Domain,
    data_layer: Ash.DataLayer.Ets

  ets do
    private? false
  end

  attributes do
    uuid_primary_key :id

    attribute :device_id, :uuid, allow_nil?: false, public?: true
    attribute :tenant_id, :uuid, public?: true
    attribute :stream_id, :uuid, allow_nil?: false, public?: true
    attribute :stream_name, :atom, allow_nil?: false, public?: true

    attribute :opened_at, :utc_datetime_usec, public?: true
    attribute :last_batch_at, :utc_datetime_usec, public?: true
    attribute :batch_count, :integer, default: 0, public?: true
    attribute :byte_count, :integer, default: 0, public?: true
    attribute :sequence_high_water, :integer, default: 0, public?: true

    create_timestamp :inserted_at
    update_timestamp :updated_at
  end

  identities do
    identity :one_per_device_stream, [:device_id, :stream_id],
      pre_check_with: SootTelemetry.Domain
  end

  actions do
    defaults [
      :read,
      :destroy,
      create: [:device_id, :tenant_id, :stream_id, :stream_name, :opened_at]
    ]

    update :record_batch do
      description "Bump batch_count, byte_count, last_batch_at, and sequence_high_water."
      accept []
      require_atomic? false

      argument :bytes, :integer, allow_nil?: false
      argument :sequence_end, :integer, allow_nil?: false

      change atomic_update(:batch_count, expr(batch_count + 1))
      change atomic_update(:byte_count, expr(byte_count + ^arg(:bytes)))
      change set_attribute(:last_batch_at, &DateTime.utc_now/0)
      change atomic_update(:sequence_high_water, expr(^arg(:sequence_end)))
    end

    read :for_device_stream do
      argument :device_id, :uuid, allow_nil?: false
      argument :stream_id, :uuid, allow_nil?: false
      get? true
      filter expr(device_id == ^arg(:device_id) and stream_id == ^arg(:stream_id))
    end

    read :for_stream do
      argument :stream_id, :uuid, allow_nil?: false
      filter expr(stream_id == ^arg(:stream_id))
    end
  end

  code_interface do
    define :create, args: [:device_id, :tenant_id, :stream_id, :stream_name, :opened_at]
    define :record_batch, args: [:bytes, :sequence_end]
    define :for_device_stream, args: [:device_id, :stream_id]
    define :for_stream, args: [:stream_id]
  end
end
