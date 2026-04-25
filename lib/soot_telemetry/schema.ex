defmodule SootTelemetry.Schema do
  @moduledoc """
  An immutable, versioned snapshot of a stream's Arrow schema.

  Each unique fingerprint produces one row. The current row for a given
  stream is referenced by `SootTelemetry.StreamRow.current_schema_id`.
  Rows are never mutated past the active/deprecated transition; new
  schemas land as new rows.
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

    attribute :stream_name, :atom do
      allow_nil? false
      public? true
    end

    attribute :version, :integer do
      allow_nil? false
      public? true
    end

    attribute :fingerprint, :string do
      allow_nil? false
      public? true
    end

    attribute :descriptor, :map do
      allow_nil? false
      public? true
    end

    attribute :status, :atom do
      constraints one_of: [:active, :deprecated, :retired]
      default :active
      allow_nil? false
      public? true
    end

    create_timestamp :inserted_at
    update_timestamp :updated_at
  end

  identities do
    identity :unique_fingerprint, [:fingerprint], pre_check_with: SootTelemetry.Domain
    identity :unique_version_per_stream, [:stream_name, :version], pre_check_with: SootTelemetry.Domain
  end

  actions do
    defaults [:read, :destroy, create: [:stream_name, :version, :fingerprint, :descriptor]]

    update :deprecate do
      accept []
      require_atomic? false
      change set_attribute(:status, :deprecated)
    end

    update :retire do
      accept []
      require_atomic? false
      change set_attribute(:status, :retired)
    end

    read :get_by_fingerprint do
      argument :fingerprint, :string, allow_nil?: false
      get? true
      filter expr(fingerprint == ^arg(:fingerprint))
    end

    read :for_stream do
      argument :stream_name, :atom, allow_nil?: false
      filter expr(stream_name == ^arg(:stream_name))
      prepare build(sort: [version: :desc])
    end
  end

  code_interface do
    define :create, args: [:stream_name, :version, :fingerprint, :descriptor]
    define :deprecate
    define :retire
    define :get_by_fingerprint, args: [:fingerprint]
    define :for_stream, args: [:stream_name]
  end
end
