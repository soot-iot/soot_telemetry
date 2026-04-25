defmodule SootTelemetry.StreamRow do
  @moduledoc """
  A registered telemetry stream.

  One row per unique stream `name`. `current_schema_id` points at the
  `SootTelemetry.Schema` row whose fingerprint the ingest endpoint
  expects right now.

  The Ash resource is named `StreamRow` to avoid colliding with the
  Spark DSL extension `SootTelemetry.Stream` that lives on user modules.
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

    attribute :name, :atom do
      allow_nil? false
      public? true
    end

    attribute :module, :atom do
      description "The Spark module carrying the DSL declarations for this stream."
      public? true
    end

    attribute :tenant_scope, :atom do
      constraints one_of: [:per_tenant, :shared]
      default :per_tenant
      allow_nil? false
      public? true
    end

    attribute :current_schema_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :clickhouse_table, :string, public?: true
    attribute :retention, :map, default: %{}, public?: true
    attribute :partitioning, :string, public?: true

    attribute :status, :atom do
      constraints one_of: [:active, :paused, :retired]
      default :active
      allow_nil? false
      public? true
    end

    create_timestamp :inserted_at
    update_timestamp :updated_at
  end

  identities do
    identity :unique_name, [:name], pre_check_with: SootTelemetry.Domain
  end

  actions do
    defaults [
      :read,
      :destroy,
      create: [
        :name,
        :module,
        :tenant_scope,
        :current_schema_id,
        :clickhouse_table,
        :retention,
        :partitioning
      ],
      update: [:current_schema_id, :clickhouse_table, :retention, :partitioning]
    ]

    update :pause do
      accept []
      require_atomic? false
      change set_attribute(:status, :paused)
    end

    update :resume do
      accept []
      require_atomic? false
      change set_attribute(:status, :active)
    end

    update :retire do
      accept []
      require_atomic? false
      change set_attribute(:status, :retired)
    end

    read :get_by_name do
      argument :name, :atom, allow_nil?: false
      get? true
      filter expr(name == ^arg(:name))
    end
  end

  code_interface do
    define :create,
      args: [:name, :module, :tenant_scope, :current_schema_id]

    define :pause
    define :resume
    define :retire
    define :get_by_name, args: [:name]
  end
end
