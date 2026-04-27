defmodule SootTelemetry.Resource.StreamRow.Transformers.Inject do
  @moduledoc false
  use Spark.Dsl.Transformer

  alias Ash.Resource.Builder
  alias SootTelemetry.Resource.StreamRow.Preparations
  alias Spark.Dsl.Transformer

  @tenant_scopes [:per_tenant, :shared]
  @statuses [:active, :paused, :retired]

  @impl true
  def before?(Ash.Resource.Transformers.CachePrimaryKey), do: true
  def before?(_), do: false

  @impl true
  def transform(dsl_state) do
    domain = Transformer.get_persisted(dsl_state, :domain) || domain_from_dsl(dsl_state)

    with {:ok, dsl_state} <- add_attributes(dsl_state),
         {:ok, dsl_state} <- add_identities(dsl_state, domain),
         {:ok, dsl_state} <- add_actions(dsl_state) do
      add_code_interface(dsl_state)
    end
  end

  defp domain_from_dsl(dsl_state) do
    Transformer.get_option(dsl_state, [:resource], :domain)
  end

  defp add_attributes(dsl_state) do
    with {:ok, dsl_state} <- ensure_uuid_primary_key(dsl_state),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :name, :atom,
             allow_nil?: false,
             public?: true
           ),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :module, :atom,
             description: "The Spark module carrying the DSL declarations for this stream.",
             public?: true
           ),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :tenant_scope, :atom,
             constraints: [one_of: @tenant_scopes],
             default: :per_tenant,
             allow_nil?: false,
             public?: true
           ),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :current_schema_id, :uuid,
             allow_nil?: false,
             public?: true
           ),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :clickhouse_table, :string, public?: true),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :retention, :map, default: %{}, public?: true),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :partitioning, :string, public?: true),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :status, :atom,
             constraints: [one_of: @statuses],
             default: :active,
             allow_nil?: false,
             public?: true
           ),
         {:ok, dsl_state} <- Builder.add_new_create_timestamp(dsl_state, :inserted_at) do
      Builder.add_new_update_timestamp(dsl_state, :updated_at)
    end
  end

  defp ensure_uuid_primary_key(dsl_state) do
    if Ash.Resource.Info.attribute(dsl_state, :id) do
      {:ok, dsl_state}
    else
      Builder.add_new_attribute(dsl_state, :id, :uuid,
        primary_key?: true,
        allow_nil?: false,
        public?: true,
        default: &Ash.UUID.generate/0,
        match_other_defaults?: true
      )
    end
  end

  defp add_identities(dsl_state, domain) do
    Builder.add_new_identity(dsl_state, :unique_name, [:name], pre_check_with: domain)
  end

  defp add_actions(dsl_state) do
    with {:ok, dsl_state} <-
           Builder.add_new_action(dsl_state, :read, :read, primary?: true),
         {:ok, dsl_state} <-
           Builder.add_new_action(dsl_state, :destroy, :destroy, primary?: true, accept: []),
         {:ok, dsl_state} <-
           Builder.add_new_action(dsl_state, :create, :create,
             primary?: true,
             accept: [
               :name,
               :module,
               :tenant_scope,
               :current_schema_id,
               :clickhouse_table,
               :retention,
               :partitioning
             ]
           ),
         {:ok, dsl_state} <-
           Builder.add_new_action(dsl_state, :update, :update,
             primary?: true,
             require_atomic?: false,
             accept: [:current_schema_id, :clickhouse_table, :retention, :partitioning]
           ),
         {:ok, dsl_state} <-
           Builder.add_new_action(dsl_state, :update, :pause,
             accept: [],
             require_atomic?: false,
             changes: [
               Builder.build_action_change(
                 {Ash.Resource.Change.SetAttribute, attribute: :status, value: :paused}
               )
             ]
           ),
         {:ok, dsl_state} <-
           Builder.add_new_action(dsl_state, :update, :resume,
             accept: [],
             require_atomic?: false,
             changes: [
               Builder.build_action_change(
                 {Ash.Resource.Change.SetAttribute, attribute: :status, value: :active}
               )
             ]
           ),
         {:ok, dsl_state} <-
           Builder.add_new_action(dsl_state, :update, :retire,
             accept: [],
             require_atomic?: false,
             changes: [
               Builder.build_action_change(
                 {Ash.Resource.Change.SetAttribute, attribute: :status, value: :retired}
               )
             ]
           ) do
      Builder.add_new_action(dsl_state, :read, :get_by_name,
        arguments: [
          Builder.build_action_argument(:name, :atom, allow_nil?: false)
        ],
        get?: true,
        preparations: [Builder.build_preparation(Preparations.GetByName)]
      )
    end
  end

  defp add_code_interface(dsl_state) do
    with {:ok, dsl_state} <-
           Builder.add_new_interface(dsl_state, :create,
             args: [:name, :module, :tenant_scope, :current_schema_id]
           ),
         {:ok, dsl_state} <- Builder.add_new_interface(dsl_state, :pause),
         {:ok, dsl_state} <- Builder.add_new_interface(dsl_state, :resume),
         {:ok, dsl_state} <- Builder.add_new_interface(dsl_state, :retire) do
      Builder.add_new_interface(dsl_state, :get_by_name, args: [:name])
    end
  end
end
