defmodule SootTelemetry.Resource.Schema.Transformers.Inject do
  @moduledoc false
  use Spark.Dsl.Transformer

  alias Ash.Resource.Builder
  alias SootTelemetry.Resource.Schema.Preparations
  alias Spark.Dsl.Transformer

  @statuses [:active, :deprecated, :retired]

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
           Builder.add_new_attribute(dsl_state, :stream_name, :atom,
             allow_nil?: false,
             public?: true
           ),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :version, :integer,
             allow_nil?: false,
             public?: true
           ),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :fingerprint, :string,
             allow_nil?: false,
             public?: true
           ),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :descriptor, :map,
             allow_nil?: false,
             public?: true
           ),
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
    with {:ok, dsl_state} <-
           Builder.add_new_identity(
             dsl_state,
             :unique_fingerprint_per_stream,
             [:stream_name, :fingerprint],
             pre_check_with: domain
           ) do
      Builder.add_new_identity(
        dsl_state,
        :unique_version_per_stream,
        [:stream_name, :version],
        pre_check_with: domain
      )
    end
  end

  defp add_actions(dsl_state) do
    with {:ok, dsl_state} <-
           Builder.add_new_action(dsl_state, :read, :read, primary?: true),
         {:ok, dsl_state} <-
           Builder.add_new_action(dsl_state, :destroy, :destroy, primary?: true, accept: []),
         {:ok, dsl_state} <-
           Builder.add_new_action(dsl_state, :create, :create,
             primary?: true,
             accept: [:stream_name, :version, :fingerprint, :descriptor]
           ),
         {:ok, dsl_state} <-
           Builder.add_new_action(dsl_state, :update, :deprecate,
             accept: [],
             require_atomic?: false,
             changes: [
               Builder.build_action_change(
                 {Ash.Resource.Change.SetAttribute, attribute: :status, value: :deprecated}
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
           ),
         {:ok, dsl_state} <-
           Builder.add_new_action(dsl_state, :read, :get_for_stream_fingerprint,
             arguments: [
               Builder.build_action_argument(:stream_name, :atom, allow_nil?: false),
               Builder.build_action_argument(:fingerprint, :string, allow_nil?: false)
             ],
             get?: true,
             preparations: [Builder.build_preparation(Preparations.GetForStreamFingerprint)]
           ) do
      Builder.add_new_action(dsl_state, :read, :for_stream,
        arguments: [
          Builder.build_action_argument(:stream_name, :atom, allow_nil?: false)
        ],
        preparations: [Builder.build_preparation(Preparations.ForStream)]
      )
    end
  end

  defp add_code_interface(dsl_state) do
    with {:ok, dsl_state} <-
           Builder.add_new_interface(dsl_state, :create,
             args: [:stream_name, :version, :fingerprint, :descriptor]
           ),
         {:ok, dsl_state} <- Builder.add_new_interface(dsl_state, :deprecate),
         {:ok, dsl_state} <- Builder.add_new_interface(dsl_state, :retire),
         {:ok, dsl_state} <-
           Builder.add_new_interface(dsl_state, :get_for_stream_fingerprint,
             args: [:stream_name, :fingerprint]
           ) do
      Builder.add_new_interface(dsl_state, :for_stream, args: [:stream_name])
    end
  end
end
