defmodule SootTelemetry.Resource.IngestSession.Transformers.Inject do
  @moduledoc false
  use Spark.Dsl.Transformer

  alias Ash.Resource.Builder
  alias SootTelemetry.IngestSession.Changes.ClampSequenceHighWater
  alias SootTelemetry.Resource.IngestSession.Preparations
  alias Spark.Dsl.Transformer

  require Ash.Expr
  import Ash.Expr, only: [arg: 1]

  @bump_batch_count Builder.build_action_change(
                      {Ash.Resource.Change.Atomic,
                       attribute: :batch_count,
                       expr: Ash.Expr.expr(batch_count + 1),
                       cast_atomic?: true}
                    )

  @bump_byte_count Builder.build_action_change(
                     {Ash.Resource.Change.Atomic,
                      attribute: :byte_count,
                      expr: Ash.Expr.expr(byte_count + ^arg(:bytes)),
                      cast_atomic?: true}
                   )

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
           Builder.add_new_attribute(dsl_state, :device_id, :uuid,
             allow_nil?: false,
             public?: true
           ),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :tenant_id, :uuid, public?: true),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :stream_id, :uuid,
             allow_nil?: false,
             public?: true
           ),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :stream_name, :atom,
             allow_nil?: false,
             public?: true
           ),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :opened_at, :utc_datetime_usec, public?: true),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :last_batch_at, :utc_datetime_usec, public?: true),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :batch_count, :integer,
             default: 0,
             public?: true
           ),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :byte_count, :integer, default: 0, public?: true),
         {:ok, dsl_state} <-
           Builder.add_new_attribute(dsl_state, :sequence_high_water, :integer,
             default: 0,
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
    Builder.add_new_identity(
      dsl_state,
      :one_per_device_stream,
      [:device_id, :stream_id],
      pre_check_with: domain
    )
  end

  defp add_actions(dsl_state) do
    with {:ok, dsl_state} <-
           Builder.add_new_action(dsl_state, :read, :read, primary?: true),
         {:ok, dsl_state} <-
           Builder.add_new_action(dsl_state, :destroy, :destroy, primary?: true, accept: []),
         {:ok, dsl_state} <-
           Builder.add_new_action(dsl_state, :create, :create,
             primary?: true,
             accept: [:device_id, :tenant_id, :stream_id, :stream_name, :opened_at]
           ),
         {:ok, dsl_state} <-
           Builder.add_new_action(dsl_state, :update, :record_batch,
             description: "Bump batch_count, byte_count, last_batch_at, and sequence_high_water.",
             accept: [],
             require_atomic?: false,
             arguments: [
               Builder.build_action_argument(:bytes, :integer, allow_nil?: false),
               Builder.build_action_argument(:sequence_end, :integer, allow_nil?: false)
             ],
             changes: [
               @bump_batch_count,
               @bump_byte_count,
               Builder.build_action_change(
                 {Ash.Resource.Change.SetAttribute,
                  attribute: :last_batch_at, value: &DateTime.utc_now/0}
               ),
               Builder.build_action_change(ClampSequenceHighWater)
             ]
           ),
         {:ok, dsl_state} <-
           Builder.add_new_action(dsl_state, :read, :for_device_stream,
             arguments: [
               Builder.build_action_argument(:device_id, :uuid, allow_nil?: false),
               Builder.build_action_argument(:stream_id, :uuid, allow_nil?: false)
             ],
             get?: true,
             preparations: [Builder.build_preparation(Preparations.ForDeviceStream)]
           ) do
      Builder.add_new_action(dsl_state, :read, :for_stream,
        arguments: [
          Builder.build_action_argument(:stream_id, :uuid, allow_nil?: false)
        ],
        preparations: [Builder.build_preparation(Preparations.ForStream)]
      )
    end
  end

  defp add_code_interface(dsl_state) do
    with {:ok, dsl_state} <-
           Builder.add_new_interface(dsl_state, :create,
             args: [:device_id, :tenant_id, :stream_id, :stream_name, :opened_at]
           ),
         {:ok, dsl_state} <-
           Builder.add_new_interface(dsl_state, :record_batch, args: [:bytes, :sequence_end]),
         {:ok, dsl_state} <-
           Builder.add_new_interface(dsl_state, :for_device_stream,
             args: [:device_id, :stream_id]
           ) do
      Builder.add_new_interface(dsl_state, :for_stream, args: [:stream_id])
    end
  end
end
