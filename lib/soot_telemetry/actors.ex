defmodule SootTelemetry.Actors do
  @moduledoc """
  Actor factory for `soot_telemetry`.

  Two System parts originate here:

    * `:registry_sync` — internal stream / schema metadata writes
      and reads (the `Registry`, the ClickHouse writer's stream
      lookup, the ingest plug's stream/schema metadata loads).

    * `:ingest_session_writer` — bookkeeping writes against
      `IngestSession` from the device ingest plug. This is a
      transitional name; once the cross-library `MTLS.Resolver` lands
      and resolves the cert to a `Device` actor (POLICY-SPEC §3.1),
      session writes will use that Device-as-actor and this part
      will be removed.

  See umbrella `soot/POLICY-SPEC.md` for the cross-library actor
  contract.
  """

  alias SootTelemetry.Actors.System

  @type system_part :: System.part()

  @doc "Build a `System` actor for an internal subsystem."
  @spec system(system_part()) :: System.t()
  def system(part) when is_atom(part), do: %System{part: part}

  @spec system(system_part(), keyword() | binary() | nil) :: System.t()
  def system(part, tenant_id) when is_atom(part) and is_binary(tenant_id),
    do: %System{part: part, tenant_id: tenant_id}

  def system(part, nil) when is_atom(part), do: %System{part: part}

  def system(part, opts) when is_atom(part) and is_list(opts),
    do: %System{part: part, tenant_id: Keyword.get(opts, :tenant_id)}
end
