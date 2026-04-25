defmodule SootTelemetry.Plug.Ingest.TenantSan do
  @moduledoc """
  Extract the tenant slug from an mTLS actor's SAN list.

  Convention: the SPIFFE-style URI SAN encodes the tenant slug —
  `URI:device://<tenant>/devices/<serial>`. If the device's cert
  doesn't follow that pattern, returns `nil`.
  """

  alias AshPki.Plug.MTLS.Actor

  @doc "Resolve a tenant slug from an actor, or `nil` when unknown."
  @spec resolve(Actor.t() | nil) :: String.t() | nil
  def resolve(%Actor{san: san}) when is_list(san) do
    Enum.find_value(san, fn
      {:uniformResourceIdentifier, charlist} ->
        case String.split(List.to_string(charlist), "/") do
          ["device:", "", tenant | _] -> tenant
          _ -> nil
        end

      _ ->
        nil
    end)
  end

  def resolve(_), do: nil
end
