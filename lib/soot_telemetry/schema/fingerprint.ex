defmodule SootTelemetry.Schema.Fingerprint do
  @moduledoc """
  Compute a stable fingerprint for a telemetry stream's schema.

  The fingerprint is the hex-encoded SHA-256 of the canonical descriptor
  produced by `descriptor/1`. Two streams with the same fields (in the
  same declaration order, with the same flags) produce the same
  fingerprint regardless of module name.

  The descriptor is JSON-serialisable so it can be stored on
  `SootTelemetry.Schema.descriptor` and shipped to devices in a contract
  bundle.
  """

  alias SootTelemetry.Stream.{Field, Info}

  @doc """
  A canonical map describing the schema. Stable across runs of the same
  source.
  """
  @spec descriptor(module()) :: %{
          required(:name) => atom(),
          required(:fields) => [map()],
          required(:tenant_scope) => atom()
        }
  def descriptor(module) do
    %{
      name: Info.name(module),
      tenant_scope: Info.telemetry_stream_tenant_scope!(module),
      fields: Enum.map(Info.fields(module), &field_descriptor/1)
    }
  end

  @doc """
  Hex-encoded SHA-256 of the JSON-serialised descriptor (lowercase).
  """
  @spec compute(module()) :: String.t()
  def compute(module) do
    descriptor(module)
    |> canonical_json()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  @doc """
  Compute the fingerprint of a descriptor map directly. Used when
  comparing a stored descriptor against a live module.
  """
  @spec compute_descriptor(map()) :: String.t()
  def compute_descriptor(descriptor) when is_map(descriptor) do
    descriptor
    |> canonical_json()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp field_descriptor(%Field{} = f) do
    %{
      name: f.name,
      type: f.type,
      required: f.required,
      dictionary: f.dictionary,
      server_set: f.server_set,
      monotonic: f.monotonic
    }
  end

  # Order keys deterministically before encoding. Jason encodes maps in
  # an unspecified order otherwise, which would break the hash.
  defp canonical_json(value) do
    value
    |> sort_keys()
    |> Jason.encode!()
  end

  defp sort_keys(value) when is_map(value) do
    value
    |> Enum.map(fn {k, v} -> {to_string(k), sort_keys(v)} end)
    |> Enum.sort_by(fn {k, _} -> k end)
    |> Jason.OrderedObject.new()
  end

  defp sort_keys(value) when is_list(value), do: Enum.map(value, &sort_keys/1)
  defp sort_keys(value), do: value
end
