defmodule SootTelemetry.FingerprintTest do
  use ExUnit.Case, async: true

  alias SootTelemetry.Schema.Fingerprint
  alias SootTelemetry.Test.Fixtures.{AmbientA, AmbientB, Power, Vibration}

  test "is deterministic across calls" do
    assert Fingerprint.compute(Vibration) == Fingerprint.compute(Vibration)
  end

  test "differs between modules with different fields" do
    refute Fingerprint.compute(Vibration) == Fingerprint.compute(Power)
  end

  test "is hex-encoded SHA-256 (64 lowercase hex chars)" do
    assert Fingerprint.compute(Vibration) =~ ~r/^[0-9a-f]{64}$/
  end

  test "descriptor is JSON-serialisable" do
    descriptor = Fingerprint.descriptor(Vibration)
    assert {:ok, _} = Jason.encode(descriptor)
  end

  test "compute_descriptor reproduces compute/1 from a stored descriptor" do
    descriptor = Fingerprint.descriptor(Vibration)
    assert Fingerprint.compute_descriptor(descriptor) == Fingerprint.compute(Vibration)
  end

  test "key ordering inside the descriptor doesn't break the hash" do
    # Manually construct a descriptor with the same content but a
    # different key insertion order; the canonical encoder should sort
    # before hashing.
    canonical = Fingerprint.descriptor(Vibration)
    shuffled = canonical |> Map.to_list() |> Enum.reverse() |> Map.new()

    assert Fingerprint.compute_descriptor(canonical) ==
             Fingerprint.compute_descriptor(shuffled)
  end

  test "field changes alter the fingerprint" do
    canonical = Fingerprint.descriptor(Vibration)

    mutated =
      Map.update!(canonical, :fields, fn fields ->
        fields ++
          [
            %{
              name: :extra,
              type: :float32,
              required: false,
              dictionary: false,
              server_set: false,
              monotonic: false
            }
          ]
      end)

    refute Fingerprint.compute_descriptor(canonical) ==
             Fingerprint.compute_descriptor(mutated)
  end

  test "stream name participates in the canonical descriptor and the fingerprint" do
    # AmbientA and AmbientB declare the same field set under different
    # stream names. Because the descriptor includes the stream name,
    # their fingerprints differ — which keeps the Schema rows distinct
    # at the registry level. This is the invariant the
    # `unique_fingerprint_per_stream` identity guards against
    # regressions in.
    assert Fingerprint.descriptor(AmbientA).name == :ambient_a
    assert Fingerprint.descriptor(AmbientB).name == :ambient_b
    refute Fingerprint.compute(AmbientA) == Fingerprint.compute(AmbientB)
  end
end
