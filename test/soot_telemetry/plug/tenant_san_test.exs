defmodule SootTelemetry.Plug.Ingest.TenantSanTest do
  use ExUnit.Case, async: true

  alias AshPki.Plug.MTLS.Actor
  alias SootTelemetry.Plug.Ingest.TenantSan

  defp actor(san),
    do: %Actor{
      certificate_id: "cert-1",
      issuer_id: nil,
      subject_dn: "/CN=device",
      serial: "1",
      fingerprint: "abc",
      san: san,
      pem: "",
      raw_cert: nil
    }

  describe "resolve/1" do
    test "extracts the tenant from a SPIFFE-style URI SAN" do
      assert TenantSan.resolve(
               actor([{:uniformResourceIdentifier, ~c"device://acme/devices/SN1"}])
             ) == "acme"
    end

    test "iterates past non-URI entries to find the URI" do
      san = [
        {:dNSName, ~c"device-001.acme"},
        {:uniformResourceIdentifier, ~c"device://acme/devices/SN1"}
      ]

      assert TenantSan.resolve(actor(san)) == "acme"
    end

    test "returns nil for a URI that doesn't match the convention" do
      assert TenantSan.resolve(
               actor([{:uniformResourceIdentifier, ~c"https://example.com"}])
             ) == nil
    end

    test "returns nil when no URI entries exist" do
      assert TenantSan.resolve(actor([{:dNSName, ~c"device-001.acme"}])) == nil
    end

    test "returns nil for an empty SAN list" do
      assert TenantSan.resolve(actor([])) == nil
    end

    test "returns nil when san is the struct's nil default" do
      assert TenantSan.resolve(actor(nil)) == nil
    end

    test "returns nil for a non-actor input" do
      assert TenantSan.resolve(nil) == nil
    end
  end
end
