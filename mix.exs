defmodule SootTelemetry.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :soot_telemetry,
      version: @version,
      elixir: "~> 1.16",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      consolidate_protocols: Mix.env() != :test,
      deps: deps(),
      description: description(),
      package: package()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {SootTelemetry.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp description do
    "Telemetry stream resources, Arrow-schema DSL, /ingest endpoint, ClickHouse DDL generator."
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{}
    ]
  end

  defp deps do
    [
      {:ash, "~> 3.24"},
      {:spark, "~> 2.6"},
      {:ash_pki, path: "../ash_pki"},
      {:soot_core, path: "../soot_core"},
      {:plug, "~> 1.19"},
      {:jason, "~> 1.4"}
    ]
  end
end
