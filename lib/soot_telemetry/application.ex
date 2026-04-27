defmodule SootTelemetry.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    children =
      [SootTelemetry.RateLimiter] ++ writer_children()

    Supervisor.start_link(children, strategy: :one_for_one, name: SootTelemetry.Supervisor)
  end

  defp writer_children do
    case Application.get_env(:soot_telemetry, :writer) do
      SootTelemetry.Writer.ClickHouse -> [SootTelemetry.Writer.ClickHouse]
      _ -> []
    end
  end
end
