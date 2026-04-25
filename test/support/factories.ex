defmodule SootTelemetry.Test.Factories do
  @moduledoc false

  def reset! do
    SootTelemetry.RateLimiter.reset_all()

    for resource <- [
          SootTelemetry.Schema,
          SootTelemetry.StreamRow,
          SootTelemetry.IngestSession
        ] do
      try do
        :ets.delete_all_objects(resource)
      rescue
        _ -> :ok
      end
    end

    :ok
  end
end
