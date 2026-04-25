defmodule SootTelemetry.Stream.Definition do
  @moduledoc """
  Use this in a host module to opt into the telemetry-stream DSL.

      defmodule MyApp.Telemetry.Vibration do
        use SootTelemetry.Stream.Definition

        telemetry_stream do
          name :vibration
          fields do
            field :ts, :timestamp_us, required: true
            ...
          end
        end
      end

  Equivalent to
  `use Spark.Dsl, default_extensions: [extensions: [SootTelemetry.Stream]]`,
  which is also a perfectly valid spelling.
  """

  use Spark.Dsl, default_extensions: [extensions: [SootTelemetry.Stream]]
end
