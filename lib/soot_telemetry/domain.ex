defmodule SootTelemetry.Domain do
  @moduledoc """
  Ash domain holding the Schema, Stream and IngestSession resources.
  """

  use Ash.Domain, otp_app: :soot_telemetry, validate_config_inclusion?: false

  resources do
    resource SootTelemetry.Schema
    resource SootTelemetry.StreamRow
    resource SootTelemetry.IngestSession
  end
end
