defmodule SootTelemetry.Actors.System do
  @moduledoc """
  Internal-subsystem actor for `soot_telemetry`.

  See `SootTelemetry.Actors` for `:part` semantics.
  """

  @enforce_keys [:part]
  defstruct [:part, :tenant_id]

  @type part :: :registry_sync | :ingest_session_writer

  @type t :: %__MODULE__{
          part: part(),
          tenant_id: String.t() | nil
        }
end
