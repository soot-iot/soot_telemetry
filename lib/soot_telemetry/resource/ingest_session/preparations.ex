defmodule SootTelemetry.Resource.IngestSession.Preparations do
  @moduledoc false

  defmodule ForDeviceStream do
    @moduledoc false
    use Ash.Resource.Preparation
    require Ash.Query

    @impl true
    def prepare(query, _opts, _context) do
      device_id = Ash.Query.get_argument(query, :device_id)
      stream_id = Ash.Query.get_argument(query, :stream_id)
      Ash.Query.filter(query, device_id == ^device_id and stream_id == ^stream_id)
    end
  end

  defmodule ForStream do
    @moduledoc false
    use Ash.Resource.Preparation
    require Ash.Query

    @impl true
    def prepare(query, _opts, _context) do
      stream_id = Ash.Query.get_argument(query, :stream_id)
      Ash.Query.filter(query, stream_id == ^stream_id)
    end
  end
end
