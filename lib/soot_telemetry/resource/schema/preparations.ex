defmodule SootTelemetry.Resource.Schema.Preparations do
  @moduledoc false

  defmodule GetForStreamFingerprint do
    @moduledoc false
    use Ash.Resource.Preparation
    require Ash.Query

    @impl true
    def prepare(query, _opts, _context) do
      stream_name = Ash.Query.get_argument(query, :stream_name)
      fingerprint = Ash.Query.get_argument(query, :fingerprint)
      Ash.Query.filter(query, stream_name == ^stream_name and fingerprint == ^fingerprint)
    end
  end

  defmodule ForStream do
    @moduledoc false
    use Ash.Resource.Preparation
    require Ash.Query

    @impl true
    def prepare(query, _opts, _context) do
      stream_name = Ash.Query.get_argument(query, :stream_name)

      query
      |> Ash.Query.filter(stream_name == ^stream_name)
      |> Ash.Query.sort(version: :desc)
    end
  end
end
