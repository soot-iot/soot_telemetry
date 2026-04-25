defmodule SootTelemetry.IngestSession.Changes.ClampSequenceHighWater do
  @moduledoc """
  Clamps `sequence_high_water` on an `IngestSession` to the monotone
  maximum of the existing value and the incoming `sequence_end`
  argument. This stops an in-grace late batch from rolling the
  high-water value backward, which would otherwise reopen replay
  headroom for the next request.
  """

  use Ash.Resource.Change

  @impl true
  def change(changeset, _opts, _context) do
    Ash.Changeset.before_action(changeset, &clamp/1)
  end

  defp clamp(changeset) do
    sequence_end = Ash.Changeset.get_argument(changeset, :sequence_end)
    current = changeset.data.sequence_high_water || 0

    Ash.Changeset.force_change_attribute(
      changeset,
      :sequence_high_water,
      max(current, sequence_end)
    )
  end
end
