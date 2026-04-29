defmodule SootTelemetry.Credo.NoAuthorizeFalse do
  @moduledoc """
  Flags `authorize?: false` in production code.

  `authorize?: false` bypasses Ash policies. It is allowed only in
  test/, priv/repo/seeds/, scripts/, lib/**/demo/, and on lines
  tagged with `# authorize-bypass: <reason>`. Replace with an
  explicit actor (`SootTelemetry.Actors.system/1` or a caller-supplied
  Device / User actor).

  See umbrella `soot/POLICY-SPEC.md` §7 for the policy.
  """

  use Credo.Check,
    base_priority: :high,
    category: :warning,
    explanations: [
      check: """
      `authorize?: false` bypasses Ash policies. Replace with an
      explicit actor:

          # before:
          Ash.read(Resource, authorize?: false)

          # after:
          Ash.read(Resource, actor: MyApp.Actors.system(:registry_sync))

      If the bypass is genuinely necessary (bootstrap path before any
      actor exists, schema migration script, etc.), tag the line:

          # authorize-bypass: bootstrap loader runs before actor module compiles
          Ash.read(Resource, authorize?: false)

      The reason is required and is logged in CI.
      """
    ]

  @impl true
  def run(%SourceFile{} = source_file, params \\ []) do
    issue_meta = IssueMeta.for(source_file, params)

    if path_allowed?(source_file.filename) do
      []
    else
      bypass = ~r/authorize\?\s*:\s*false/
      tag = ~r/#\s*authorize-bypass\s*:\s*\S/

      source_file
      |> SourceFile.source()
      |> String.split("\n")
      |> Enum.with_index(1)
      |> Enum.reduce({false, []}, fn {line, lineno}, {in_heredoc, acc} ->
        next_in_heredoc = if heredoc_boundary?(line), do: not in_heredoc, else: in_heredoc

        cond do
          in_heredoc -> {next_in_heredoc, acc}
          Regex.match?(bypass, line) and not Regex.match?(tag, line) ->
            {next_in_heredoc, [issue_for(issue_meta, lineno, line) | acc]}
          true ->
            {next_in_heredoc, acc}
        end
      end)
      |> elem(1)
      |> Enum.reverse()
    end
  end

  defp heredoc_boundary?(line) do
    # A line opening or closing a `"""` heredoc. Exact start-and-end forms
    # like `"""foo"""` exist but are vanishingly rare in real Elixir; this
    # check toggles on any `"""` occurrence which is good enough.
    line |> String.split("\"\"\"") |> length() > 1
  end

  defp path_allowed?(nil), do: false

  defp path_allowed?(path) do
    patterns = [
      ~r{(^|/)test/},
      ~r{(^|/)priv/repo/seeds/},
      ~r{(^|/)scripts/},
      ~r{/demo/}
    ]

    Enum.any?(patterns, &Regex.match?(&1, path))
  end

  defp issue_for(issue_meta, lineno, line) do
    format_issue(
      issue_meta,
      message:
        "authorize?: false bypasses Ash policies — pass an explicit actor, or tag with `# authorize-bypass: <reason>`",
      line_no: lineno,
      trigger: String.trim(line)
    )
  end
end
