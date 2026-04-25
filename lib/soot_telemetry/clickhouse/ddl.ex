defmodule SootTelemetry.ClickHouse.DDL do
  @moduledoc """
  Render `CREATE TABLE` statements for telemetry streams.

  Type mapping:

      :int8         → Int8
      :int16        → Int16
      :int32        → Int32
      :int64        → Int64
      :uint8        → UInt8
      :uint16       → UInt16
      :uint32       → UInt32
      :uint64       → UInt64
      :float32      → Float32
      :float64      → Float64
      :bool         → UInt8
      :string       → String  (or LowCardinality(String) when dictionary: true)
      :binary       → String
      :timestamp_us → DateTime64(6, 'UTC')
      :timestamp_ms → DateTime64(3, 'UTC')
      :timestamp_s  → DateTime
      :date32       → Date

  `required: false` columns wrap the inner type in `Nullable(_)`.

  Engine, ORDER BY, PARTITION BY, and TTL come from the `clickhouse` block;
  retention falls back to a TTL clause derived from the `retention`
  keyword (e.g. `[months: 12]`).
  """

  alias SootTelemetry.Stream.{Field, Info}

  @doc """
  Render the full `CREATE TABLE IF NOT EXISTS` statement for a stream
  module.
  """
  @spec create_table(module(), keyword()) :: String.t()
  def create_table(module, opts \\ []) do
    table = Keyword.get(opts, :table, default_table_name(module))
    db = Keyword.get(opts, :database)
    fully_qualified = if db, do: "#{db}.#{table}", else: table

    column_lines =
      module
      |> Info.fields()
      |> Enum.map(&render_column/1)
      |> Enum.map(&("    " <> &1))
      |> Enum.join(",\n")

    ch = Info.clickhouse(module)
    engine = ch.engine || "MergeTree"

    order_by =
      ch.order_by
      |> Enum.map(&Atom.to_string/1)
      |> Enum.join(", ")

    base = """
    CREATE TABLE IF NOT EXISTS #{fully_qualified} (
    #{column_lines}
    )
    ENGINE = #{engine}
    ORDER BY (#{order_by})\
    """

    base
    |> append_partition_by(ch.partition_by)
    |> append_ttl(ch.ttl || ttl_from_retention(Info.retention(module)))
    |> append_settings(ch.settings)
    |> Kernel.<>(";")
  end

  @doc "Render `CREATE TABLE …` for every module in a list, joined by blank lines."
  @spec create_tables([module()], keyword()) :: String.t()
  def create_tables(modules, opts \\ []) do
    modules
    |> Enum.map(&create_table(&1, opts))
    |> Enum.join("\n\n")
  end

  @doc """
  Diff two schema descriptors (as stored on `SootTelemetry.Schema`) and
  return a list of `ALTER TABLE` statements describing the additive
  changes (new columns only). Anything that isn't additive returns
  `{:error, :non_additive}` so the operator is forced to handle it
  explicitly.
  """
  @spec alter_for_descriptor_change(String.t(), map(), map()) ::
          {:ok, [String.t()]} | {:error, :non_additive}
  def alter_for_descriptor_change(table, %{"fields" => from} = _from_desc, %{"fields" => to} = _to_desc) do
    diff(from, to, table)
  end

  def alter_for_descriptor_change(table, %{fields: from}, %{fields: to}),
    do: diff(from, to, table)

  defp diff(from, to, table) do
    from_names = MapSet.new(from, &normalize_name/1)
    added = Enum.filter(to, &(not MapSet.member?(from_names, normalize_name(&1))))

    removed_or_changed =
      Enum.any?(from, fn old ->
        case Enum.find(to, &(normalize_name(&1) == normalize_name(old))) do
          nil -> true
          new -> not field_compatible?(old, new)
        end
      end)

    if removed_or_changed do
      {:error, :non_additive}
    else
      stmts =
        added
        |> Enum.map(fn f ->
          "ALTER TABLE #{table} ADD COLUMN #{render_column(struct_from_descriptor(f))};"
        end)

      {:ok, stmts}
    end
  end

  # ─── helpers ───────────────────────────────────────────────────────────

  defp default_table_name(module) do
    "telemetry_" <> Atom.to_string(Info.name(module))
  end

  defp render_column(%Field{} = f) do
    base = clickhouse_type(f.type)
    typed = if f.dictionary, do: "LowCardinality(#{base})", else: base
    typed = if f.required, do: typed, else: "Nullable(#{typed})"
    Atom.to_string(f.name) <> " " <> typed
  end

  defp render_column(%{name: name, type: type, required: req?, dictionary: dict?}),
    do: render_column(%Field{name: name, type: type, required: req?, dictionary: dict?})

  defp clickhouse_type(:int8), do: "Int8"
  defp clickhouse_type(:int16), do: "Int16"
  defp clickhouse_type(:int32), do: "Int32"
  defp clickhouse_type(:int64), do: "Int64"
  defp clickhouse_type(:uint8), do: "UInt8"
  defp clickhouse_type(:uint16), do: "UInt16"
  defp clickhouse_type(:uint32), do: "UInt32"
  defp clickhouse_type(:uint64), do: "UInt64"
  defp clickhouse_type(:float32), do: "Float32"
  defp clickhouse_type(:float64), do: "Float64"
  defp clickhouse_type(:bool), do: "UInt8"
  defp clickhouse_type(:string), do: "String"
  defp clickhouse_type(:binary), do: "String"
  defp clickhouse_type(:timestamp_us), do: "DateTime64(6, 'UTC')"
  defp clickhouse_type(:timestamp_ms), do: "DateTime64(3, 'UTC')"
  defp clickhouse_type(:timestamp_s), do: "DateTime"
  defp clickhouse_type(:date32), do: "Date"

  defp append_partition_by(sql, nil), do: sql
  defp append_partition_by(sql, expr), do: sql <> "\nPARTITION BY " <> expr

  defp append_ttl(sql, nil), do: sql
  defp append_ttl(sql, expr), do: sql <> "\nTTL " <> expr

  defp append_settings(sql, []), do: sql

  defp append_settings(sql, settings) do
    rendered =
      settings
      |> Enum.map(fn {k, v} -> "#{k} = #{format_setting(v)}" end)
      |> Enum.join(", ")

    sql <> "\nSETTINGS " <> rendered
  end

  defp format_setting(v) when is_binary(v), do: ~s('#{v}')
  defp format_setting(v), do: to_string(v)

  defp ttl_from_retention(nil), do: nil
  defp ttl_from_retention([]), do: nil

  defp ttl_from_retention(retention) when is_list(retention) do
    case retention do
      [{:days, n}] -> "ts + INTERVAL #{n} DAY"
      [{:months, n}] -> "ts + INTERVAL #{n} MONTH"
      [{:years, n}] -> "ts + INTERVAL #{n} YEAR"
      _ -> nil
    end
  end

  defp normalize_name(%Field{name: n}), do: to_string(n)
  defp normalize_name(%{name: n}), do: to_string(n)
  defp normalize_name(%{"name" => n}), do: to_string(n)

  defp field_compatible?(old, new) do
    fetch(old, :type) == fetch(new, :type) and
      fetch(old, :required) == fetch(new, :required) and
      fetch(old, :dictionary) == fetch(new, :dictionary)
  end

  defp fetch(map, key) when is_map(map) do
    Map.get(map, key) ||
      Map.get(map, to_string(key)) ||
      Map.get(map, Atom.to_string(key))
  end

  defp struct_from_descriptor(%{"name" => name, "type" => type} = m) do
    %Field{
      name: maybe_atom(name),
      type: maybe_atom(type),
      required: Map.get(m, "required", false),
      dictionary: Map.get(m, "dictionary", false),
      server_set: Map.get(m, "server_set", false),
      monotonic: Map.get(m, "monotonic", false)
    }
  end

  defp struct_from_descriptor(%Field{} = f), do: f
  defp struct_from_descriptor(%{name: _} = m), do: struct(Field, m)

  defp maybe_atom(v) when is_atom(v), do: v
  defp maybe_atom(v) when is_binary(v), do: String.to_atom(v)
end
