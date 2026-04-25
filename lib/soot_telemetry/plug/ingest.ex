defmodule SootTelemetry.Plug.Ingest do
  @moduledoc """
  `POST /ingest/:stream_name` — telemetry batch endpoint.

  Mount behind `AshPki.Plug.MTLS`:

      forward "/ingest/:stream_name", to: Plug.Builder.compile([
        {AshPki.Plug.MTLS, [require_known_certificate: true]},
        SootTelemetry.Plug.Ingest
      ])

  ## Required headers

    * `x-stream`              — must match the path's `stream_name`.
    * `x-schema-fingerprint`  — must match the stream's active schema's
                                fingerprint; otherwise the response is
                                `409` with the expected fingerprint and
                                a hint URL pointing the device at a
                                contract-bundle re-fetch.
    * `x-sequence-start`, `x-sequence-end` — monotonic integers; a
                                regression beyond a small grace window
                                yields `409`.

  ## Rejection branches

    * `401` — no mTLS actor on the conn.
    * `400` — required header missing or malformed.
    * `404` — unknown stream name.
    * `409` — fingerprint mismatch *or* sequence regression.
    * `423` — stream paused or retired.
    * `429` — per-device or per-tenant rate-limit exhausted.
    * `500` — writer error.

  Successful inserts respond `204` (no body) and update the
  per-(device, stream) `IngestSession` row.
  """

  @behaviour Plug
  import Plug.Conn
  require Logger

  alias AshPki.Plug.MTLS.Actor
  alias SootTelemetry.{IngestSession, RateLimiter, Schema, StreamRow, Writer}

  @max_body_bytes 16 * 1024 * 1024
  @sequence_grace_window 16

  @impl true
  def init(opts), do: opts

  @impl true
  def call(%Plug.Conn{method: "POST"} = conn, opts) do
    stream_name = path_stream(conn) || header(conn, "x-stream")

    with {:ok, %Actor{} = actor} <- fetch_actor(conn),
         {:ok, stream_atom} <- resolve_stream_name(stream_name),
         {:ok, stream} <- load_stream(stream_atom),
         :ok <- ensure_active(stream),
         {:ok, schema} <- load_active_schema(stream),
         :ok <- check_fingerprint(conn, schema),
         {:ok, seq_start, seq_end} <- read_sequence_headers(conn),
         :ok <- check_sequence(actor, stream, seq_start),
         :ok <- check_rate_limits(actor, stream, opts),
         {:ok, body} <- read_request_body(conn),
         :ok <- write_batch(body, stream, schema, actor, seq_start, seq_end) do
      record_session(actor, stream, byte_size(body), seq_end)

      send_resp(conn, 204, "") |> halt()
    else
      {:error, reason} -> error_response(conn, reason)
    end
  end

  def call(conn, _opts), do: error_response(conn, :method_not_allowed)

  # ─── lookups ────────────────────────────────────────────────────────────

  defp path_stream(%Plug.Conn{path_params: %{"stream_name" => name}}), do: name
  defp path_stream(_), do: nil

  defp header(conn, name) do
    case get_req_header(conn, name) do
      [v | _] -> v
      _ -> nil
    end
  end

  defp fetch_actor(%Plug.Conn{assigns: %{ash_pki_actor: %Actor{} = actor}}), do: {:ok, actor}
  defp fetch_actor(_), do: {:error, :missing_mtls_actor}

  defp resolve_stream_name(nil), do: {:error, :missing_stream_name}

  defp resolve_stream_name(name) when is_binary(name) do
    {:ok, String.to_existing_atom(name)}
  rescue
    ArgumentError -> {:error, {:unknown_stream, name}}
  end

  defp load_stream(name) do
    case StreamRow.get_by_name(name, authorize?: false) do
      {:ok, %StreamRow{} = stream} -> {:ok, stream}
      _ -> {:error, {:unknown_stream, name}}
    end
  end

  defp ensure_active(%StreamRow{status: :active}), do: :ok
  defp ensure_active(%StreamRow{status: status}), do: {:error, {:stream_unavailable, status}}

  defp load_active_schema(%StreamRow{current_schema_id: id}) do
    case Ash.get(Schema, id, authorize?: false) do
      {:ok, %Schema{} = schema} -> {:ok, schema}
      _ -> {:error, :no_active_schema}
    end
  end

  # ─── header validation ─────────────────────────────────────────────────

  defp check_fingerprint(conn, %Schema{fingerprint: expected}) do
    case header(conn, "x-schema-fingerprint") do
      nil ->
        {:error, :missing_fingerprint_header}

      ^expected ->
        :ok

      provided ->
        {:error, {:fingerprint_mismatch, %{expected: expected, provided: provided}}}
    end
  end

  defp read_sequence_headers(conn) do
    with {:ok, start} <- parse_int_header(conn, "x-sequence-start"),
         {:ok, finish} <- parse_int_header(conn, "x-sequence-end") do
      if finish < start do
        {:error, :sequence_end_before_start}
      else
        {:ok, start, finish}
      end
    end
  end

  defp parse_int_header(conn, name) do
    case header(conn, name) do
      nil ->
        {:error, {:missing_header, name}}

      raw ->
        case Integer.parse(raw) do
          {n, ""} when n >= 0 -> {:ok, n}
          _ -> {:error, {:invalid_header, name, raw}}
        end
    end
  end

  defp check_sequence(actor, stream, seq_start) do
    case IngestSession.for_device_stream(actor.certificate_id, stream.id, authorize?: false) do
      {:ok, %IngestSession{sequence_high_water: high_water}}
      when seq_start + @sequence_grace_window < high_water ->
        {:error, {:sequence_regression, %{seen: high_water, got: seq_start}}}

      _ ->
        :ok
    end
  end

  # ─── rate limits ───────────────────────────────────────────────────────

  defp check_rate_limits(actor, stream, opts) do
    device_key = {:device_stream, actor.certificate_id, stream.id}
    tenant_key = {:tenant_stream, tenant_id_from_actor(actor) || "no-tenant", stream.id}

    with {:ok, _} <- RateLimiter.take(device_key, 1, opts),
         {:ok, _} <- RateLimiter.take(tenant_key, 1, opts) do
      :ok
    else
      {:rate_limited, %{retry_after_ms: ms}} -> {:error, {:rate_limited, ms}}
    end
  end

  # Convention: the SPIFFE-style URI SAN encodes the tenant slug —
  # `URI:device://<tenant>/devices/<serial>`. If the device's cert
  # doesn't follow that, fall back to nil.
  defp tenant_id_from_actor(%Actor{san: san}) when is_list(san) do
    Enum.find_value(san, fn
      {:uniformResourceIdentifier, charlist} ->
        case String.split(List.to_string(charlist), "/") do
          ["device:", "", tenant | _] -> tenant
          _ -> nil
        end

      _ ->
        nil
    end)
  end

  defp tenant_id_from_actor(_), do: nil

  # ─── body + side effects ──────────────────────────────────────────────

  defp read_request_body(conn) do
    case Plug.Conn.read_body(conn, length: @max_body_bytes) do
      {:ok, body, _conn} -> {:ok, body}
      {:more, _, _} -> {:error, :body_too_large}
      {:error, _} -> {:error, :body_read_failed}
    end
  end

  defp write_batch(body, stream, schema, actor, seq_start, seq_end) do
    case Writer.write(%{
           body: body,
           stream: stream.name,
           fingerprint: schema.fingerprint,
           sequence_start: seq_start,
           sequence_end: seq_end,
           device_id: actor.fingerprint,
           tenant_id: tenant_id_from_actor(actor),
           received_at: DateTime.utc_now()
         }) do
      :ok -> :ok
      {:error, reason} -> {:error, {:writer_error, reason}}
      other -> {:error, {:writer_error, other}}
    end
  end

  defp record_session(actor, stream, bytes, seq_end) do
    case IngestSession.for_device_stream(actor.certificate_id, stream.id, authorize?: false) do
      {:ok, %IngestSession{} = session} ->
        log_session_error(IngestSession.record_batch(session, bytes, seq_end, authorize?: false))

      _ ->
        IngestSession.create(
          actor.certificate_id,
          tenant_id_from_actor(actor),
          stream.id,
          stream.name,
          DateTime.utc_now(),
          authorize?: false
        )
        |> case do
          {:ok, session} ->
            log_session_error(
              IngestSession.record_batch(session, bytes, seq_end, authorize?: false)
            )

          err ->
            log_session_error(err)
        end
    end
  end

  defp log_session_error({:ok, _} = ok), do: ok

  defp log_session_error({:error, reason} = err) do
    Logger.warning(fn ->
      "soot_telemetry: IngestSession write failed: " <> inspect(reason)
    end)

    err
  end

  # ─── responses ─────────────────────────────────────────────────────────

  defp error_response(conn, reason) do
    {status, code, headers, body} = response_for(reason)

    conn = Enum.reduce(headers, conn, fn {k, v}, c -> put_resp_header(c, k, v) end)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(Map.put(body, :error, code)))
    |> halt()
  end

  defp response_for(:method_not_allowed), do: {405, "method_not_allowed", [], %{}}
  defp response_for(:missing_mtls_actor), do: {401, "mtls_required", [], %{}}
  defp response_for(:missing_stream_name), do: {400, "missing_stream_name", [], %{}}

  defp response_for({:unknown_stream, name}),
    do: {404, "unknown_stream", [], %{stream: to_string(name)}}

  defp response_for({:stream_unavailable, status}),
    do: {423, "stream_unavailable", [], %{status: Atom.to_string(status)}}

  defp response_for(:no_active_schema), do: {500, "no_active_schema", [], %{}}
  defp response_for(:missing_fingerprint_header), do: {400, "missing_fingerprint", [], %{}}

  defp response_for({:fingerprint_mismatch, %{expected: e, provided: p}}),
    do:
      {409, "fingerprint_mismatch", [],
       %{
         expected: e,
         provided: p,
         hint: "GET /.well-known/soot/contract for the current schema descriptor"
       }}

  defp response_for({:missing_header, name}), do: {400, "missing_header", [], %{header: name}}

  defp response_for({:invalid_header, name, raw}),
    do: {400, "invalid_header", [], %{header: name, value: raw}}

  defp response_for(:sequence_end_before_start),
    do: {400, "sequence_end_before_start", [], %{}}

  defp response_for({:sequence_regression, %{seen: seen, got: got}}),
    do: {409, "sequence_regression", [], %{seen_high_water: seen, batch_start: got}}

  defp response_for({:rate_limited, ms}),
    do: {429, "rate_limited", [{"retry-after", retry_after_seconds(ms)}], %{retry_after_ms: ms}}

  defp response_for(:body_too_large), do: {413, "body_too_large", [], %{}}
  defp response_for(:body_read_failed), do: {400, "body_read_failed", [], %{}}

  defp response_for({:writer_error, reason}) do
    Logger.error(fn -> "soot_telemetry: writer rejected batch: " <> inspect(reason) end)
    {500, "writer_error", [], %{}}
  end

  defp response_for(_), do: {500, "internal_error", [], %{}}

  defp retry_after_seconds(:infinity), do: "3600"
  defp retry_after_seconds(ms), do: max(1, div(ms, 1_000)) |> Integer.to_string()
end
