defmodule SootTelemetry.Plug.IngestTest do
  use ExUnit.Case, async: false
  import Plug.Test
  import Plug.Conn

  alias AshPki.Plug.MTLS.Actor
  alias SootTelemetry.{IngestSession, Plug.Ingest, RateLimiter, Registry, StreamRow}
  alias SootTelemetry.Test.Factories
  alias SootTelemetry.Test.Fixtures.Vibration

  setup do
    Factories.reset!()
    {:ok, %{schema: schema, stream: stream}} = Registry.register(Vibration)

    actor = %Actor{
      certificate_id: Ecto.UUID.generate(),
      issuer_id: nil,
      subject_dn: "/CN=device-001",
      serial: "1",
      fingerprint: "deadbeef",
      san: [],
      pem: "",
      raw_cert: nil
    }

    {:ok, schema: schema, stream: stream, actor: actor}
  end

  defp request(actor, stream_name, body, headers, opts \\ []) do
    conn(:post, "/ingest/#{stream_name}", body)
    |> Map.put(:path_params, %{"stream_name" => to_string(stream_name)})
    |> assign(:ash_pki_actor, actor)
    |> apply_headers(headers)
    |> Ingest.call(Ingest.init(opts))
  end

  defp apply_headers(conn, headers) do
    Enum.reduce(headers, conn, fn {k, v}, c -> put_req_header(c, k, v) end)
  end

  defp valid_headers(schema, seq_start \\ 0, seq_end \\ 9) do
    [
      {"x-stream", "vibration"},
      {"x-schema-fingerprint", schema.fingerprint},
      {"x-sequence-start", Integer.to_string(seq_start)},
      {"x-sequence-end", Integer.to_string(seq_end)},
      {"content-type", "application/octet-stream"}
    ]
  end

  describe "happy path" do
    test "204 + IngestSession update", ctx do
      conn = request(ctx.actor, :vibration, "fake-arrow-bytes", valid_headers(ctx.schema))

      assert conn.status == 204
      assert conn.resp_body == ""

      {:ok, session} =
        IngestSession.for_device_stream(ctx.actor.certificate_id, ctx.stream.id,
          authorize?: false
        )

      assert session.batch_count == 1
      assert session.byte_count == byte_size("fake-arrow-bytes")
      assert session.sequence_high_water == 9
      assert session.stream_name == :vibration
    end

    test "second batch advances counters", ctx do
      _ = request(ctx.actor, :vibration, "first", valid_headers(ctx.schema, 0, 9))
      conn = request(ctx.actor, :vibration, "second", valid_headers(ctx.schema, 10, 19))

      assert conn.status == 204

      {:ok, session} =
        IngestSession.for_device_stream(ctx.actor.certificate_id, ctx.stream.id,
          authorize?: false
        )

      assert session.batch_count == 2
      assert session.sequence_high_water == 19
    end
  end

  describe "rejection branches" do
    test "non-POST → 405", ctx do
      conn =
        conn(:get, "/ingest/vibration")
        |> Map.put(:path_params, %{"stream_name" => "vibration"})
        |> assign(:ash_pki_actor, ctx.actor)
        |> Ingest.call(Ingest.init([]))

      assert conn.status == 405
    end

    test "missing mTLS actor → 401", ctx do
      conn =
        conn(:post, "/ingest/vibration", "x")
        |> Map.put(:path_params, %{"stream_name" => "vibration"})
        |> apply_headers(valid_headers(ctx.schema))
        |> Ingest.call(Ingest.init([]))

      assert conn.status == 401
      assert Jason.decode!(conn.resp_body)["error"] == "mtls_required"
    end

    test "unknown stream → 404", ctx do
      headers = [
        {"x-stream", "no_such_stream"}
        | Enum.reject(valid_headers(ctx.schema), fn {k, _} -> k == "x-stream" end)
      ]

      conn =
        conn(:post, "/ingest/no_such_stream", "x")
        |> Map.put(:path_params, %{"stream_name" => "no_such_stream"})
        |> assign(:ash_pki_actor, ctx.actor)
        |> apply_headers(headers)
        |> Ingest.call(Ingest.init([]))

      assert conn.status == 404
      assert Jason.decode!(conn.resp_body)["error"] == "unknown_stream"
    end

    test "missing fingerprint header → 400", ctx do
      headers =
        Enum.reject(valid_headers(ctx.schema), fn {k, _} -> k == "x-schema-fingerprint" end)

      conn = request(ctx.actor, :vibration, "x", headers)

      assert conn.status == 400
      assert Jason.decode!(conn.resp_body)["error"] == "missing_fingerprint"
    end

    test "fingerprint mismatch → 409 with hint", ctx do
      headers =
        valid_headers(ctx.schema)
        |> Enum.reject(fn {k, _} -> k == "x-schema-fingerprint" end)
        |> Kernel.++([
          {"x-schema-fingerprint", "00" <> String.slice(ctx.schema.fingerprint, 2..-1//1)}
        ])

      conn = request(ctx.actor, :vibration, "x", headers)

      assert conn.status == 409
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "fingerprint_mismatch"
      assert body["expected"] == ctx.schema.fingerprint
      assert is_binary(body["hint"])
    end

    test "missing x-sequence-start → 400", ctx do
      headers = Enum.reject(valid_headers(ctx.schema), fn {k, _} -> k == "x-sequence-start" end)
      conn = request(ctx.actor, :vibration, "x", headers)

      assert conn.status == 400
      assert Jason.decode!(conn.resp_body)["error"] == "missing_header"
    end

    test "non-numeric sequence header → 400", ctx do
      headers =
        valid_headers(ctx.schema)
        |> Enum.reject(fn {k, _} -> k == "x-sequence-start" end)
        |> Kernel.++([{"x-sequence-start", "abc"}])

      conn = request(ctx.actor, :vibration, "x", headers)
      assert conn.status == 400
      assert Jason.decode!(conn.resp_body)["error"] == "invalid_header"
    end

    test "sequence regression → 409", ctx do
      # First batch establishes a high-water of 99.
      _ = request(ctx.actor, :vibration, "first", valid_headers(ctx.schema, 90, 99))

      # Second batch starts well below the grace window.
      conn = request(ctx.actor, :vibration, "second", valid_headers(ctx.schema, 0, 5))

      assert conn.status == 409
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "sequence_regression"
      assert body["seen_high_water"] == 99
    end

    test "sequence_end < sequence_start → 400", ctx do
      headers =
        valid_headers(ctx.schema)
        |> Enum.reject(fn {k, _} -> k in ["x-sequence-start", "x-sequence-end"] end)
        |> Kernel.++([{"x-sequence-start", "100"}, {"x-sequence-end", "50"}])

      conn = request(ctx.actor, :vibration, "x", headers)
      assert conn.status == 400
      assert Jason.decode!(conn.resp_body)["error"] == "sequence_end_before_start"
    end

    test "rate-limited → 429 with retry-after header", ctx do
      RateLimiter.reset_all()

      opts = [device_stream: %{capacity: 1.0, refill_per_second: 0.0}]

      assert request(ctx.actor, :vibration, "first", valid_headers(ctx.schema, 0, 1), opts).status ==
               204

      conn = request(ctx.actor, :vibration, "second", valid_headers(ctx.schema, 2, 3), opts)
      assert conn.status == 429
      assert Jason.decode!(conn.resp_body)["error"] == "rate_limited"
      assert get_resp_header(conn, "retry-after") != []
    end

    test "paused stream → 423", ctx do
      {:ok, _} = StreamRow.pause(ctx.stream)

      conn = request(ctx.actor, :vibration, "x", valid_headers(ctx.schema))
      assert conn.status == 423
      assert Jason.decode!(conn.resp_body)["error"] == "stream_unavailable"
    end

    test "retired stream → 423 with status retired", ctx do
      {:ok, _} = StreamRow.retire(ctx.stream)

      conn = request(ctx.actor, :vibration, "x", valid_headers(ctx.schema))
      assert conn.status == 423
      body = Jason.decode!(conn.resp_body)
      assert body["error"] == "stream_unavailable"
      assert body["status"] == "retired"
    end

    test "stream pointing at a missing schema → 500 no_active_schema", ctx do
      {:ok, _} =
        Ash.update(ctx.stream, %{current_schema_id: Ecto.UUID.generate()},
          action: :update,
          authorize?: false
        )

      conn = request(ctx.actor, :vibration, "x", valid_headers(ctx.schema))
      assert conn.status == 500
      assert Jason.decode!(conn.resp_body)["error"] == "no_active_schema"
    end

    test "writer-side error surfaces as a structured 500", ctx do
      defmodule FailingWriter do
        @behaviour SootTelemetry.Writer
        def write(_), do: {:error, :downstream_unavailable}
      end

      Application.put_env(:soot_telemetry, :writer, FailingWriter)
      on_exit(fn -> Application.delete_env(:soot_telemetry, :writer) end)

      conn = request(ctx.actor, :vibration, "x", valid_headers(ctx.schema))
      assert conn.status == 500
      assert Jason.decode!(conn.resp_body)["error"] == "writer_error"
    end

    test "body exceeding max_body_bytes → 413", ctx do
      body = String.duplicate("x", 200)

      conn =
        request(ctx.actor, :vibration, body, valid_headers(ctx.schema), max_body_bytes: 64)

      assert conn.status == 413
      assert Jason.decode!(conn.resp_body)["error"] == "body_too_large"
    end

    test "rate-limited with zero refill returns retry-after = 3600", ctx do
      RateLimiter.reset_all()
      opts = [device_stream: %{capacity: 1.0, refill_per_second: 0.0}]

      assert request(ctx.actor, :vibration, "ok", valid_headers(ctx.schema, 0, 1), opts).status ==
               204

      conn = request(ctx.actor, :vibration, "again", valid_headers(ctx.schema, 2, 3), opts)
      assert conn.status == 429
      assert get_resp_header(conn, "retry-after") == ["3600"]
    end
  end
end
