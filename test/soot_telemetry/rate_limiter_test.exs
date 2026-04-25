defmodule SootTelemetry.RateLimiterTest do
  use ExUnit.Case, async: false

  alias SootTelemetry.RateLimiter

  setup do
    RateLimiter.reset_all()
    :ok
  end

  describe "take/3" do
    test "succeeds while tokens remain and exhausts cleanly" do
      key = {:device_stream, "d1", "s1"}
      # Non-zero refill so retry_after_ms is a finite integer.
      opts = [device_stream: %{capacity: 3.0, refill_per_second: 1.0}]

      for _ <- 1..3 do
        assert {:ok, _} = RateLimiter.take(key, 1, opts)
      end

      assert {:rate_limited, %{retry_after_ms: ms}} = RateLimiter.take(key, 1, opts)
      assert is_integer(ms) and ms >= 1
    end

    test "refill_per_second of 0 returns :infinity for retry_after_ms" do
      key = {:device_stream, "d-zero", "s1"}
      opts = [device_stream: %{capacity: 1.0, refill_per_second: 0.0}]

      assert {:ok, _} = RateLimiter.take(key, 1, opts)
      assert {:rate_limited, %{retry_after_ms: :infinity}} = RateLimiter.take(key, 1, opts)
    end

    test "tokens refill over time (using a fast refill rate)" do
      key = {:device_stream, "d2", "s1"}
      # Empty bucket, fast refill — wait a tick, take again.
      opts = [device_stream: %{capacity: 1.0, refill_per_second: 1_000.0}]

      assert {:ok, _} = RateLimiter.take(key, 1, opts)
      assert {:rate_limited, _} = RateLimiter.take(key, 1, opts)

      Process.sleep(20)
      assert {:ok, _} = RateLimiter.take(key, 1, opts)
    end

    test "different keys have independent buckets" do
      a = {:device_stream, "d1", "s1"}
      b = {:device_stream, "d2", "s1"}
      opts = [device_stream: %{capacity: 1.0, refill_per_second: 0.0}]

      assert {:ok, _} = RateLimiter.take(a, 1, opts)
      # Bucket a is empty.
      assert {:rate_limited, _} = RateLimiter.take(a, 1, opts)
      # Bucket b is still full.
      assert {:ok, _} = RateLimiter.take(b, 1, opts)
    end

    test "tenant_stream key uses its own configured limit" do
      tenant_key = {:tenant_stream, "t1", "s1"}
      opts = [tenant_stream: %{capacity: 2.0, refill_per_second: 0.0}]

      assert {:ok, _} = RateLimiter.take(tenant_key, 1, opts)
      assert {:ok, _} = RateLimiter.take(tenant_key, 1, opts)
      assert {:rate_limited, _} = RateLimiter.take(tenant_key, 1, opts)
    end

    test "cost > 1 deducts that many tokens" do
      key = {:device_stream, "d1", "s1"}
      opts = [device_stream: %{capacity: 5.0, refill_per_second: 0.0}]

      assert {:ok, %{remaining: 2.0}} = RateLimiter.take(key, 3, opts)
      assert {:rate_limited, _} = RateLimiter.take(key, 3, opts)
    end
  end

  describe "reset/1" do
    test "drops the bucket and the next take starts full" do
      key = {:device_stream, "d1", "s1"}
      opts = [device_stream: %{capacity: 1.0, refill_per_second: 0.0}]

      assert {:ok, _} = RateLimiter.take(key, 1, opts)
      assert {:rate_limited, _} = RateLimiter.take(key, 1, opts)

      RateLimiter.reset(key)

      assert {:ok, _} = RateLimiter.take(key, 1, opts)
    end
  end

  describe "config sources" do
    test "falls back to application env when no inline opts" do
      Application.put_env(:soot_telemetry, :rate_limits,
        device_stream: [capacity: 1, refill_per_second: 0]
      )

      key = {:device_stream, "d-app-env", "s1"}
      assert {:ok, _} = RateLimiter.take(key)
      assert {:rate_limited, _} = RateLimiter.take(key)
    after
      Application.delete_env(:soot_telemetry, :rate_limits)
    end
  end
end
