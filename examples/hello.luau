-- Step 5 scenario: one GET per iteration. With `run --vus N --duration D` this
-- gets looped by N virtual users, and the aggregate report shows throughput and
-- latency percentiles. No per-request printing — it would flood at high VU
-- counts. Run it with:
--
--   cargo run --release -p moonson-cli -- run examples/hello.luau --vus 50 --duration 10s
--   cargo run --release -p moonson-cli -- run examples/hello.luau --base-url http://127.0.0.1:8000 --vus 50 --duration 10s

scenario("hello", function(vu)
  http.get("/")
end)
