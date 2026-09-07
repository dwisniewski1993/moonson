-- Throughput scenario for the density benchmark: one GET, no think time, looped
-- as fast as possible. Point it at `moonson serve-bench`:
--
--   cargo run --release -p moonson-cli -- serve-bench
--   ./target/release/moonson run bench/bench.luau \
--       --base-url http://127.0.0.1:8080 --vus 50 --duration 20s

scenario("bench", function(vu)
  http.get("/")
end)
