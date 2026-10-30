-- Shows that a failing iteration is counted, not fatal: this scenario raises an
-- error on every other iteration, yet the whole run completes and the report
-- shows an "iteration errors" count. Run against the bench target:
--
--   cargo run --release -p moonson-cli -- serve-bench
--   cargo run --release -p moonson-cli -- run examples/flaky.luau \
--       --base-url http://127.0.0.1:8080 --vus 4 --duration 2s

local n = 0

scenario("flaky", function(vu)
  http.get("/")
  n = n + 1
  if n % 2 == 0 then
    error("boom") -- deliberate failure on every other iteration
  end
end)
