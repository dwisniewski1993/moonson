-- A realistic-looking flow that actually runs against the default base URL
-- (https://httpbin.org). All three endpoints return 200, so the check passes.
--
--   cargo run --release -p moonson-cli -- run examples/smoke.luau --vus 5 --duration 10s
--
-- Note: think(1) paces each virtual user to ~1 iteration/second, so this
-- demonstrates the DSL rather than raw throughput.

scenario("login_flow", function(vu)
  http.get("/get")

  local r = http.post("/post", {
    json = { user = "test", pass = "123" },
    headers = { ["X-Demo"] = "moonson" },
  })
  check(r, { ["auth status is 200"] = r.status == 200 })

  think(1)

  http.get("/status/200")
end)
