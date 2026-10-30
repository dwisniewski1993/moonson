-- Data-driven scenario (M4): read rows from a CSV once, then each iteration
-- takes the next row (round-robin across the worker's VUs). Run against the
-- fast bench target (which answers any request with 200):
--
--   cargo run --release -p moonson-cli -- serve-bench
--   cargo run --release -p moonson-cli -- run examples/data_feed.luau \
--       --base-url http://127.0.0.1:8080 --vus 3 --duration 3s

-- data.open runs once, at the top level, so the CSV is read a single time.
local users = data.open("examples/users.csv")

scenario("login", function(vu)
  local u = users:next()
  local r = http.post("/login", { json = { user = u.user, pass = u.pass } })
  check(r, { ["status 200"] = r.status == 200 })
end)
