-- HTTP and WebSocket in the SAME virtual user, interleaved — the case most
-- load tools handle awkwardly (k6, for instance, can't cleanly mix its
-- WebSocket and HTTP APIs in one script). moonson runs both on one coroutine.
--
-- Run against moonson's dual-protocol echo server (answers plain HTTP with 200
-- and upgrades WebSocket requests to an echo):
--
--   cargo run --release -p moonson-cli -- serve-echo
--   cargo run --release -p moonson-cli -- run examples/mixed.luau \
--       --base-url http://127.0.0.1:9001 --vus 5 --duration 5s

scenario("mixed", function(vu)
  -- 1) an HTTP call, e.g. "log in"
  local auth = http.get("/login")
  check(auth, { ["login ok"] = auth.status == 200 })

  -- 2) open a WebSocket and stream on it
  local ws = websocket.connect("/stream")
  for i = 1, 10 do
    ws:send("tick " .. i)
    local msg = ws:recv()

    -- 3) react to a WS message with an HTTP call — HTTP and WS interleaved
    if msg ~= nil then
      local ack = http.post("/ack", { json = { seq = i } })
      check(ack, { ["ack ok"] = ack.status == 200 })
    end

    think(0.05)
  end
  ws:close()
end)
