-- A long-lived WebSocket stream: connect ONCE and exchange many messages over
-- the same socket — the stateful pattern streaming load tests need. Unlike HTTP,
-- you do not reconnect per message; churning short connections would just
-- exhaust local ports (TIME_WAIT). Run against the built-in echo server:
--
--   cargo run --release -p moonson-cli -- serve-echo
--   cargo run --release -p moonson-cli -- run examples/ws_stream.luau \
--       --base-url http://127.0.0.1:9001 --vus 10 --duration 5s

scenario("ws_stream", function(vu)
  local ws = websocket.connect("/")
  for i = 1, 50 do
    ws:send("ping " .. i)
    local reply = ws:recv()
    check(reply, { ["got a reply"] = reply ~= nil })
    think(0.02) -- ~50 messages/second per VU
  end
  ws:close()
end)
