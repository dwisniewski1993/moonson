-- Minimal WebSocket check (M1). Connects to --base-url (scheme swapped to
-- ws/wss), sends a message, reads the echo, and closes.
--
-- Reliable local test — start moonson's built-in echo server in one terminal:
--
--   cargo run --release -p moonson-cli -- serve-echo
--
-- then run the scenario against it in another:
--
--   cargo run --release -p moonson-cli -- run examples/ws_echo.luau \
--       --base-url http://127.0.0.1:9001 --vus 1 --duration 3s

scenario("ws_echo", function(vu)
  local ws = websocket.connect("/")
  local sent = "hello from moonson"
  ws:send(sent)
  local reply = ws:recv()
  check(reply, { ["echo matches"] = reply == sent })
  ws:close()
end)
