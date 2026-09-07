-- gRPC bidirectional streaming (M2, step 2): open one stream, then send and
-- receive many messages over it — the streaming pattern k6 handles awkwardly.
-- Run against moonson's built-in gRPC echo server:
--
--   cargo run --release -p moonson-cli -- serve-grpc
--   cargo run --release -p moonson-cli -- run examples/grpc_stream.luau \
--       --vus 5 --duration 5s

scenario("grpc_stream", function(vu)
  local c = grpc.connect("http://127.0.0.1:50051")
  local s = c:stream()
  for i = 1, 20 do
    s:send("tick " .. i)
    local reply = s:recv()
    check(reply, { ["echo matches"] = reply == ("tick " .. i) })
    think(0.02)
  end
  s:close()
end)
