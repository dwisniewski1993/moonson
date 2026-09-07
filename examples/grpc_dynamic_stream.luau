-- Dynamic gRPC bidirectional streaming (M2, final): open a stream to a method
-- described by a runtime-loaded .proto, then send/recv many messages built from
-- Lua tables. Run against moonson's gRPC echo server:
--
--   cargo run --release -p moonson-cli -- serve-grpc
--   cargo run --release -p moonson-cli -- run examples/grpc_dynamic_stream.luau --vus 5 --duration 5s

scenario("grpc_dynamic_stream", function(vu)
  local c = grpc.dial("http://127.0.0.1:50051", "proto/echo.proto")
  local s = c:stream("echo.Echo/Stream")
  for i = 1, 20 do
    s:send({ text = "tick " .. i })
    local reply = s:recv()
    check(reply, { ["echo matches"] = reply ~= nil and reply.text == ("tick " .. i) })
    think(0.02)
  end
  s:close()
end)
