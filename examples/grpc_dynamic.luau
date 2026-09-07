-- Dynamic gRPC (M2): load a .proto at run time and call a method built from a
-- Lua table — this works against ANY service, not just one compiled into
-- moonson. Here it talks to moonson's own gRPC echo server:
--
--   cargo run --release -p moonson-cli -- serve-grpc
--   cargo run --release -p moonson-cli -- run examples/grpc_dynamic.luau --vus 5 --duration 5s

scenario("grpc_dynamic", function(vu)
  local c = grpc.dial("http://127.0.0.1:50051", "proto/echo.proto")
  for i = 1, 50 do
    local reply = c:unary("echo.Echo/Unary", { text = "ping " .. i })
    check(reply, { ["echo matches"] = reply.text == ("ping " .. i) })
    think(0.02)
  end
end)
