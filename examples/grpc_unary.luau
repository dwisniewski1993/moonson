-- Minimal gRPC check (M2, step 1). Connects once and makes many unary calls
-- over the same channel. Run against moonson's built-in gRPC echo server:
--
--   cargo run --release -p moonson-cli -- serve-grpc
--   cargo run --release -p moonson-cli -- run examples/grpc_unary.luau \
--       --vus 5 --duration 5s
--
-- (The gRPC address is given directly to grpc.connect, independent of
-- --base-url, since gRPC runs on its own port.)

scenario("grpc_unary", function(vu)
  local c = grpc.connect("http://127.0.0.1:50051")
  for i = 1, 50 do
    local reply = c:unary("ping " .. i)
    check(reply, { ["echo matches"] = reply == ("ping " .. i) })
    think(0.02)
  end
end)
