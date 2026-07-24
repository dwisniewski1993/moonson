-- A minimal scenario for Step 3. `http.get` is still a stub that only logs the
-- call — real asynchronous requests arrive in Step 4. Run it with:
--
--   cargo run -p moonson-cli -- run examples/hello.luau

scenario("hello", function(vu)
  http.get("/login")
  http.get("/dashboard")
end)
