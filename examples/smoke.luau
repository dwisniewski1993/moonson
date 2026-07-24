-- The scenario we are building toward. None of this runs yet — it is the target
-- shape of the DSL, kept here so the docs and the code stay honest about the
-- goal. Steps 3-5 make it real.

scenario("login_flow", function(vu)
  http.get("/login")

  local r = http.post("/auth", { json = { user = "test", pass = "123" } })
  check(r, { ["status is 200"] = r.status == 200 })

  think(1) -- pretend the user reads the page for a second

  http.get("/dashboard")
end)
