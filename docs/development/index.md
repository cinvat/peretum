---
label: Development
icon: code
order: 100
---

# Development

Everything you need to extend Peretum: repo layout, the testing bar, and how
to add new plugins and targets.

- [Project layout](layout.md)
- [Testing](testing.md)
- [Adding a plugin](adding-a-plugin.md)
- [Contributing](contributing.md)

## Quick dev loop

```bash
go build ./...                                # compile everything
go run .                                      # run the proxy against config.yaml/config.d/
go run ./cmd/testserver                       # upstream test server on :8082
```

Tests bind fixed ports (`:8081`, `:8082`, `:9090`) — stop any proxy /
testserver / metrics process on those before running the suite, or
port-conflict tests skip.