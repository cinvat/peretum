---
label: Testing
order: 300
---

# Testing

**New code requires matching tests, never a relaxed bar.** CI fails below 95%
total statement coverage and warns on any individual function under 100%, so
100% everywhere is the goal; 95% is the gate.

```bash
go test ./... -cover           # all packages
go test ./internal/cache/disk/ -v -race
go test ./internal/cache/disk/ -bench=.
```

## Gates to run before submitting

```bash
go build ./...                 # compile everything
go vet ./...                   # static checks
gofmt -l .                     # nothing printed
go test ./... -race            # full suite under the race detector
```

## Coverage

All packages (including the `cmd` command tree and the `main` entrypoint) carry
suite-level tests. The number is measured in CI and published to Codecov, so the
badge in the README always reflects the last run rather than a hand-maintained
figure. To check locally:

```bash
go test -cover ./cmd/ ./internal/config/ ./plugins/waf/ ...
```

Note that some branches are hard to reach from a test — Pebble and NATS error
paths, for example — which is why the total sits in the mid-90s rather than at
100%. Cover the error paths you *can* reach.

## Ports

Tests bind fixed ports — `:8081`, `:8082`, `:9090`. Stop any proxy, testserver
or metrics process on those ports before running the suite, or
port-conflict-dependent tests will be skipped.

## Test fixtures

- The WAF package generates its own GeoLite-style `Country`/`ASN`/`City`
  `.mmdb` fixtures in-tests (`mmdbwriter`) so geo lookups run without
  downloading MaxMind databases.
- TLS tests generate in-memory certificates (`crypto/x509`).