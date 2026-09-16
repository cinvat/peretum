# Testing

**Every package must keep 100% statement coverage** — new code requires
matching tests, never a relaxed bar.

```bash
go test ./... -cover           # suite must stay at 100% per package
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

All 20 packages (including the `cmd` command tree and the `main` entrypoint)
carry suite-level tests at 100% statement coverage. Check per-package coverage
with:

```bash
go test -cover ./cmd/ ./internal/config/ ./plugins/waf/ ...
```

## Ports

Tests bind fixed ports — `:8081`, `:8082`, `:9090`. Stop any proxy, testserver
or metrics process on those ports before running the suite, or
port-conflict-dependent tests will be skipped.

## Test fixtures

- The WAF package generates its own GeoLite-style `Country`/`ASN`/`City`
  `.mmdb` fixtures in-tests (`mmdbwriter`) so geo lookups run without
  downloading MaxMind databases.
- TLS tests generate in-memory certificates (`crypto/x509`).
