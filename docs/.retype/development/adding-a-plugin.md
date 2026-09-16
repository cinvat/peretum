# Adding a plugin

1. **Implement the contracts** from `plugins/base/base.go`:
   - `Plugin` — `Name()`, `Init(config map[string]any)`, `Start(ctx)`,
     `Stop(ctx)` (embed `base.BasePlugin` for the shared no-op defaults).
   - Optional interfaces discovered structurally:
     - `RequestHook` — `BeforeProxy`, `AfterProxy`
     - `ResponseBodyHook` — `TransformResponseBody`
     - `CacheHook` — `BeforeCacheStore`, `AfterCacheHit`, `ShouldCache`
     - `MetricsHook` — `RecordRequest`, `RecordCacheHit/Miss/Eviction/Expiration`
     - `ErrorHook` — `LogError`
     - `RouterWrapper` — `WrapRouter` (frontend-level wrapping)
     - `Reopen` — log-rotation on `SIGHUP`
2. **Register** it in `plugins/registry/init.go`:
   ```go
   Register("my_plugin", func() base.Plugin { return myplugin.New() })
   ```
3. **Wire the config** in `cmd/server.go` `buildPluginConfigs` (and, for
   location-scoped plugins, a `structToMap`-style conversion) so that your
   plugin's keys are emitted with `enabled` as the load gate.
4. **Enable it** under the `plugins:`/location settings in your YAML config.

## Data model

- `Init` receives a `map[string]any`; use the panic-free accessors
  (`base.GetBool`, `GetInt`, `GetString`, `GetStringSlice`) from
  `plugins/base/helpers.go`.
- Request IDs are available via `base.EnsureRequestID` / `base.RequestID`.
- Location-scoped plugins are **global and last-wins** today; if your plugin
  needs per-location policies (like the WAF), aggregate a
  `target|location`-indexed map in the wiring instead.

## Tests

New code requires the package to stay at **100% statement coverage** — write
tests for `Init`, `Start`/`Stop`, and every hook you implement, and run the
[full gate set](testing.md) before submitting.
