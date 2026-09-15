# Command line

`peretum` is a single command (cobra-based):

```bash
peretum                  # run the proxy (defaults below)
peretum -t               # check configuration syntax and exit ('config OK')
peretum -r               # reload the running proxy (SIGHUP to the pid file)
```

The default command runs the proxy with a graceful shutdown: `SIGINT` /
`SIGTERM` trigger a 30&nbsp;second graceful drain of in-flight requests.

## Flags

| Flag | Description | Default |
| --- | --- | --- |
| `-t, --test` | Validate `config.yaml` and every `config.d/*.yaml` without starting the server; prints `config OK`. | `false` |
| `-r, --reload` | Read the running proxy's pid file and send it `SIGHUP` for a hot config reload. | `false` |
| `--config <path>` | Path to the proxy configuration file. | `config.yaml` |
| `--targets <dir>` | Directory of per-target configuration files. | `config.d` |
| `--pid-file <path>` | Pid file written at startup and read by `--reload`. | `peretum.pid` |

## `--test` (config check)

`checkConfig` validates, without binding ports:

- `max_cache_size`, `max_cache_age`, `max_response_body_size` size/duration
  formats;
- `max_write_workers` is `-1` (unlimited) or greater;
- every `listeners:` directive (syntax, flags, duplicate flags, conflicting
  `ssl` + `h2c`);
- every target's upstream URLs;
- every per-location `cache_ttl` duration.

## `--reload` (hot reload)

Sending `SIGHUP` (via `peretum -r` or `kill -HUP <pid>`) to a running proxy:

- reloads and re-validates `config.yaml` and `config.d/`;
- rebuilds the host/location router and re-applies TLS certificates
  (`GetConfigForClient` for dynamic per-target certs);
- rotates log outputs on plugins that implement `Reopen()` (the JSON-log
  plugin), which supports `logrotate`-style setups.

Plugin *behavior* changes (e.g. rule lists, levels) require a full restart:
`Init` is not re-run on reload.

## Exit codes

A non-zero exit code is returned on any startup or reload failure; syntax
errors from `-t` exit non-zero as well.
