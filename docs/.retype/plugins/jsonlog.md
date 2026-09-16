# JSON logs

nginx-style access and error logs as JSON Lines (JSONL).

Global config in `config.yaml`:

```yaml
json_log:
  enabled: true
  access_log: "./logs/access.jsonl"
  error_log: "./logs/error.jsonl"
  stdout: true
```

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool | false | Master switch. |
| `access_log` | string | `access.jsonl` | Access log file; `"-"` = stdout. |
| `error_log` | string | `error.jsonl` | Error log file; `"-"` = stdout. |
| `stdout` | bool | false | Additionally mirror every line to stdout. |

## Access entries

Recorded for **every** request (including cache hits and gRPC):

`method`, `uri`, `host`, `remote_addr`, `protocol`, `target`, `location`,
`user_agent`, `referer`, `request_id`, `level`, `type`, `time`, `status`,
`bytes_sent`, `request_time`, `cache` (`HIT`/`MISS`), `content_type`.

## Error entries

Routed from proxy/gRPC errors, transform failures and cache-write failures via
the `ErrorHook`:

`level`, `type: "error"`, `time`, `msg`, `target`, `location`, `request_id`,
plus arbitrary extra fields.

## File handling

- `Start` opens buffered (64&nbsp;KB) writers; lines are flushed per record.
- `Reopen()` rotates the underlying files on `SIGHUP` — drop-in for
  `logrotate` and similar.
- `Stop` flushes and closes the writers.
