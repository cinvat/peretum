# Contributing

## Adding a target

Drop a new YAML file in `config.d/` (see the [targets reference](../configuration/targets.md)), verify it with `peretum -t`, then `SIGHUP` a running proxy (or use `peretum -r`, or restart it). No code changes needed.

## Changing behavior

Document any config/behavior change in this documentation site (and the
`README.md`), and keep the [test coverage](testing.md) at 100% per package.

## Documentation site

This site is built from `docs/` by **Retype** and published to GitHub Pages:

- **Source:** `docs/` — plain Markdown with `label`/`order` front matter.
- **Project config:** `docs/retype.yml` (title, links, edit-on-GitHub,
  last-updated footer).
- **Navigation** is generated automatically: files in a folder become
  children of that folder's index page; `order` (larger = higher) sorts the
  sidebar, `icon` adds an Octicon.
- **Home page:** `docs/index.md`.
- **Links:** internal links reference `.md` paths (e.g. `../configuration/targets.md`)
  and are resolved to the generated pages automatically.
- **Deployment:** `.github/workflows/retype-action.yml` builds with
  `retypeapp/action-build` and publishes with
  `retypeapp/action-github-pages` on every push to `main`.

Preview locally:

```bash
npx retypeapp start          # serves docs/ with live reload
# http://localhost:5000/
```

Build the static site without a server:

```bash
npx retypeapp build          # writes the site to docs/.retype
```

## PR checklist

- [ ] `go build ./...` and `go vet ./...` are clean
- [ ] `gofmt -l .` prints nothing
- [ ] `go test ./... -race` passes and `go test ./... -cover` stays at 100%
      statement coverage in every package
- [ ] Config/behavior changes are documented in this docs site and the README
