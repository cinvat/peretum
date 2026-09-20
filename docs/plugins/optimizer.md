---
label: Optimizer
order: 800
---

# Optimizer

Minifies CSS/JavaScript and re-encodes/resizes JPEG and PNG images.

Location-scoped config (last location wins; applies proxy-wide):

```yaml
locations:
  - path: "/"
    optimize:
      enabled: true
      minify_css: true
      minify_js: true
      uglify_js: true
      images:
        enabled: true
        max_width: 1600
        max_height: 1200
        quality: 82          # 1-100 (default 85)
        format: "webp"       # see note below
        strip_metadata: true
        progressive: true
```

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool | false | Master switch. |
| `minify_css` | bool | false | Minify `text/css` / `*css` responses. |
| `minify_js` | bool | false | Minify `application/javascript` / `text/javascript` / `*js`. |
| `uglify_js` | bool | false | Same minification path as `minify_js`. |
| `images.enabled` | bool | false | Enable image optimization. |
| `images.max_width` / `max_height` | int | 0 (no resize) | Resize bounds (Catmull-Rom scaling). |
| `images.quality` | int | 85 | JPEG re-encode quality, 1–100. |
| `images.format` | string | — | Declared output format (`webp`/`avif`/`jpeg`/`png`). |
| `images.strip_metadata` | bool | false | Strip EXIF/metadata. |
| `images.progressive` | bool | false | Progressive encoding. |

> **`format` is not used at runtime.** Images are re-encoded in their **original
> decoded format** (JPEG or PNG only) so the stored `Content-Type` stays valid;
> WebP and GIF pass through untouched. Undecodable images pass through
> unchanged.

## Where it runs

`TransformResponseBody` runs **only on the caching path**: after a successful
2xx upstream fetch, immediately before the entry is stored — so the **cache
entry** is the optimized one and subsequent cache hits serve the transformed
body. Non-cached locations, live (non-cached) first responses, and gRPC never
see transformations.