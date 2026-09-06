package optimizer

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/gif"
	"image/jpeg"
	"image/png"
	"io"
	"testing"

	"github.com/tdewolff/minify/v2"
)

func newOptimizer(t *testing.T, cfg map[string]any) *OptimizerPlugin {
	t.Helper()
	p := NewOptimizerPlugin()
	if err := p.Init(cfg); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	return p
}

func jpegBytes(t *testing.T, w, h, quality int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: quality}); err != nil {
		t.Fatalf("jpeg encode: %v", err)
	}
	return buf.Bytes()
}

func pngBytes(t *testing.T, w, h int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("png encode: %v", err)
	}
	return buf.Bytes()
}

func gifBytes(t *testing.T, w, h int) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	var buf bytes.Buffer
	if err := gif.Encode(&buf, img, nil); err != nil {
		t.Fatalf("gif encode: %v", err)
	}
	return buf.Bytes()
}

func TestInit(t *testing.T) {
	p := newOptimizer(t, map[string]any{
		"enabled":    true,
		"minify_css": true,
		"minify_js":  true,
		"uglify_js":  true,
		"images": map[string]any{
			"enabled":        true,
			"max_width":      800,
			"max_height":     600.0,
			"quality":        80,
			"format":         "webp",
			"strip_metadata": true,
			"progressive":    true,
		},
	})
	if !p.config.Enabled || !p.config.MinifyCSS || !p.config.MinifyJS || !p.config.UglifyJS {
		t.Errorf("flags: %+v", p.config)
	}
	img := p.config.Images
	if !img.Enabled || img.MaxWidth != 800 || img.MaxHeight != 600 || img.Quality != 80 {
		t.Errorf("images config: %+v", img)
	}
	if img.Format != "webp" || !img.StripMetadata || !img.Progressive {
		t.Errorf("images config 2: %+v", img)
	}
	if p.minifier == nil {
		t.Error("minifier not created")
	}
}

func TestInitNoImages(t *testing.T) {
	p := newOptimizer(t, map[string]any{"enabled": true})
	if p.config.Images.Enabled {
		t.Error("images should be disabled")
	}
}

func TestInitQualityDefault(t *testing.T) {
	p := newOptimizer(t, map[string]any{
		"enabled": true,
		"images": map[string]any{
			"enabled": true,
		},
	})
	if p.config.Images.Quality != 85 {
		t.Errorf("default quality = %d", p.config.Images.Quality)
	}
}

func TestStartStop(t *testing.T) {
	p := newOptimizer(t, map[string]any{"enabled": true})
	if err := p.Start(context.Background()); err != nil {
		t.Errorf("Start: %v", err)
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Errorf("Stop: %v", err)
	}
}

func TestTransformDisabled(t *testing.T) {
	p := newOptimizer(t, map[string]any{"enabled": false})
	body := []byte("anything")
	out, err := p.TransformResponseBody("t", "l", "text/css", body)
	if err != nil || !bytes.Equal(out, body) {
		t.Errorf("disabled should pass through untouched: out=%q err=%v", out, err)
	}
}

func TestTransformCSS(t *testing.T) {
	p := newOptimizer(t, map[string]any{"enabled": true, "minify_css": true})
	css := []byte("body { color: red; color: red; }")
	out, err := p.TransformResponseBody("t", "l", "text/css; charset=utf-8", css)
	if err != nil {
		t.Fatalf("css transform: %v", err)
	}
	if len(out) >= len(css) {
		t.Errorf("css not minified: %q", out)
	}
}

func TestTransformCSSSuffix(t *testing.T) {
	p := newOptimizer(t, map[string]any{"enabled": true, "minify_css": true})
	css := []byte("a { color: black; }")
	out, err := p.TransformResponseBody("t", "l", "text/x-custom.css", css)
	if err != nil || len(out) == 0 {
		t.Errorf("css suffix transform failed: %q err=%v", out, err)
	}
}

func TestTransformJS(t *testing.T) {
	p := newOptimizer(t, map[string]any{"enabled": true, "minify_js": true})
	js := []byte("function foo() { return 1; }; function foo() { return 1; };")
	out, err := p.TransformResponseBody("t", "l", "application/javascript", js)
	if err != nil {
		t.Fatalf("js transform: %v", err)
	}
	if len(out) >= len(js) {
		t.Errorf("js not minified: %q", out)
	}
}

func TestTransformJSSuffixAndUglify(t *testing.T) {
	p := newOptimizer(t, map[string]any{"enabled": true, "uglify_js": true})
	js := []byte("var x = 1; var y = 2;")
	out, err := p.TransformResponseBody("t", "l", "text/javascript.js", js)
	if err != nil || len(out) == 0 {
		t.Errorf("js suffix/uglify transform failed: %q err=%v", out, err)
	}
}

func TestTransformNoMatch(t *testing.T) {
	p := newOptimizer(t, map[string]any{"enabled": true})
	body := []byte("payload")
	out, err := p.TransformResponseBody("t", "l", "text/plain", body)
	if err != nil || !bytes.Equal(out, body) {
		t.Errorf("no-match should passthrough: err=%v", err)
	}
}

func TestMinifyCSSNilMinifier(t *testing.T) {
	p := newOptimizer(t, map[string]any{"enabled": true, "minify_css": true})
	p.minifier = nil
	out, err := p.minifyCSS([]byte("a{}"))
	if err != nil || string(out) != "a{}" {
		t.Errorf("nil minifier should passthrough: %q err=%v", out, err)
	}
}

func TestMinifyCSSError(t *testing.T) {
	p := newOptimizer(t, map[string]any{"enabled": true, "minify_css": true})
	p.minifier = minify.New()
	out, err := p.minifyCSS([]byte("a{}"))
	if err != nil || string(out) != "a{}" {
		t.Errorf("error should fall back to body: %q err=%v", out, err)
	}
}

func TestMinifyJSNilMinifier(t *testing.T) {
	p := newOptimizer(t, map[string]any{"enabled": true, "minify_js": true})
	p.minifier = nil
	out, err := p.minifyJS([]byte("var a=1;"))
	if err != nil || string(out) != "var a=1;" {
		t.Errorf("nil minifier should passthrough: %q err=%v", out, err)
	}
}

func TestMinifyJSError(t *testing.T) {
	p := newOptimizer(t, map[string]any{"enabled": true, "minify_js": true})
	p.minifier = minify.New()
	out, err := p.minifyJS([]byte("var a=1;"))
	if err != nil || string(out) != "var a=1;" {
		t.Errorf("error should fall back to body: %q err=%v", out, err)
	}
}

func TestOptimizeImageDecodeError(t *testing.T) {
	p := newOptimizer(t, map[string]any{"enabled": true, "images": map[string]any{"enabled": true}})
	body := []byte("not an image")
	out, err := p.TransformResponseBody("t", "l", "image/jpeg", body)
	if err != nil || !bytes.Equal(out, body) {
		t.Errorf("decode error should passthrough: err=%v", err)
	}
}

func TestOptimizeJPEG(t *testing.T) {
	p := newOptimizer(t, map[string]any{
		"enabled": true,
		"images": map[string]any{
			"enabled":    true,
			"max_width":  2,
			"max_height": 2,
			"quality":    90,
		},
	})
	body := jpegBytes(t, 40, 30, 90)
	out, err := p.TransformResponseBody("t", "l", "image/jpeg", body)
	if err != nil {
		t.Fatalf("jpeg optimize: %v", err)
	}
	if len(out) == 0 {
		t.Error("empty jpeg output")
	}
}

func TestOptimizeJPEGDefaultQuality(t *testing.T) {
	p := newOptimizer(t, map[string]any{
		"enabled": true,
		"images": map[string]any{
			"enabled": true,
			"quality": 0,
		},
	})
	body := jpegBytes(t, 8, 8, 80)
	out, err := p.TransformResponseBody("t", "l", "image/jpeg; charset=binary", body)
	if err != nil || len(out) == 0 {
		t.Errorf("jpeg default quality: err=%v", err)
	}
}

func TestOptimizePNG(t *testing.T) {
	p := newOptimizer(t, map[string]any{
		"enabled": true,
		"images":  map[string]any{"enabled": true},
	})
	body := pngBytes(t, 16, 16)
	out, err := p.TransformResponseBody("t", "l", "image/png", body)
	if err != nil || len(out) == 0 {
		t.Errorf("png optimize: err=%v", err)
	}
}

func TestOptimizeGIFReturnsAsIs(t *testing.T) {
	p := newOptimizer(t, map[string]any{
		"enabled": true,
		"images":  map[string]any{"enabled": true},
	})
	body := gifBytes(t, 8, 8)
	out, err := p.TransformResponseBody("t", "l", "image/gif", body)
	if err != nil || !bytes.Equal(out, body) {
		t.Errorf("gif should passthrough: err=%v", err)
	}
}

func TestOptimizeJPEGEncodeError(t *testing.T) {
	p := newOptimizer(t, map[string]any{
		"enabled": true,
		"images":  map[string]any{"enabled": true},
	})
	body := jpegBytes(t, 8, 8, 80)
	orig := encodeJPEG
	defer func() { encodeJPEG = orig }()
	encodeJPEG = func(w io.Writer, img image.Image, quality int) error {
		return errors.New("encode failed")
	}
	out, err := p.TransformResponseBody("t", "l", "image/jpeg", body)
	if err != nil || !bytes.Equal(out, body) {
		t.Errorf("encode error should passthrough: err=%v", err)
	}
}

func TestOptimizePNGEncodeError(t *testing.T) {
	p := newOptimizer(t, map[string]any{
		"enabled": true,
		"images":  map[string]any{"enabled": true},
	})
	body := pngBytes(t, 8, 8)
	orig := encodePNG
	defer func() { encodePNG = orig }()
	encodePNG = func(w io.Writer, img image.Image) error {
		return errors.New("encode failed")
	}
	out, err := p.TransformResponseBody("t", "l", "image/png", body)
	if err != nil || !bytes.Equal(out, body) {
		t.Errorf("encode error should passthrough: err=%v", err)
	}
}

func TestResizeImage(t *testing.T) {
	// scaleY >= scaleX, scale < 1
	img := image.NewRGBA(image.Rect(0, 0, 200, 100))
	got := resizeImage(img, 100, 100)
	if b := got.Bounds(); b.Dx() != 100 || b.Dy() != 50 {
		t.Errorf("resize(200x100,100x100) = %dx%d", b.Dx(), b.Dy())
	}

	// scaleY < scaleX -> pick scaleY
	img2 := image.NewRGBA(image.Rect(0, 0, 100, 200))
	got = resizeImage(img2, 100, 100)
	if b := got.Bounds(); b.Dx() != 50 || b.Dy() != 100 {
		t.Errorf("resize(100x200,100x100) = %dx%d", b.Dx(), b.Dy())
	}

	// scale >= 1.0 -> return original
	small := image.NewRGBA(image.Rect(0, 0, 10, 10))
	if got := resizeImage(small, 100, 100); got != image.Image(small) {
		t.Error("upscaling should return original image")
	}

	// maxWidth <= 0 and maxHeight <= 0 defaults
	got = resizeImage(img, 0, 50)
	if b := got.Bounds(); b.Dx() != 100 || b.Dy() != 50 {
		t.Errorf("resize(maxW=0) = %dx%d", b.Dx(), b.Dy())
	}
	got = resizeImage(img, 50, 0)
	if b := got.Bounds(); b.Dx() != 50 || b.Dy() != 25 {
		t.Errorf("resize(maxH=0) = %dx%d", b.Dx(), b.Dy())
	}
}
