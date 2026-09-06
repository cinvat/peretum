package optimizer

import (
	"bytes"
	"context"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"strings"
	"sync"

	"github.com/tdewolff/minify/v2"
	"github.com/tdewolff/minify/v2/css"
	"github.com/tdewolff/minify/v2/js"
	"github.com/tdewolff/minify/v2/svg"
	"golang.org/x/image/draw"
	_ "golang.org/x/image/webp"

	"github.com/cinvat/peretum/plugins/base"
)

type OptimizerPlugin struct {
	*base.BasePlugin

	minifier *minify.M
	config   OptimizerConfig
	mu       sync.RWMutex
}

type OptimizerConfig struct {
	MinifyCSS bool
	MinifyJS  bool
	UglifyJS  bool
	Enabled   bool

	Images struct {
		Enabled       bool
		MaxWidth      int
		MaxHeight     int
		Quality       int
		Format        string
		StripMetadata bool
		Progressive   bool
	}
}

func NewOptimizerPlugin() *OptimizerPlugin {
	return &OptimizerPlugin{
		BasePlugin: base.NewBasePlugin("optimizer"),
	}
}

func (p *OptimizerPlugin) Init(config map[string]any) error {
	p.BasePlugin.Init(config)

	p.config.Enabled = base.GetBool(config, "enabled")
	p.config.MinifyCSS = base.GetBool(config, "minify_css")
	p.config.MinifyJS = base.GetBool(config, "minify_js")
	p.config.UglifyJS = base.GetBool(config, "uglify_js")

	if imgConfig, ok := config["images"].(map[string]any); ok {
		p.config.Images.Enabled = base.GetBool(imgConfig, "enabled")
		p.config.Images.MaxWidth = base.GetIntOrDefault(imgConfig, "max_width", 0)
		p.config.Images.MaxHeight = base.GetIntOrDefault(imgConfig, "max_height", 0)
		p.config.Images.Quality = base.GetIntOrDefault(imgConfig, "quality", 85)
		p.config.Images.Format = base.GetString(imgConfig, "format")
		p.config.Images.StripMetadata = base.GetBool(imgConfig, "strip_metadata")
		p.config.Images.Progressive = base.GetBool(imgConfig, "progressive")
	}

	p.minifier = minify.New()
	p.minifier.AddFunc("text/css", css.Minify)
	p.minifier.AddFunc("application/javascript", js.Minify)
	p.minifier.AddFunc("text/javascript", js.Minify)
	p.minifier.AddFunc("image/svg+xml", svg.Minify)

	return nil
}

func (p *OptimizerPlugin) Start(ctx context.Context) error { return nil }
func (p *OptimizerPlugin) Stop(ctx context.Context) error  { return nil }

// TransformResponseBody is applied by the handler before the response is
// stored in cache, guaranteeing the cached copy is the optimized one.
func (p *OptimizerPlugin) TransformResponseBody(target, location, contentType string, body []byte) ([]byte, error) {
	p.mu.RLock()
	config := p.config
	p.mu.RUnlock()

	if !config.Enabled {
		return body, nil
	}

	ct := strings.ToLower(strings.TrimSpace(contentType))
	ct = strings.Split(ct, ";")[0]

	if config.MinifyCSS && (ct == "text/css" || strings.HasSuffix(ct, "css")) {
		return p.minifyCSS(body)
	}

	if (config.MinifyJS || config.UglifyJS) && (ct == "application/javascript" || ct == "text/javascript" || strings.HasSuffix(ct, "js")) {
		return p.minifyJS(body)
	}

	if config.Images.Enabled && strings.HasPrefix(ct, "image/") {
		return p.optimizeImage(body)
	}

	return body, nil
}

func (p *OptimizerPlugin) minifyCSS(body []byte) ([]byte, error) {
	if p.minifier == nil {
		return body, nil
	}
	var buf bytes.Buffer
	if err := p.minifier.Minify("text/css", &buf, bytes.NewReader(body)); err != nil {
		return body, nil
	}
	return buf.Bytes(), nil
}

func (p *OptimizerPlugin) minifyJS(body []byte) ([]byte, error) {
	if p.minifier == nil {
		return body, nil
	}
	var buf bytes.Buffer
	if err := p.minifier.Minify("application/javascript", &buf, bytes.NewReader(body)); err != nil {
		return body, nil
	}
	return buf.Bytes(), nil
}

func (p *OptimizerPlugin) optimizeImage(body []byte) ([]byte, error) {
	img, format, err := image.Decode(bytes.NewReader(body))
	if err != nil {
		// Not a decodable image; leave it untouched.
		return body, nil
	}

	cfg := p.config.Images
	if cfg.MaxWidth > 0 || cfg.MaxHeight > 0 {
		img = resizeImage(img, cfg.MaxWidth, cfg.MaxHeight)
	}

	// Re-encode in the ORIGINAL format so the Content-Type header stays
	// valid (this hook cannot mutate headers). WebP/GIF cannot be re-encoded
	// with the standard library, so those are served as-is.
	var buf bytes.Buffer
	switch format {
	case "jpeg":
		quality := cfg.Quality
		if quality <= 0 {
			quality = 85
		}
		if err := encodeJPEG(&buf, img, quality); err != nil {
			return body, nil
		}
	case "png":
		if err := encodePNG(&buf, img); err != nil {
			return body, nil
		}
	default:
		return body, nil
	}

	return buf.Bytes(), nil
}

// encodeJPEG and encodePNG are test seams so the re-encode error branches
// can be exercised without simulating impossible writer failures.
var encodeJPEG = func(w io.Writer, img image.Image, quality int) error {
	return jpeg.Encode(w, img, &jpeg.Options{Quality: quality})
}

var encodePNG = func(w io.Writer, img image.Image) error {
	return (&png.Encoder{CompressionLevel: png.BestCompression}).Encode(w, img)
}

func resizeImage(img image.Image, maxWidth, maxHeight int) image.Image {
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()

	if maxWidth <= 0 {
		maxWidth = width
	}
	if maxHeight <= 0 {
		maxHeight = height
	}

	scaleX := float64(maxWidth) / float64(width)
	scaleY := float64(maxHeight) / float64(height)
	scale := scaleX
	if scaleY < scale {
		scale = scaleY
	}

	if scale >= 1.0 {
		return img
	}

	newWidth := int(float64(width) * scale)
	newHeight := int(float64(height) * scale)

	dst := image.NewRGBA(image.Rect(0, 0, newWidth, newHeight))
	draw.CatmullRom.Scale(dst, dst.Bounds(), img, bounds, draw.Over, nil)
	return dst
}
