package headers

import (
	"context"
	"net/http"

	"github.com/cinvat/peretum/plugins/base"
)

type HeadersPlugin struct {
	*base.BasePlugin
	config HeadersConfig
}

type HeadersConfig struct {
	RequestAdd     map[string]string
	RequestRemove  []string
	ResponseAdd    map[string]string
	ResponseRemove []string
}

func NewHeadersPlugin() *HeadersPlugin {
	return &HeadersPlugin{
		BasePlugin: base.NewBasePlugin("headers"),
	}
}

func (p *HeadersPlugin) Init(config map[string]any) error {
	p.BasePlugin.Init(config)

	if reqAdd, ok := config["request_add"].(map[string]any); ok {
		p.config.RequestAdd = make(map[string]string)
		for k, v := range reqAdd {
			if str, ok := v.(string); ok {
				p.config.RequestAdd[k] = str
			}
		}
	}

	if reqRemove, ok := config["request_remove"].([]any); ok {
		for _, v := range reqRemove {
			if str, ok := v.(string); ok {
				p.config.RequestRemove = append(p.config.RequestRemove, str)
			}
		}
	}

	if respAdd, ok := config["response_add"].(map[string]any); ok {
		p.config.ResponseAdd = make(map[string]string)
		for k, v := range respAdd {
			if str, ok := v.(string); ok {
				p.config.ResponseAdd[k] = str
			}
		}
	}

	if respRemove, ok := config["response_remove"].([]any); ok {
		for _, v := range respRemove {
			if str, ok := v.(string); ok {
				p.config.ResponseRemove = append(p.config.ResponseRemove, str)
			}
		}
	}

	return nil
}

func (p *HeadersPlugin) Start(ctx context.Context) error { return nil }
func (p *HeadersPlugin) Stop(ctx context.Context) error  { return nil }

func (p *HeadersPlugin) BeforeProxy(w http.ResponseWriter, r *http.Request, target, location string) error {
	if p.config.RequestAdd != nil {
		for k, v := range p.config.RequestAdd {
			r.Header.Set(k, v)
		}
	}
	for _, k := range p.config.RequestRemove {
		r.Header.Del(k)
	}
	return nil
}

func (p *HeadersPlugin) AfterProxy(w http.ResponseWriter, r *http.ResponseWriter, target, location string, resp *http.Response) error {
	if p.config.ResponseAdd != nil {
		for k, v := range p.config.ResponseAdd {
			w.Header().Set(k, v)
		}
	}
	for _, k := range p.config.ResponseRemove {
		w.Header().Del(k)
	}
	return nil
}
