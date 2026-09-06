package rewrite

import (
	"context"
	"fmt"
	"net/http"
	"regexp"

	"k8s.io/klog/v2"

	"github.com/cinvat/peretum/plugins/base"
)

type RewritePlugin struct {
	*base.BasePlugin
	rules map[string]*RewriteRule
}

type RewriteRule struct {
	Pattern     *regexp.Regexp
	Replacement string
	Break       bool
	Redirect    string
	PatternStr  string
}

func NewRewritePlugin() *RewritePlugin {
	return &RewritePlugin{
		BasePlugin: base.NewBasePlugin("rewrite"),
		rules:      make(map[string]*RewriteRule),
	}
}

func (p *RewritePlugin) Init(config map[string]any) error {
	p.BasePlugin.Init(config)

	if rulesConfig, ok := config["rules"].([]any); ok {
		for i, ruleConfig := range rulesConfig {
			ruleMap, ok := ruleConfig.(map[string]any)
			if !ok {
				continue
			}

			patternStr := base.GetString(ruleMap, "pattern")
			replacement := base.GetString(ruleMap, "replacement")
			breakFlag := base.GetBool(ruleMap, "break")
			redirect := base.GetString(ruleMap, "redirect")

			if patternStr == "" || replacement == "" {
				continue
			}

			pattern, err := regexp.Compile(patternStr)
			if err != nil {
				klog.Warningf("Invalid regex pattern in rewrite rule %d: %v", i, err)
				continue
			}

			rule := &RewriteRule{
				Pattern:     pattern,
				Replacement: replacement,
				Break:       breakFlag,
				Redirect:    redirect,
				PatternStr:  patternStr,
			}
			p.rules[fmt.Sprintf("rule_%d", i)] = rule
			klog.Infof("Rewrite rule registered: %s -> %s (break=%v, redirect=%s)", patternStr, replacement, breakFlag, redirect)
		}
	}

	return nil
}

func (p *RewritePlugin) Start(ctx context.Context) error { return nil }
func (p *RewritePlugin) Stop(ctx context.Context) error  { return nil }

func (p *RewritePlugin) BeforeProxy(w http.ResponseWriter, r *http.Request, target, location string) error {
	for _, rule := range p.rules {
		if rule.Pattern.MatchString(r.URL.Path) {
			newPath := rule.Pattern.ReplaceAllString(r.URL.Path, rule.Replacement)
			if newPath != r.URL.Path {
				if rule.Redirect != "" {
					status := http.StatusMovedPermanently
					if rule.Redirect == "redirect" {
						status = http.StatusFound
					}
					query := r.URL.RawQuery
					if query != "" {
						query = "?" + query
					}
					http.Redirect(w, r, newPath+query, status)
					return fmt.Errorf("redirect")
				}

				r.URL.Path = newPath
				if rule.Break {
					break
				}
			}
		}
	}
	return nil
}

func (p *RewritePlugin) AfterProxy(w http.ResponseWriter, r *http.ResponseWriter, target, location string, resp *http.Response) error {
	return nil
}
