package main

import (
	"fmt"
	"net/http"
	"strings"
)

// newTestHandler returns the handler used by the test server. Responses:
//   - /style.css returns a repeated CSS block (~5600 bytes) for compression testing
//   - /script.js returns a repeated JS block (~5100 bytes) for compression testing
//   - /image.png returns a minimal PNG header
//   - everything else returns a plain-text line echoing the request path
func newTestHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/style.css":
			w.Header().Set("Content-Type", "text/css")
			css := "/* Comment */\nbody { color: red; }\n.class { margin: 0; }\n"
			w.Write([]byte(strings.Repeat(css, 100)))
		case "/script.js":
			w.Header().Set("Content-Type", "application/javascript")
			js := "// Comment\nfunction test() { var x = 1; return x; }\n"
			w.Write([]byte(strings.Repeat(js, 100)))
		case "/image.png":
			w.Header().Set("Content-Type", "image/png")
			w.Write([]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A})
		default:
			w.Header().Set("Content-Type", "text/plain")
			fmt.Fprintf(w, "OK from test server - path: %s\n", r.URL.Path)
		}
	})
	return mux
}

// StartServer listens on addr and serves the test handler. It returns
// when the server is shut down, or an error if the listener fails.
func StartServer(addr string) error {
	return http.ListenAndServe(addr, newTestHandler())
}

func main() {
	if err := StartServer(":8082"); err != nil {
		panic(err)
	}
}
