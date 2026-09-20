package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/quic-go/quic-go/http3"
)

func main() {
	host := flag.String("host", "localhost", "target host")
	port := flag.Int("port", 443, "target port")
	insecure := flag.Bool("k", false, "skip TLS verify")
	flag.Parse()

	url := fmt.Sprintf("https://%s:%d/", *host, *port)

	transport := &http3.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: *insecure,
			ServerName:         *host,
		},
	}
	defer transport.Close()

	client := &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
	}

	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	fmt.Printf("protocol: %s status: %d\n", resp.Proto, resp.StatusCode)
}
