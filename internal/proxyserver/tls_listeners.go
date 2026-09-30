package proxyserver

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"github.com/cinvat/peretum/internal/config"
	"github.com/quic-go/quic-go/http3"
	"k8s.io/klog/v2"
	"math/big"
	"net"
	"net/http"
	"time"
)

func (ps *proxyServer) buildTLSConfig() *tls.Config {
	var certs []tls.Certificate

	for _, target := range ps.targets {
		if target.TLS != nil && target.TLS.CertFile != "" && target.TLS.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(target.TLS.CertFile, target.TLS.KeyFile)
			if err != nil {
				klog.Warningf("failed to load TLS cert for target %s: %v", target.ServerName, err)
				continue
			}
			certs = append(certs, cert)
			klog.Infof("loaded TLS cert for target: %s", target.ServerName)
		}
	}

	if ps.proxyCfg.TLSCertFile != "" && ps.proxyCfg.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(ps.proxyCfg.TLSCertFile, ps.proxyCfg.TLSKeyFile)
		if err != nil {
			klog.Warningf("failed to load global TLS cert: %v", err)
		} else {
			certs = append(certs, cert)
			klog.Info("loaded global TLS cert")
		}
	}

	if len(certs) == 0 {
		if ps.tlsRequested() {
			cert, err := generateSelfSignedCert()
			if err != nil {
				klog.Warningf("failed to generate self-signed cert: %v", err)
				return nil
			}
			certs = append(certs, cert)
			klog.Info("no TLS certificates configured; generated self-signed certificate")
		} else {
			return nil
		}
	}

	// Advertise HTTP/2 (and HTTP/1.1 fallback) via ALPN explicitly. The
	// per-handshake config returned by GetConfigForClient replaces the base
	// config, so net/http's automatic "h2" ALPN entry (added to the base
	// config) would otherwise be discarded and every client would negotiate
	// plain HTTP/1.1.
	nextProtos := []string{"h2", "http/1.1"}
	return &tls.Config{
		Certificates: certs,
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			return &tls.Config{Certificates: certs, NextProtos: nextProtos}, nil
		},
		NextProtos: nextProtos,
		MinVersion: tls.VersionTLS12,
	}
}

// tlsRequested reports whether the proxy is expected to serve TLS, either
// because an explicit listener asks for ssl or quic, global TLS cert paths
// are set, or any target declares per-target TLS cert paths. When a
// self-signed certificate is generated in this case, all HTTP versions
// (HTTP/1.1, HTTP/2, and HTTP/3) work out of the box.
func (ps *proxyServer) tlsRequested() bool {
	if ps.proxyCfg == nil {
		return false
	}
	// Check listeners for ssl/quic flags
	specs, _ := ps.proxyCfg.ParseListeners()
	for _, sp := range specs {
		if sp.SSL || sp.QUIC {
			return true
		}
	}
	if ps.proxyCfg.TLSCertFile != "" || ps.proxyCfg.TLSKeyFile != "" {
		return true
	}
	for i := range ps.targets {
		if t := ps.targets[i].TLS; t != nil && (t.CertFile != "" || t.KeyFile != "") {
			return true
		}
	}
	return false
}

// generateSelfSignedCert creates a throwaway ECDSA certificate so the proxy
// can serve TLS, HTTP/2, and HTTP/3 when no real certificate is configured.
// Injectable crypto seams so tests can exercise the failure branches of
// self-signed certificate generation without touching the real RNG.
var (
	ecdsaGenerateKey       = ecdsa.GenerateKey
	createX509Certificate  = x509.CreateCertificate
	marshalECPrivateKey    = x509.MarshalECPrivateKey
	generateSelfSignedCert = newSelfSignedCert
)

// newSelfSignedCert creates a throwaway ECDSA certificate so the proxy
// can serve TLS, HTTP/2, and HTTP/3 when no real certificate is configured.
// Clients must explicitly trust it (e.g. curl -k).
func newSelfSignedCert() (tls.Certificate, error) {
	priv, err := ecdsaGenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate key: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: "peretum"},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}

	der, err := createX509Certificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create certificate: %w", err)
	}
	keyDER, err := marshalECPrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("marshal key: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return tls.X509KeyPair(certPEM, keyPEM)
}

func (ps *proxyServer) tcpServers() []*http.Server {
	if len(ps.srvs) > 0 {
		return ps.srvs
	}
	if ps.srv != nil {
		return []*http.Server{ps.srv}
	}
	return nil
}

// quicServers returns every active HTTP/3 server, falling back to the
// legacy singular h3Srv field when no listener slice was built.
func (ps *proxyServer) quicServers() []*http3.Server {
	if len(ps.h3Srvs) > 0 {
		return ps.h3Srvs
	}
	if ps.h3Srv != nil {
		return []*http3.Server{ps.h3Srv}
	}
	return nil
}

// packetConns returns every active QUIC packet connection, falling back to
// the legacy singular h3Conn field when no listener slice was built.
func (ps *proxyServer) packetConns() []net.PacketConn {
	if len(ps.h3Conns) > 0 {
		return ps.h3Conns
	}
	if ps.h3Conn != nil {
		return []net.PacketConn{ps.h3Conn}
	}
	return nil
}

// closeListeners releases any sockets bound before a startup failure so a
// partially started proxy cannot hold onto ports.
func (ps *proxyServer) closeListeners() {
	for _, ln := range ps.lns {
		ln.Close()
	}
	for _, c := range ps.h3Conns {
		c.Close()
	}
}

// listenerSpecs resolves the frontend listeners for start().
func (ps *proxyServer) listenerSpecs() ([]config.ListenersSpec, error) {
	if ps.proxyCfg == nil {
		return []config.ListenersSpec{{Addr: ":8081"}}, nil
	}
	specs, err := ps.proxyCfg.ParseListeners()
	if err != nil {
		return nil, err
	}
	if len(specs) > 0 {
		return specs, nil
	}
	// Default if no listeners configured.
	return []config.ListenersSpec{{Addr: ":8081"}}, nil
}

// waitForConfigReplay blocks until the retained config events have been applied
// to the target store and the live consumer is attached.
//
// It is a startup gate, not a nicety. The router resolves hostnames by looking
// them up in the store, so a target whose event has not been replayed yet has no
// route: the edge would answer 404 for exactly the targets it is supposed to
// serve, and a fresh edge replaying a large stream is the normal case. Failing
// to wait turns startup into an outage window; failing loudly here lets the
// orchestrator restart the edge instead.
func (ps *proxyServer) startQuicListener(addr string, tlsConfig *tls.Config) error {
	if addr == "" {
		return nil
	}
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return fmt.Errorf("listen http3 on %s: %w", addr, err)
	}
	ps.h3Conns = append(ps.h3Conns, conn)
	ps.h3Srvs = append(ps.h3Srvs, &http3.Server{
		Addr:      addr,
		Handler:   ps.frontendHandler(),
		TLSConfig: tlsConfig.Clone(),
	})
	klog.Infof("HTTP/3 (QUIC) server listening on %s", addr)
	return nil
}

// startHTTP3 is no longer used — HTTP/3 is configured via listeners array with 'quic' flag.
func (ps *proxyServer) startHTTP3(tlsConfig *tls.Config) error {
	return nil
}
