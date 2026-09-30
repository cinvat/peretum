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
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/cinvat/peretum/internal/cluster"
)

func genCertFiles(t *testing.T, dir string) (string, string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	writeFile(t, certFile, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})))
	writeFile(t, keyFile, string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})))
	return certFile, keyFile
}

func mustLoadCert(t *testing.T, certFile, keyFile string) []tls.Certificate {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("load cert: %v", err)
	}
	return []tls.Certificate{cert}
}

type closedPacketConn struct{}

func (closedPacketConn) Close() error { return fmt.Errorf("conn-fail") }
func (closedPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	return 0, nil, fmt.Errorf("closed")
}
func (closedPacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	return 0, fmt.Errorf("closed")
}
func (closedPacketConn) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
}
func (closedPacketConn) SetDeadline(t time.Time) error      { return nil }
func (closedPacketConn) SetReadDeadline(t time.Time) error  { return nil }
func (closedPacketConn) SetWriteDeadline(t time.Time) error { return nil }

func lazyTestStore(t *testing.T) *cluster.TargetStore {
	t.Helper()
	store, err := cluster.OpenTargetStore(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatalf("OpenTargetStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func lazyEchoUpstream(t *testing.T, body string) string {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, body)
	}))
	t.Cleanup(ts.Close)
	return ts.URL
}
