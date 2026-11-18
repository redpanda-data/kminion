package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// testCA is a certificate authority that signs the server certificates of the tests.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kminion test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return &testCA{cert: cert, key: key}
}

func (ca *testCA) pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	return pool
}

// writeServerCert writes tls.crt and tls.key for 127.0.0.1 to dir. It replaces each file atomically.
func (ca *testCA) writeServerCert(t *testing.T, dir string, serial int64) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "kminion"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	writeFileAtomic(t, filepath.Join(dir, "tls.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	writeFileAtomic(t, filepath.Join(dir, "tls.key"), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

// writeFileAtomic writes a temporary file and renames it, so a reader never sees a partial file.
func writeFileAtomic(t *testing.T, path string, data []byte) {
	t.Helper()
	tmp := path + ".tmp"
	require.NoError(t, os.WriteFile(tmp, data, 0o600))
	require.NoError(t, os.Rename(tmp, path))
}

// copyWebConfig copies a file from testdata/webconfig to dir and returns the path of the copy.
func copyWebConfig(t *testing.T, dir, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "webconfig", name))
	require.NoError(t, err)
	path := filepath.Join(dir, "web.yml")
	writeFileAtomic(t, path, data)
	return path
}

// startServer runs serveHTTP on a free local port. The handler counts its calls.
func startServer(t *testing.T, webConfigFilepath string, logger *zap.Logger) (string, *atomic.Int64) {
	t.Helper()
	calls := &atomic.Int64{}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- serveHTTP(l, srv, webConfigFilepath, logger) }()
	t.Cleanup(func() {
		require.NoError(t, srv.Close())
		err := <-errCh
		require.True(t, errors.Is(err, http.ErrServerClosed), "unexpected serve error: %v", err)
	})
	return l.Addr().String(), calls
}

func tlsClient(ca *testCA, keepAlive bool) *http.Client {
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{RootCAs: ca.pool()},
			DisableKeepAlives: !keepAlive,
			ForceAttemptHTTP2: true,
		},
	}
}

func TestServeHTTP_NoWebConfig(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	addr, calls := startServer(t, "", zap.New(core))

	resp, err := http.Get("http://" + addr + "/metrics")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, int64(1), calls.Load())
	// Without a web config file, the toolkit does not run, so it writes no log lines.
	require.Zero(t, logs.Len())
}

func TestServeHTTP_TLS(t *testing.T) {
	tests := []struct {
		file      string
		wantProto string
		check     func(t *testing.T, addr string, ca *testCA, resp *http.Response)
	}{
		{file: "tls.yml", wantProto: "HTTP/2.0"},
		{
			file:      "tls-min-version.yml",
			wantProto: "HTTP/2.0",
			check: func(t *testing.T, addr string, ca *testCA, _ *http.Response) {
				client := tlsClient(ca, false)
				client.Transport.(*http.Transport).TLSClientConfig.MaxVersion = tls.VersionTLS12
				_, err := client.Get("https://" + addr + "/metrics")
				require.Error(t, err)
			},
		},
		{
			file:      "tls-http1-headers.yml",
			wantProto: "HTTP/1.1",
			check: func(t *testing.T, _ string, _ *testCA, resp *http.Response) {
				require.Equal(t, "max-age=31536000", resp.Header.Get("Strict-Transport-Security"))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			dir := t.TempDir()
			ca := newTestCA(t)
			ca.writeServerCert(t, dir, 2)
			addr, calls := startServer(t, copyWebConfig(t, dir, tt.file), zap.NewNop())

			resp, err := tlsClient(ca, false).Get("https://" + addr + "/metrics")
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.NotNil(t, resp.TLS)
			require.Equal(t, tt.wantProto, resp.Proto)
			require.Equal(t, int64(1), calls.Load())
			if tt.check != nil {
				tt.check(t, addr, ca, resp)
			}

			// A plain HTTP request to the TLS port does not reach the handler.
			resp, err = http.Get("http://" + addr + "/metrics")
			if err == nil {
				require.NoError(t, resp.Body.Close())
				require.NotEqual(t, http.StatusOK, resp.StatusCode)
			}
			require.Equal(t, int64(1), calls.Load())
		})
	}
}

func TestServeHTTP_CertificateRotation(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	ca.writeServerCert(t, dir, 2)
	addr, _ := startServer(t, copyWebConfig(t, dir, "tls.yml"), zap.NewNop())
	client := tlsClient(ca, false)

	serial := func() int64 {
		resp, err := client.Get("https://" + addr + "/metrics")
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return resp.TLS.PeerCertificates[0].SerialNumber.Int64()
	}
	require.Equal(t, int64(2), serial())

	// A new certificate applies to the next connection, without a restart.
	ca.writeServerCert(t, dir, 3)
	require.Equal(t, int64(3), serial())
}

func TestServeHTTP_MalformedWebConfig(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	ca.writeServerCert(t, dir, 2)
	path := copyWebConfig(t, dir, "tls.yml")
	addr, calls := startServer(t, path, zap.NewNop())
	url := "https://" + addr + "/metrics"

	keepAliveClient := tlsClient(ca, true)
	resp, err := keepAliveClient.Get(url)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)

	valid, err := os.ReadFile(path)
	require.NoError(t, err)
	writeFileAtomic(t, path, []byte("tls_server_config: [\n"))

	// The file is read on each request. On the open connection, the request gets a 500 and does not reach the handler.
	resp, err = keepAliveClient.Get(url)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	require.Equal(t, int64(1), calls.Load())

	// A new connection fails the TLS handshake.
	_, err = tlsClient(ca, false).Get(url)
	require.Error(t, err)

	// A valid file applies again without a restart.
	writeFileAtomic(t, path, valid)
	resp, err = tlsClient(ca, false).Get(url)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestServeHTTP_HandshakeErrorIsLogged(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	ca.writeServerCert(t, dir, 2)
	core, logs := observer.New(zapcore.InfoLevel)
	addr, _ := startServer(t, copyWebConfig(t, dir, "tls.yml"), zap.New(core))

	// The client does not trust the CA, so the handshake fails.
	_, err := tlsClient(newTestCA(t), false).Get("https://" + addr + "/metrics")
	require.Error(t, err)

	require.Eventually(t, func() bool {
		for _, e := range logs.FilterLoggerName("http").All() {
			if e.Level == zapcore.WarnLevel && strings.Contains(e.Message, "TLS handshake error") {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
}

// TestWebConfigCompatibility checks that the example files still pass validation. A toolkit upgrade must pass it
// without changes to the files.
func TestWebConfigCompatibility(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "webconfig", "*.yml"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	ca := newTestCA(t)
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			dir := t.TempDir()
			ca.writeServerCert(t, dir, 2)
			cfg := Config{}
			cfg.SetDefaults()
			cfg.Kafka.Brokers = []string{"a:9092"}
			cfg.Exporter.WebConfigFilepath = copyWebConfig(t, dir, filepath.Base(f))
			require.NoError(t, cfg.Validate())
		})
	}
}
