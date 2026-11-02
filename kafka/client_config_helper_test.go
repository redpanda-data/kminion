package kafka

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/youmark/pkcs8"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestNewKgoConfig_GSSAPIUserAuth(t *testing.T) {
	krb5Path := filepath.Join(t.TempDir(), "krb5.conf")
	require.NoError(t, os.WriteFile(krb5Path, []byte("[libdefaults]\n  default_realm = EXAMPLE.COM\n"), 0o600))

	cfg := Config{}
	cfg.SetDefaults()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.SASL.Enabled = true
	cfg.SASL.Mechanism = SASLMechanismGSSAPI
	cfg.SASL.GSSAPI.AuthType = "USER_AUTH"
	cfg.SASL.GSSAPI.KerberosConfigPath = krb5Path
	cfg.SASL.GSSAPI.Username = "user"
	cfg.SASL.GSSAPI.Password = "password"
	cfg.SASL.GSSAPI.Realm = "EXAMPLE.COM"
	require.NoError(t, cfg.Validate())

	_, err := NewKgoConfig(cfg, zap.NewNop())
	require.NoError(t, err)
}

func TestNewKgoConfig_EncryptedTLSKey(t *testing.T) {
	const passphrase = "dummy-passphrase"
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "kminion"}}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	pkcs8DER, err := pkcs8.MarshalPrivateKey(key, []byte(passphrase), nil)
	require.NoError(t, err)
	legacyBlock, err := x509.EncryptPEMBlock(rand.Reader, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key), []byte(passphrase), x509.PEMCipherAES256)
	require.NoError(t, err)

	tests := []struct {
		name     string
		key      []byte
		wantWarn bool
	}{
		{name: "pkcs8", key: pem.EncodeToMemory(&pem.Block{Type: "ENCRYPTED PRIVATE KEY", Bytes: pkcs8DER}), wantWarn: false},
		{name: "legacy", key: pem.EncodeToMemory(legacyBlock), wantWarn: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{}
			cfg.SetDefaults()
			cfg.Brokers = []string{"localhost:9092"}
			cfg.TLS.Enabled = true
			cfg.TLS.Cert = string(certPEM)
			cfg.TLS.Key = string(tt.key)
			cfg.TLS.Passphrase = passphrase

			legacyPEMWarnOnce = sync.Once{}
			core, logs := observer.New(zap.WarnLevel)
			_, err := NewKgoConfig(cfg, zap.New(core))
			require.NoError(t, err)
			_, err = NewKgoConfig(cfg, zap.New(core))
			require.NoError(t, err)
			wantWarnings := 0
			if tt.wantWarn {
				wantWarnings = 1
			}
			require.Equal(t, wantWarnings, logs.FilterMessageSnippet("legacy PEM encryption").Len())

			cfg.TLS.Passphrase = "wrong"
			_, err = NewKgoConfig(cfg, zap.NewNop())
			require.Error(t, err)

			cfg.TLS.Passphrase = ""
			_, err = NewKgoConfig(cfg, zap.NewNop())
			require.Error(t, err)
		})
	}
}
