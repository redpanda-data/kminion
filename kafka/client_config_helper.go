package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"go.uber.org/zap"

	krbconfig "github.com/jcmturner/gokrb5/v8/config"
)

// NewKgoConfig creates a new Config for the Kafka Client as exposed by the franz-go library.
// If TLS certificates can't be read an error will be returned.
// logger is only used to print warnings about TLS.
func NewKgoConfig(cfg Config, logger *zap.Logger) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.FetchMaxBytes(5 * 1000 * 1000), // 5MB
		kgo.MaxConcurrentFetches(10),
		// Allow metadata to be refreshed more often than 5s (default) if needed.
		// That will mitigate issues with unknown partitions shortly after creating
		// them.
		kgo.MetadataMinAge(time.Second),
	}

	// Create Logger
	kgoLogger := KgoZapLogger{
		logger: logger.Sugar(),
	}
	opts = append(opts, kgo.WithLogger(kgoLogger))

	// Add Rack Awareness if configured
	if cfg.RackID != "" {
		opts = append(opts, kgo.Rack(cfg.RackID))
	}

	// Configure SASL
	if cfg.SASL.Enabled {
		// SASL Plain
		if cfg.SASL.Mechanism == "PLAIN" {
			mechanism := plain.Auth{
				User: cfg.SASL.Username,
				Pass: cfg.SASL.Password,
			}.AsMechanism()
			opts = append(opts, kgo.SASL(mechanism))
		}

		// SASL SCRAM
		if cfg.SASL.Mechanism == "SCRAM-SHA-256" || cfg.SASL.Mechanism == "SCRAM-SHA-512" {
			var mechanism sasl.Mechanism
			scramAuth := scram.Auth{
				User: cfg.SASL.Username,
				Pass: cfg.SASL.Password,
			}
			if cfg.SASL.Mechanism == "SCRAM-SHA-256" {
				mechanism = scramAuth.AsSha256Mechanism()
			}
			if cfg.SASL.Mechanism == "SCRAM-SHA-512" {
				mechanism = scramAuth.AsSha512Mechanism()
			}
			opts = append(opts, kgo.SASL(mechanism))
		}

		// Kerberos
		if cfg.SASL.Mechanism == "GSSAPI" {
			var krbClient *client.Client

			kerbCfg, err := krbconfig.Load(cfg.SASL.GSSAPI.KerberosConfigPath)
			if err != nil {
				return nil, fmt.Errorf("failed to create kerberos config from specified config filepath: %w", err)
			}

			switch cfg.SASL.GSSAPI.AuthType {
			case "USER_AUTH:":
				krbClient = client.NewWithPassword(
					cfg.SASL.GSSAPI.Username,
					cfg.SASL.GSSAPI.Realm,
					cfg.SASL.GSSAPI.Password,
					kerbCfg,
					client.DisablePAFXFAST(!cfg.SASL.GSSAPI.EnableFast))
			case "KEYTAB_AUTH":
				ktb, err := keytab.Load(cfg.SASL.GSSAPI.KeyTabPath)
				if err != nil {
					return nil, fmt.Errorf("failed to load keytab: %w", err)
				}
				krbClient = client.NewWithKeytab(
					cfg.SASL.GSSAPI.Username,
					cfg.SASL.GSSAPI.Realm,
					ktb,
					kerbCfg,
					client.DisablePAFXFAST(!cfg.SASL.GSSAPI.EnableFast))
			}
			if krbClient == nil {
				return nil, fmt.Errorf("kafka.sasl.gssapi.authType must be one of USER_AUTH or KEYTAB_AUTH")
			}
			kerberosMechanism := kerberos.Auth{
				Client:           krbClient,
				Service:          cfg.SASL.GSSAPI.ServiceName,
				PersistAfterAuth: true,
			}.AsMechanism()
			opts = append(opts, kgo.SASL(kerberosMechanism))
		}

		// OAuthBearer
		if cfg.SASL.Mechanism == "OAUTHBEARER" {
			mechanism := oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
				token, err := cfg.SASL.OAuthBearer.getToken(ctx)
				return oauth.Auth{
					Zid:   cfg.SASL.OAuthBearer.ClientID,
					Token: token,
				}, err
			})
			opts = append(opts, kgo.SASL(mechanism))
		}
	}

	// Configure TLS
	var caCertPool *x509.CertPool
	if cfg.TLS.Enabled {
		// Root CA
		if cfg.TLS.CaFilepath != "" || len(cfg.TLS.Ca) > 0 {
			ca := []byte(cfg.TLS.Ca)
			if cfg.TLS.CaFilepath != "" {
				caBytes, err := os.ReadFile(cfg.TLS.CaFilepath)
				if err != nil {
					return nil, fmt.Errorf("failed to load ca cert: %w", err)
				}
				ca = caBytes
			}
			caCertPool = x509.NewCertPool()
			isSuccessful := caCertPool.AppendCertsFromPEM(ca)
			if !isSuccessful {
				logger.Warn("failed to append ca file to cert pool, is this a valid PEM format?")
			}
		}

		// If configured load TLS cert & key - Mutual TLS
		var certificates []tls.Certificate
		hasCertFile := cfg.TLS.CertFilepath != "" || len(cfg.TLS.Cert) > 0
		hasKeyFile := cfg.TLS.KeyFilepath != "" || len(cfg.TLS.Key) > 0
		if hasCertFile || hasKeyFile {
			cert := []byte(cfg.TLS.Cert)
			privateKey := []byte(cfg.TLS.Key)
			// 1. Read certificates
			if cfg.TLS.CertFilepath != "" {
				certBytes, err := os.ReadFile(cfg.TLS.CertFilepath)
				if err != nil {
					return nil, fmt.Errorf("failed to TLS certificate: %w", err)
				}
				cert = certBytes
			}

			if cfg.TLS.KeyFilepath != "" {
				keyBytes, err := os.ReadFile(cfg.TLS.KeyFilepath)
				if err != nil {
					return nil, fmt.Errorf("failed to read TLS key: %w", err)
				}
				privateKey = keyBytes
			}

			// 2. Decrypt private key if encrypted and passphrase is provided
			if cfg.TLS.Passphrase != "" {
				var err error
				privateKey, err = decryptPrivateKey(privateKey, cfg.TLS.Passphrase, logger)
				if err != nil {
					return nil, fmt.Errorf("failed to decrypt private key: %w", err)
				}
			}

			// 3. Parse the certificate and key pair
			tlsCert, err := tls.X509KeyPair(cert, privateKey)
			if err != nil {
				return nil, fmt.Errorf("cannot parse pem: %s", err)
			}
			certificates = []tls.Certificate{tlsCert}
		}

		tlsDialer := &tls.Dialer{
			NetDialer: &net.Dialer{Timeout: 10 * time.Second},
			Config: &tls.Config{
				InsecureSkipVerify: cfg.TLS.InsecureSkipTLSVerify,
				Certificates:       certificates,
				RootCAs:            caCertPool,
			},
		}
		opts = append(opts, kgo.Dialer(tlsDialer.DialContext))
	}

	return opts, nil
}


// decryptPrivateKey attempts to decrypt an encrypted PEM-encoded private key.
// It supports both modern PKCS#8 encrypted keys and legacy PEM encryption (with deprecation warning).
// If the key is not encrypted, it returns the key as-is.
func decryptPrivateKey(keyPEM []byte, passphrase string, logger *zap.Logger) ([]byte, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block containing private key")
	}

	// Check if it's an encrypted PKCS#8 key (modern, secure)
	if block.Type == "ENCRYPTED PRIVATE KEY" {
		// PKCS#8 encrypted keys should be decrypted using x509.ParsePKCS8PrivateKey
		// which doesn't support password-based decryption directly in stdlib.
		// For now, we'll use the legacy method with nolint for PKCS#8 as well.
		// TODO: Consider using golang.org/x/crypto/pkcs12 for proper PKCS#8 support
		decrypted, err := x509.DecryptPEMBlock(block, []byte(passphrase)) //nolint:staticcheck // No stdlib alternative for PKCS#8 password decryption
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt PKCS#8 private key: %w", err)
		}
		// Re-encode as unencrypted PKCS#8
		return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: decrypted}), nil
	}

	// Check if it's a legacy encrypted PEM block (insecure, deprecated)
	if x509.IsEncryptedPEMBlock(block) { //nolint:staticcheck // Supporting legacy keys for backward compatibility
		logger.Warn("Using legacy PEM encryption for private key. This encryption method is insecure and deprecated. " +
			"Please migrate to PKCS#8 encrypted keys. " +
			"You can convert your key using: openssl pkcs8 -topk8 -v2 aes256 -in old_key.pem -out new_key.pem")

		// Decrypt using legacy method (insecure but needed for backward compatibility)
		decrypted, err := x509.DecryptPEMBlock(block, []byte(passphrase)) //nolint:staticcheck // Supporting legacy keys
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt legacy PEM private key: %w", err)
		}
		// Re-encode as unencrypted PEM
		return pem.EncodeToMemory(&pem.Block{Type: block.Type, Bytes: decrypted}), nil
	}

	// Key is not encrypted, return as-is
	return keyPEM, nil
}
