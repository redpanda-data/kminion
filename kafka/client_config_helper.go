package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"net"
	"time"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
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
		kgo.MaxVersions(kversion.V2_7_0()),
		kgo.ClientID(cfg.ClientID),
		kgo.FetchMaxBytes(5 * 1000 * 1000), // 5MB
		kgo.MaxConcurrentFetches(10),
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
				caBytes, err := ioutil.ReadFile(cfg.TLS.CaFilepath)
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
				certBytes, err := ioutil.ReadFile(cfg.TLS.CertFilepath)
				if err != nil {
					return nil, fmt.Errorf("failed to TLS certificate: %w", err)
				}
				cert = certBytes
			}

			if cfg.TLS.KeyFilepath != "" {
				keyBytes, err := ioutil.ReadFile(cfg.TLS.KeyFilepath)
				if err != nil {
					return nil, fmt.Errorf("failed to read TLS key: %w", err)
				}
				privateKey = keyBytes
			}

			// 2. Check if private key needs to be decrypted. Decrypt it if passphrase is given, otherwise return error
			pemBlock, _ := pem.Decode(privateKey)
			if pemBlock == nil {
				return nil, fmt.Errorf("no valid private key found")
			}

			if x509.IsEncryptedPEMBlock(pemBlock) {
				decryptedKey, err := x509.DecryptPEMBlock(pemBlock, []byte(cfg.TLS.Passphrase))
				if err != nil {
					return nil, fmt.Errorf("private key is encrypted, but could not decrypt it: %s", err)
				}
				// If private key was encrypted we can overwrite the original contents now with the decrypted version
				privateKey = pem.EncodeToMemory(&pem.Block{Type: pemBlock.Type, Bytes: decryptedKey})
			}
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
