package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google-cloud-tools/kafka-minion/options"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
)

// saramaClientConfig returns a sarama config pre initialized with SASL / TLS settings
// This function panics if the config can not be validated, for example due to a
// wrong TLS passphrase to decrypt the certificate.
func saramaClientConfig(opts *options.Options) *sarama.Config {
	clientConfig := sarama.NewConfig()
	clientConfig.ClientID = "kafka-lag-collector-1"
	clientConfig.Version = sarama.V0_11_0_2

	// SASL
	if opts.SASLEnabled {
		clientConfig.Net.SASL.Enable = true
		clientConfig.Net.SASL.Handshake = opts.UseSASLHandshake

		if opts.SASLUsername != "" {
			clientConfig.Net.SASL.User = opts.SASLUsername
		}
		if opts.SASLPassword != "" {
			clientConfig.Net.SASL.Password = opts.SASLPassword
		}
	}

	// Setup TLS
	if opts.TLSEnabled {
		// Ensure that Cert and Key can be read
		canReadCertAndKey, err := canReadCertAndKey(opts.TLSCertFilePath, opts.TLSKeyFilePath)
		if err != nil {
			log.Panic(err)
		}

		clientConfig.Net.TLS.Enable = true
		clientConfig.Net.TLS.Config = &tls.Config{
			RootCAs:            x509.NewCertPool(),
			InsecureSkipVerify: opts.TLSInsecureSkipTLSVerify,
		}

		// Load CA file
		if opts.TLSCAFilePath != "" {
			if ca, err := ioutil.ReadFile(opts.TLSCAFilePath); err == nil {
				clientConfig.Net.TLS.Config.RootCAs.AppendCertsFromPEM(ca)
			} else {
				log.Panic(err)
			}
		}

		// Load Cert file, if necessary it will be decrypted with a passphrase too
		if canReadCertAndKey {
			cert, err := getCert(opts)
			if err == nil {
				clientConfig.Net.TLS.Config.Certificates = cert
			} else {
				log.Panic(err)
			}
		}
	}

	err := clientConfig.Validate()
	if err != nil {
		log.Panicf("Error validating kafka client config. %s", err)
	}
	log.Debug("Sarama client config has been created successfully")

	return clientConfig
}

// canReadCertAndKey returns true if the certificate and key files already exists,
// otherwise returns false. If lost one of cert and key, returns error.
func canReadCertAndKey(certPath, keyPath string) (bool, error) {
	certReadable := canReadFile(certPath)
	keyReadable := canReadFile(keyPath)

	if certReadable == false && keyReadable == false {
		return false, nil
	}

	if certReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", certPath)
	}

	if keyReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", keyPath)
	}

	return true, nil
}

// If the file represented by path exists and
// readable, returns true otherwise returns false.
func canReadFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}

	defer f.Close()

	return true
}

// getCert returns a Certificate from the CertFile and KeyFile in 'options',
// if the key is encrypted, the Passphrase in 'options' will be used to decrypt it.
func getCert(options *options.Options) ([]tls.Certificate, error) {
	if options.TLSCertFilePath == "" && options.TLSKeyFilePath == "" {
		return nil, fmt.Errorf("No file path specified for TLS key and certificate in environment variables")
	}

	errMessage := "Could not load X509 key pair. "

	cert, err := ioutil.ReadFile(options.TLSCertFilePath)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	prKeyBytes, err := ioutil.ReadFile(options.TLSKeyFilePath)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	prKeyBytes, err = getPrivateKey(prKeyBytes, options.TLSPassphrase)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	tlsCert, err := tls.X509KeyPair(cert, prKeyBytes)
	if err != nil {
		return nil, fmt.Errorf(errMessage, err)
	}

	return []tls.Certificate{tlsCert}, nil
}

// getPrivateKey returns the private key in 'keyBytes', in PEM-encoded format.
// If the private key is encrypted, 'passphrase' is used to decrypted the private key.
func getPrivateKey(keyBytes []byte, passphrase string) ([]byte, error) {
	// this section makes some small changes to code from notary/tuf/utils/x509.go
	pemBlock, _ := pem.Decode(keyBytes)
	if pemBlock == nil {
		return nil, fmt.Errorf("no valid private key found")
	}

	var err error
	if x509.IsEncryptedPEMBlock(pemBlock) {
		keyBytes, err = x509.DecryptPEMBlock(pemBlock, []byte(passphrase))
		if err != nil {
			return nil, fmt.Errorf("private key is encrypted, but could not decrypt it: '%s'", err)
		}
		keyBytes = pem.EncodeToMemory(&pem.Block{Type: pemBlock.Type, Bytes: keyBytes})
	}

	return keyBytes, nil
}
