package kafka

import (
	"crypto/tls"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/google-cloud-tools/kafka-minion/options"
	"testing"
)

func TestTLSNilPanic(t *testing.T){
	defer func() {
		if r := recover(); r != nil{
			fmt.Println("recovered from panic")
		}
	}()

	opts := &options.Options{TLSEnabled: true, TLSInsecureSkipTLSVerify: true}
	clientConfig := sarama.Config{}
	clientConfig.Net.TLS.Enable = true

	if opts.TLSEnabled{
		clientConfig.Net.TLS.Enable = true
		clientConfig.Net.TLS.Enable = true
		clientConfig.Net.TLS.Config.InsecureSkipVerify = opts.TLSInsecureSkipTLSVerify
	}

}

func TestTLSNilPanicFixed(t *testing.T){
	opts := &options.Options{TLSEnabled: true, TLSInsecureSkipTLSVerify: true}
	clientConfig := sarama.Config{}
	clientConfig.Net.TLS.Enable = true

	if opts.TLSEnabled{
		clientConfig.Net.TLS.Config = &tls.Config{}
		clientConfig.Net.TLS.Enable = true
		clientConfig.Net.TLS.Enable = true
		clientConfig.Net.TLS.Config.InsecureSkipVerify = opts.TLSInsecureSkipTLSVerify
	}

}