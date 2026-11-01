package kafka

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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
