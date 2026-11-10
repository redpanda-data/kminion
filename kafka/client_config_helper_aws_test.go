package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"go.uber.org/zap"
)

const mskBroker = "b-1.example.c2.kafka.us-east-1.amazonaws.com:9098"

// isolateAWSEnv keeps the developer's or CI's AWS configuration out of the credential chain.
func isolateAWSEnv(t *testing.T) {
	t.Helper()
	empty := filepath.Join(t.TempDir(), "empty")
	require.NoError(t, os.WriteFile(empty, nil, 0o600))

	for key, value := range map[string]string{
		"AWS_CONFIG_FILE":                        empty,
		"AWS_SHARED_CREDENTIALS_FILE":            empty,
		"AWS_PROFILE":                            "",
		"AWS_DEFAULT_PROFILE":                    "",
		"AWS_REGION":                             "",
		"AWS_DEFAULT_REGION":                     "",
		"AWS_ACCESS_KEY_ID":                      "",
		"AWS_SECRET_ACCESS_KEY":                  "",
		"AWS_SESSION_TOKEN":                      "",
		"AWS_ROLE_ARN":                           "",
		"AWS_WEB_IDENTITY_TOKEN_FILE":            "",
		"AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI":     "",
		"AWS_ENDPOINT_URL":                       "",
		"AWS_ENDPOINT_URL_STS":                   "",
		"AWS_EC2_METADATA_DISABLED":              "true",
	} {
		t.Setenv(key, value)
	}
}

func awsMSKIAMConfig() Config {
	cfg := Config{}
	cfg.SetDefaults()
	cfg.Brokers = []string{mskBroker}
	cfg.TLS.Enabled = true
	cfg.SASL.Enabled = true
	cfg.SASL.Mechanism = SASLMechanismAWSMSKIAM
	return cfg
}

func awsMSKIAMMechanism(t *testing.T, cfg Config) sasl.Mechanism {
	t.Helper()
	require.NoError(t, cfg.Validate())

	opts, err := NewKgoConfig(cfg, zap.NewNop())
	require.NoError(t, err)
	client, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	mechanisms := client.OptValue(kgo.SASL).([]sasl.Mechanism)
	require.Len(t, mechanisms, 1)
	require.Equal(t, SASLMechanismAWSMSKIAM, mechanisms[0].Name())
	return mechanisms[0]
}

func authenticate(t *testing.T, mechanism sasl.Mechanism) map[string]string {
	t.Helper()
	_, message, err := mechanism.Authenticate(context.Background(), mskBroker)
	require.NoError(t, err)

	var challenge map[string]string
	require.NoError(t, json.Unmarshal(message, &challenge))
	return challenge
}

func TestNewKgoConfig_AWSMSKIAMDefaultCredentialChain(t *testing.T) {
	tests := []struct {
		name         string
		sessionToken string
	}{
		{name: "static credentials"},
		{name: "temporary credentials", sessionToken: "session-token"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isolateAWSEnv(t)
			t.Setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
			t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
			t.Setenv("AWS_SESSION_TOKEN", tt.sessionToken)

			challenge := authenticate(t, awsMSKIAMMechanism(t, awsMSKIAMConfig()))

			require.Equal(t, "kafka-cluster:Connect", challenge["action"])
			require.Equal(t, "b-1.example.c2.kafka.us-east-1.amazonaws.com", challenge["host"])
			require.Equal(t, "2020_10_22", challenge["version"])
			require.Regexp(t, `^AKIAEXAMPLE/\d{8}/us-east-1/kafka-cluster/aws4_request$`, challenge["x-amz-credential"])
			require.NotEmpty(t, challenge["x-amz-signature"])

			token, ok := challenge["x-amz-security-token"]
			require.Equal(t, tt.sessionToken != "", ok)
			require.Equal(t, tt.sessionToken, token)
		})
	}
}

type stsStub struct {
	mu       sync.Mutex
	requests []url.Values
	expires  time.Time
}

func (s *stsStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.requests = append(s.requests, r.PostForm)
	call := len(s.requests)
	s.mu.Unlock()

	w.Header().Set("Content-Type", "text/xml")
	fmt.Fprintf(w, `<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>ASIAASSUMED%d</AccessKeyId>
      <SecretAccessKey>assumed-secret</SecretAccessKey>
      <SessionToken>assumed-token-%d</SessionToken>
      <Expiration>%s</Expiration>
    </Credentials>
    <AssumedRoleUser>
      <Arn>arn:aws:sts::123456789012:assumed-role/kminion/kminion-session</Arn>
      <AssumedRoleId>AROAEXAMPLE:kminion-session</AssumedRoleId>
    </AssumedRoleUser>
  </AssumeRoleResult>
  <ResponseMetadata><RequestId>%d</RequestId></ResponseMetadata>
</AssumeRoleResponse>`, call, call, s.expires.UTC().Format(time.RFC3339), call)
}

func (s *stsStub) calls() []url.Values {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]url.Values(nil), s.requests...)
}

func assumeRoleConfig(t *testing.T, stub *stsStub) Config {
	isolateAWSEnv(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIABASE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "base-secret")

	server := httptest.NewServer(stub)
	t.Cleanup(server.Close)
	t.Setenv("AWS_ENDPOINT_URL_STS", server.URL)

	cfg := awsMSKIAMConfig()
	cfg.SASL.AWS.Region = "us-east-1"
	cfg.SASL.AWS.RoleARN = "arn:aws:iam::123456789012:role/kminion"
	cfg.SASL.AWS.ExternalID = "external-id"
	cfg.SASL.AWS.RoleSessionName = "kminion-session"
	return cfg
}

func TestNewKgoConfig_AWSMSKIAMAssumeRole(t *testing.T) {
	stub := &stsStub{expires: time.Now().Add(time.Hour)}
	mechanism := awsMSKIAMMechanism(t, assumeRoleConfig(t, stub))

	first := authenticate(t, mechanism)
	second := authenticate(t, mechanism)

	calls := stub.calls()
	require.Len(t, calls, 1, "assumed credentials are cached until they expire")
	require.Equal(t, "AssumeRole", calls[0].Get("Action"))
	require.Equal(t, "arn:aws:iam::123456789012:role/kminion", calls[0].Get("RoleArn"))
	require.Equal(t, "external-id", calls[0].Get("ExternalId"))
	require.Equal(t, "kminion-session", calls[0].Get("RoleSessionName"))

	for _, challenge := range []map[string]string{first, second} {
		require.Regexp(t, `^ASIAASSUMED1/`, challenge["x-amz-credential"])
		require.Equal(t, "assumed-token-1", challenge["x-amz-security-token"])
	}
}

func TestNewKgoConfig_AWSMSKIAMAssumeRoleRefreshesExpiredCredentials(t *testing.T) {
	stub := &stsStub{expires: time.Now().Add(-time.Minute)}
	mechanism := awsMSKIAMMechanism(t, assumeRoleConfig(t, stub))

	first := authenticate(t, mechanism)
	second := authenticate(t, mechanism)

	require.Len(t, stub.calls(), 2)
	require.Equal(t, "assumed-token-1", first["x-amz-security-token"])
	require.Equal(t, "assumed-token-2", second["x-amz-security-token"])
}

func TestNewKgoConfig_AWSMSKIAMRoleARNRequiresRegion(t *testing.T) {
	isolateAWSEnv(t)

	cfg := awsMSKIAMConfig()
	cfg.SASL.AWS.RoleARN = "arn:aws:iam::123456789012:role/kminion"
	require.NoError(t, cfg.Validate())

	_, err := NewKgoConfig(cfg, zap.NewNop())
	require.EqualError(t, err, "kafka.sasl.aws.region (or AWS_REGION) is required when roleArn is set")

	t.Setenv("AWS_REGION", "us-east-1")
	_, err = NewKgoConfig(cfg, zap.NewNop())
	require.NoError(t, err)
}

func TestSASLConfig_ValidateAWSMSKIAM(t *testing.T) {
	tests := []struct {
		name    string
		aws     AWSConfig
		wantErr bool
	}{
		{name: "default credential chain", aws: AWSConfig{}},
		{name: "region only", aws: AWSConfig{Region: "us-east-1"}},
		{name: "assume role", aws: AWSConfig{RoleARN: "arn:aws:iam::123456789012:role/kminion", ExternalID: "id", RoleSessionName: "s"}},
		{name: "external id without role", aws: AWSConfig{ExternalID: "id"}, wantErr: true},
		{name: "session name without role", aws: AWSConfig{RoleSessionName: "s"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := SASLConfig{Enabled: true, Mechanism: SASLMechanismAWSMSKIAM, AWS: tt.aws}
			if tt.wantErr {
				require.Error(t, cfg.Validate())
			} else {
				require.NoError(t, cfg.Validate())
			}
		})
	}
}
