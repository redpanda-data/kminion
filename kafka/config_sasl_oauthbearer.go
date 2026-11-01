package kafka

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

type OAuthBearerConfig struct {
	TokenEndpoint string `koanf:"tokenEndpoint"`
	ClientID      string `koanf:"clientId"`
	ClientSecret  string `koanf:"clientSecret"`
	Scope         string `koanf:"scope"`
}

func (c *OAuthBearerConfig) Validate() error {
	if c.TokenEndpoint == "" {
		return fmt.Errorf("OAuthBearer token endpoint is not specified")
	}
	if c.ClientID == "" || c.ClientSecret == "" {
		return fmt.Errorf("OAuthBearer client credentials are not specified")
	}
	return nil
}

// same as AcquireToken in Console https://github.com/redpanda-data/console/blob/master/backend/pkg/config/kafka_sasl_oauth.go#L56
func (c *OAuthBearerConfig) getToken(ctx context.Context) (string, error) {
	authHeaderValue := base64.StdEncoding.EncodeToString([]byte(c.ClientID + ":" + c.ClientSecret))

	queryParams := url.Values{
		"grant_type": []string{"client_credentials"},
		"scope":      []string{c.Scope},
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.TokenEndpoint, strings.NewReader(queryParams.Encode()))
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.URL.RawQuery = queryParams.Encode()

	req.Header.Set("Authorization", "Basic "+authHeaderValue)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("HTTP request failed: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token request failed with status code %d", resp.StatusCode)
	}

	var tokenResponse map[string]interface{}
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&tokenResponse); err != nil {
		return "", fmt.Errorf("failed to parse token response: %w", err)
	}

	accessToken, ok := tokenResponse["access_token"].(string)
	if !ok {
		return "", fmt.Errorf("access_token not found in token response")
	}

	return accessToken, nil
}
