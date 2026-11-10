package kafka

import "fmt"

type AWSConfig struct {
	Region          string `koanf:"region"`
	RoleARN         string `koanf:"roleArn"`
	ExternalID      string `koanf:"externalId"`
	RoleSessionName string `koanf:"roleSessionName"`
}

func (c *AWSConfig) Validate() error {
	if c.RoleARN == "" && (c.ExternalID != "" || c.RoleSessionName != "") {
		return fmt.Errorf("AWS externalId and roleSessionName are only used with roleArn, which is not specified")
	}
	return nil
}
