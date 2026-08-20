package kafka

type AWSConfig struct {
	Region          string `koanf:"region"`
	RoleARN         string `koanf:"roleArn"`
	ExternalID      string `koanf:"externalId"`
	RoleSessionName string `koanf:"roleSessionName"`
}
