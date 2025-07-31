package kafka

type AWS struct {
	RoleARN         string `koanf:"roleArn"`
	ExternalID      string `koanf:"externalId"`
	RoleSessionName string `koanf:"roleSessionName"`
}
